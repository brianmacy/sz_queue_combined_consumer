# syntax=docker/dockerfile:1
#
# Distroless multi-stage build for the combined Rust Senzing load+redo driver.
#
#   docker build -t brian/sz_rabbit_combined_consumer .                     # both backends
#   docker build --build-arg WITH_MSSQL=0   -t ...:pg    .              # PostgreSQL only
#   docker build --build-arg WITH_POSTGRES=0 -t ...:mssql .             # SQL Server only
#
# Backend selection is build-time. At least one backend must be enabled; the
# build errors out if both are disabled. See DOCKER_NOTES.md for the measured
# library manifest and the verification of the distroless contents.
#
# This Dockerfile shares ONE canonical "stage the Senzing native closure into a
# distroless image" pattern with sz_simple_redoer_rust. Everything below the
# builder stage (the backend-libs collector + the runtime stage) is byte-for-byte
# identical between the two repos; only the builder's source layout, the app
# binary name in the final COPY, and the ENTRYPOINT differ.

ARG SENZING_RUNTIME_IMAGE=senzing/senzingsdk-runtime:4.3.3
ARG RUST_IMAGE=rust:1.88

# Global build args (declared before the first FROM so the backend-libs stage
# and every per-stage ARG below inherit the same defaults). Must be 1 or 0.
ARG WITH_POSTGRES=1
ARG WITH_MSSQL=1

# ---------------------------------------------------------------------------
# Stage 0: szruntime — alias the (version-parameterized) Senzing runtime image
# ONCE, so every COPY --from below pulls the SAME version. Override
# SENZING_RUNTIME_IMAGE (e.g. --build-arg SENZING_RUNTIME_IMAGE=senzing/
# senzingsdk-runtime:4.2.4) to rebuild the whole image against another engine
# version. 4.2.4 / 4.3.3 / 4.4.0 runtimes are all Debian 13 (trixie, glibc
# 2.41), so the cc-debian13 runtime base + debian/13 MS ODBC repo below hold
# across versions. (Previously each COPY hardcoded :4.3.2.)
# ---------------------------------------------------------------------------
FROM ${SENZING_RUNTIME_IMAGE} AS szruntime

# ---------------------------------------------------------------------------
# Stage 1: builder — compile the Rust binary against libSz.
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS builder

# Bring in the Senzing runtime so the sz-rust-sdk build.rs can link dylib=Sz. It
# searches /opt/senzing/er/lib by default (overridable via SENZING_LIB_PATH).
COPY --from=szruntime /opt/senzing /opt/senzing
ENV SENZING_LIB_PATH=/opt/senzing/er/lib
ENV LD_LIBRARY_PATH=/opt/senzing/er/lib

WORKDIR /app

# Which workspace binary to build: sz_rabbit_combined_consumer (RabbitMQ, default)
# or sz_sqs_combined_consumer (Amazon SQS). `cargo build -p ${BIN}` compiles only
# that bin + its deps, so the RabbitMQ image never pulls the AWS SDK and the SQS
# image never pulls lapin.
ARG BIN=sz_rabbit_combined_consumer

# Build the selected binary from the workspace. (The single-crate dummy-src
# dep-cache trick does not translate cleanly to a multi-crate workspace; the CI
# target cache covers incremental speedups for the non-Docker jobs.)
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p "${BIN}"

# ===========================================================================
# CANONICAL SENZING SECTION — keep byte-identical across the sibling repos
# (sz_rabbit_consumer_rust and sz_simple_redoer_rust).
# ===========================================================================

# ---------------------------------------------------------------------------
# Stage 2: backend-libs — assemble the backend-specific native library closure
# into a staging tree, selected by build args. Conditional COPY is not possible
# in Dockerfiles, so the selection happens here in shell and the runtime stage
# copies the whole staging tree unconditionally.
# ---------------------------------------------------------------------------
FROM ${SENZING_RUNTIME_IMAGE} AS backend-libs
ARG WITH_POSTGRES=1
ARG WITH_MSSQL=1
USER root

RUN set -eu; \
    if [ "$WITH_POSTGRES" != 1 ] && [ "$WITH_MSSQL" != 1 ]; then \
        echo "ERROR: enable at least one of WITH_POSTGRES / WITH_MSSQL" >&2; exit 1; \
    fi; \
    mkdir -p /staging/lib /staging/etc /staging/opt; \
    LIBDIR=/lib/x86_64-linux-gnu; \
    USRLIBDIR=/usr/lib/x86_64-linux-gnu; \
    # cp_lib copies each requested soname into /staging/lib, dereferencing
    # symlinks (-L): many of these (e.g. libpq.so.5 -> libpq.so.5.17) are
    # versioned symlinks; copying the symlink alone would leave it dangling, so
    # we copy the real file under the soname the loader looks for. It FAILS THE
    # BUILD LOUDLY (echo "MISSING: ..."; exit 1) if any requested soname is
    # absent from both lib dirs, instead of silently producing an image that
    # dlopen-fails (SENZ0087) in production.
    cp_lib() { for f in "$@"; do \
        if [ -e "$LIBDIR/$f" ]; then cp -L "$LIBDIR/$f" /staging/lib/"$f"; \
        elif [ -e "$USRLIBDIR/$f" ]; then cp -L "$USRLIBDIR/$f" /staging/lib/"$f"; \
        else echo "MISSING: $f" >&2; exit 1; fi; done; }; \
    # COMMON (always): libz/libzstd are DT_NEEDED of libSz/the plugins but are
    # NOT shipped by distroless/cc, so they are staged for every backend.
    cp_lib libz.so.1 libzstd.so.1; \
    if [ "$WITH_POSTGRES" = 1 ]; then \
        # PostgreSQL: libpostgresqlplugin.so reaches PostgreSQL via libpq. The
        # libpq closure below is the union of the two repos' validated lists,
        # measured with ldd in this image; libstdc++, libgcc_s, libssl,
        # libcrypto, libm, libc are already in the distroless/cc runtime so they
        # are intentionally omitted.
        cp_lib libpq.so.5 libldap.so.2 liblber.so.2 libsasl2.so.2 \
               libgssapi_krb5.so.2 libkrb5.so.3 libk5crypto.so.3 \
               libkrb5support.so.0 libcom_err.so.2 libkeyutils.so.1 \
               libresolv.so.2; \
    fi; \
    if [ "$WITH_MSSQL" = 1 ]; then \
        # SQL Server: libmssqlplugin.so reaches SQL Server through the unixODBC
        # stack and the dlopen'd Microsoft ODBC driver. These are NOT present in
        # the senzingsdk-runtime base, so install them from the Microsoft repo
        # (Debian 13 config path).
        apt-get update; \
        apt-get -y install --no-install-recommends ca-certificates curl gnupg apt-transport-https; \
        curl -sSL -o /tmp/packages-microsoft-prod.deb https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb; \
        dpkg -i /tmp/packages-microsoft-prod.deb; rm -f /tmp/packages-microsoft-prod.deb; \
        apt-get update; \
        ACCEPT_EULA=Y apt-get -y install --no-install-recommends msodbcsql18 unixodbc; \
        # unixODBC driver manager + libtool dl + the dlopen'd driver itself.
        cp_lib libodbc.so.2 libodbcinst.so.2 libltdl.so.7; \
        DRIVER="$(find /opt/microsoft/msodbcsql18/lib64 -name 'libmsodbcsql-18*.so*' | head -n1)"; \
        cp -L "$DRIVER" /staging/lib/"$(basename "$DRIVER")"; \
        # Driver dependency closure beyond what distroless/cc already provides.
        cp_lib libkrb5.so.3 libgssapi_krb5.so.2 libk5crypto.so.3 \
               libcom_err.so.2 libkrb5support.so.0 libkeyutils.so.1 \
               libldap.so.2 liblber.so.2 libsasl2.so.2; \
        # Stage the full driver tree under /opt so odbcinst.ini's absolute
        # Driver= path resolves in the runtime image. Copy the WHOLE msodbcsql18
        # tree, not just lib64/: the driver loads its resource/message catalog
        # share/resources/en_US/msodbcsqlr18.rll during SQLAllocHandle(HENV), and
        # without it env allocation fails with IM004 (verified: lib64-only staging
        # → "Driver's SQLAllocHandle on SQL_HANDLE_HENV failed"; adding share/ fixes it).
        mkdir -p /staging/opt/microsoft/msodbcsql18; \
        cp -a /opt/microsoft/msodbcsql18/. /staging/opt/microsoft/msodbcsql18/; \
        # ODBC driver registration + DSN. odbcinst.ini maps the driver name to
        # the .so; odbc.ini's [MSSQL] DSN sets AutoTranslate=No (prevents UTF-8
        # corruption). Server/Database/port come from the engine connection string.
        printf '[ODBC Driver 18 for SQL Server]\nDescription=Microsoft ODBC Driver 18 for SQL Server\nDriver=%s\nUsageCount=1\n' "$DRIVER" > /staging/etc/odbcinst.ini; \
        printf '[MSSQL]\nDriver = ODBC Driver 18 for SQL Server\nAutoTranslate = No\n' > /staging/etc/odbc.ini; \
    fi

# ---------------------------------------------------------------------------
# Stage 3: runtime — distroless. No interpreter, no apt, no shell.
# ---------------------------------------------------------------------------
# Runtime base MUST match the glibc of senzingsdk-runtime. As of 4.3.3 that base
# is Debian 13 (trixie, glibc 2.41), so cc-debian13 is required — cc-debian12
# (glibc 2.36) fails at runtime with "GLIBC_2.38 not found" when the trixie-built
# libpq is loaded. Verified empirically.

# ---------------------------------------------------------------------------
# unixODBC driver-manager from Debian BOOKWORM (debian:12) — apples-to-apples
# with the Python consumer images (built on the debian:12 senzingsdk-runtime).
# WHY: the senzingsdk-runtime base moved to trixie, whose unixODBC 2.3.12
# validates EVERY ODBC handle by walking a global list under a global mutex (no
# effective --enable-fastvalidate). With 96 engine threads/process that serializes
# all SQL to ~1 in-flight call — measured: 36% of cycles in libodbc's locked
# list-walk, ~1.5 cores/process, DB starved at 94% idle, ~59 SQL round-trips/record
# capped at ~17k batch/s → ~300 rec/s. Bookworm's build is the one that scales
# (Senzing "Database Tuning": Ubuntu/Debian ship --enable-fastvalidate; the
# senzingsdk consumer images have always used Debian's). We override ONLY the DM
# libs here; the Microsoft driver (msodbcsql18) + krb5/etc closure still come from
# the backend-libs stage. glibc is forward-compatible (bookworm 2.36 libs run on
# the cc-debian13 / glibc 2.41 runtime).
FROM debian:12-slim AS odbcdm
RUN apt-get update && apt-get install -y --no-install-recommends unixodbc && \
    mkdir -p /dm && \
    cp -L /usr/lib/x86_64-linux-gnu/libodbc.so.2     /dm/ && \
    cp -L /usr/lib/x86_64-linux-gnu/libodbcinst.so.2 /dm/ && \
    cp -L /usr/lib/x86_64-linux-gnu/libltdl.so.7     /dm/

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

# COMMON: the whole Senzing er/lib tree (libSz.so + all g2 comparator/hasher/
# parser plugins + the *ECreator feature-expression libs + db plugins +
# szvec/szzstd), plus resources and the build-version manifest. Copying the
# entire lib directory guarantees the engine's full plugin set is present; the
# ECreator libs (libg2*ECreator.so) ship in the runtime image and are required
# by the engine (their absence raises SENZ0087).
COPY --from=szruntime /opt/senzing/er/lib       /opt/senzing/er/lib
COPY --from=szruntime /opt/senzing/er/resources /opt/senzing/er/resources
COPY --from=szruntime /opt/senzing/er/szBuildVersion.json /opt/senzing/er/szBuildVersion.json

# SUPPORTPATH data (transliteration models, name/address data models, etc.) and
# the CONFIGPATH templates. The engine loads these at init — measured: omitting
# /opt/senzing/data raises SENZ7426 (e.g. "Could not load transliterator module:
# thaiTransRules.sz"). The default engine config points SUPPORTPATH at
# /opt/senzing/data and CONFIGPATH at /etc/opt/senzing.
COPY --from=szruntime /opt/senzing/data /opt/senzing/data
COPY --from=szruntime /etc/opt/senzing  /etc/opt/senzing

# Backend-specific libraries + ODBC ini files + driver tree assembled in stage 2.
#
# IMPORTANT: the backend libraries are staged INTO /opt/senzing/er/lib (next to
# libSz.so), NOT into /usr/lib/x86_64-linux-gnu. The db plugins are dlopen'd by
# libSz and the distroless image has no /etc/ld.so.cache, so the default system
# lib dirs are not searched for the plugins' transitive deps — only the
# directories on LD_LIBRARY_PATH are. Placing the closure next to libSz (which is
# on LD_LIBRARY_PATH) is what makes dlopen resolve libpq / krb5 / ODBC. This was
# verified empirically: copying to /usr/lib/x86_64-linux-gnu left libpq.so.5 (and
# then libgssapi_krb5.so.2) unresolvable; copying to er/lib resolves them.
COPY --from=backend-libs /staging/lib/ /opt/senzing/er/lib/
COPY --from=backend-libs /staging/etc/ /etc/
COPY --from=backend-libs /staging/opt/ /opt/

# Override the unixODBC driver-manager with the bookworm (fastvalidate) build.
# MUST be the LAST er/lib COPY so it wins over trixie's libodbc (from the runtime
# base + backend-libs). This is the fix for the global-mutex handle-validation
# serialization — see the odbcdm stage above.
COPY --from=odbcdm /dm/ /opt/senzing/er/lib/

# er/lib carries libSz + all plugins + the backend closure. The MSSQL driver
# lives under /opt/microsoft and is referenced absolutely by odbcinst.ini, but
# its own deps resolve via er/lib too.
ENV LD_LIBRARY_PATH=/opt/senzing/er/lib

# ===========================================================================
# END CANONICAL SENZING SECTION.
# ===========================================================================

# Memory mitigation (GDEV-4294). Under sustained giant-component load the
# compare/scoring path allocates large transient buffers that glibc frees but
# retains at arena high-water (process RSS balloons to 20-70 GB while engine live
# is ~1 GB). Pinning MALLOC_MMAP_THRESHOLD_ disables glibc's dynamic threshold so
# large allocations go through mmap and are returned to the OS on free — RSS then
# tracks the live working set instead of pinning. MALLOC_TRIM_THRESHOLD_ keeps the
# main-arena top trimmed. Validated on a 330 M load: a host with these set held
# steady / recovered under load while an unmodified control ballooned to OOM.
# Stopgap only (blunt: applies to every >128 KB alloc) — the real fix is in-engine
# (mmap-backed compare/scoring arenas, GDEV-4294). Unset if the per-alloc mmap
# cost outweighs the RSS benefit for your workload. Do NOT LD_PRELOAD jemalloc/
# tcmalloc — an interposed allocator SIGSEGVs libSz.
ENV MALLOC_MMAP_THRESHOLD_=131072 \
    MALLOC_TRIM_THRESHOLD_=131072

# Re-declare in this stage (ARGs do not cross FROM boundaries). Selects which
# built binary is installed + labeled; must match the builder-stage BIN.
ARG BIN=sz_rabbit_combined_consumer
LABEL org.opencontainers.image.title="${BIN}" \
      org.opencontainers.image.description="Combined Senzing load + redo driver (Rust): ${BIN}" \
      org.opencontainers.image.licenses="Apache-2.0"

# Files-only NSS — STANDARD PRACTICE for these distroless service containers.
# The cc-debian13 base ships Debian defaults (passwd/group: compat, netgroup: nis).
# Under the msodbc18 / krb5 / GSSAPI per-connection lookup path at high worker
# concurrency, compat+nis route getpwnam/getgrnam/netgroup through NIS/SunRPC
# (clntudp/authdes) and serialize every worker thread on the glibc NSS global
# rwlock — a dominant, silent throughput bottleneck (CPU half-idle, negative
# scaling). This container reaches the datastore/AMQP by IP with SQL auth and
# never needs NIS/LDAP, so files-only removes the convoy. (Apply identically to
# the sibling sz_rabbit_consumer_rust / sz_simple_redoer_rust images.)
COPY nsswitch.conf /etc/nsswitch.conf

# Fixed install path so the exec-form ENTRYPOINT need not interpolate ${BIN}
# (which it cannot). The image tag distinguishes the RabbitMQ vs SQS backend.
COPY --from=builder /app/target/release/${BIN} /usr/local/bin/sz-consumer

ENTRYPOINT ["/usr/local/bin/sz-consumer"]
