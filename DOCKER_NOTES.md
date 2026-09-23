<!-- Restored 2026-09-23 from sz_rabbit_consumer_rust (the Dockerfile has
     referenced this file since the merge, but it was not carried over). The
     measurements below were taken against that repo's image on 2026-06-29;
     the builder base is now rust:1.94.1 (see Dockerfile / CHANGELOG), the
     runtime manifest findings are unchanged. -->

# Docker / distroless verification notes

These notes record the build-time verification of the distroless runtime image
manifest. Findings were **measured** against the actual images on 2026-06-29,
not assumed. The PostgreSQL variant was built and run end-to-end against a real
PostgreSQL backend; the MSSQL variant is blocked by an upstream Microsoft repo
issue (see below).

## Image versions (and why they must match)

- Builder base: `rust:1.88`.
- Senzing runtime source: `senzing/senzingsdk-runtime:4.3.3` — **Debian 13
  (trixie), glibc 2.41**.
- Runtime base: `gcr.io/distroless/cc-debian13:nonroot` — **glibc 2.41**.

### The runtime base must be cc-debian13, NOT cc-debian12

The plan specified `gcr.io/distroless/cc-debian12`. That is WRONG for the
current Senzing runtime image: `senzing/senzingsdk-runtime:4.3.3` is built on
Debian 13 (trixie, glibc 2.41), but `cc-debian12` ships glibc 2.36. The
trixie-built `libpq.so.5` requires `GLIBC_2.38`, so on cc-debian12 the engine
fails at runtime:

```
SENZ0087 ... FAILED TO LOAD LIBRARY[libpostgresqlplugin.so]:
  /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found
  (required by /opt/senzing/er/lib/libpq.so.5)
```

Switching the runtime base to `gcr.io/distroless/cc-debian13:nonroot` (glibc
2.41, verified to also ship `libstdc++.so.6`, `libgcc_s.so.1`, `libssl.so.3`,
`libcrypto.so.3`, `libgomp.so.1`) resolves it. Keep the distroless Debian
generation aligned with the senzingsdk-runtime base on every version bump.

### Version note (4.3.3 vs 4.4.0)

The plan referenced `senzing/senzingsdk-runtime:4.4.0`, and the locally
installed SDK on the dev box is 4.4.0. However, **4.4.0 is not published to
Docker Hub** — the newest published tag is `4.3.3` (verified via the Docker Hub
tags API). The Dockerfile pins `4.3.3`. When 4.4.0 is published, bump
`SENZING_RUNTIME_IMAGE` + the `COPY --from` refs, and re-check the runtime
image's Debian generation against the distroless tag.

## Item (a): does distroless/cc ship libstdc++ and libgcc_s?

**Yes — both are present in cc-debian13 (and cc-debian12), so they do NOT need
to be added to the MSSQL copy set.** Exported the image and listed libs:

```bash
cid=$(docker create --entrypoint /nonexistent gcr.io/distroless/cc-debian13:nonroot)
docker export "$cid" | tar -t | grep -E '\.so'
docker rm "$cid"
```

cc-debian13 provides (relevant subset): `libstdc++.so.6` (6.0.33),
`libgcc_s.so.1`, `libssl.so.3`, `libcrypto.so.3`, `libc.so.6`, `libm.so.6`,
`libdl.so.2`, `libpthread.so.0`, `libresolv.so.1`, `libgomp.so.1`. The backend
copy closures intentionally omit these.

## Item (b): are the ECreator / feature-expression libs needed at runtime?

**Yes the engine needs them — and they ship in the runtime image already**, so
copying the whole `/opt/senzing/er/lib` directory captures them. No separate
`senzingsdk-setup` step is required.

```bash
cid=$(docker create --entrypoint /nonexistent senzing/senzingsdk-runtime:4.3.3)
docker export "$cid" | tar -t | grep -iE 'ECreator'
docker rm "$cid"
# -> opt/senzing/er/lib/libg2CreditCardECreator.so
#    opt/senzing/er/lib/libg2PlacekeyECreator.so
```

`/opt/senzing/er/lib` in the runtime image also contains ~50 `libg2*`
comparator/hasher/parser plugins that the resolution (and therefore consume)
path loads. The Dockerfile copies the **entire** directory rather than
enumerating files, which is robust against version drift and guarantees the
ECreators and every plugin are present (their absence raises SENZ0087).

## Additional COMMON manifest items the plan missed (measured)

The plan's COMMON list omitted two directories the engine loads at init. Both
were found by running the binary and reading the actual error:

- **`/opt/senzing/data` (SUPPORTPATH)** — transliteration models, name/address
  data models, etc. Omitting it raises:
  `SENZ7426 ... Could not load transliterator module: thaiTransRules.sz`.
- **`/etc/opt/senzing` (CONFIGPATH)** — config templates (`cfgVariant.json`,
  custom Gn/On/Sn lists).

Both are now copied in the COMMON section. (The plan's `libszvec.so`/
`libszzstd.so` names are also stale — in 4.3.3 they are `szvec.so`/`szzstd.so`
inside er/lib, captured automatically by the whole-directory copy.)

## Backend libraries go in /opt/senzing/er/lib, not /lib/x86_64-linux-gnu

The db plugins (`libpostgresqlplugin.so`, `libmssqlplugin.so`) are **dlopen'd by
libSz**, and distroless has **no `/etc/ld.so.cache`**. With no cache, the
loader does not search the standard `/lib/x86_64-linux-gnu` for the plugins'
transitive deps — only the directories on `LD_LIBRARY_PATH`. Verified: copying
the closure to `/lib/x86_64-linux-gnu` left `libpq.so.5` unresolvable; copying
it into `/opt/senzing/er/lib` (which is on `LD_LIBRARY_PATH` next to libSz)
resolves it. The `backend-libs` stage therefore stages into er/lib.

## Symlinks must be dereferenced when copying

`libpq.so.5` (and friends) are **versioned symlinks** (`libpq.so.5 ->
libpq.so.5.17`). Copying the symlink alone (`cp -a`) leaves it dangling and the
loader reports `cannot open shared object file`. The `cp_lib` helper uses
`cp -L` to copy the real file under the soname the loader looks for.

## PostgreSQL closure (measured)

Full transitive closure of `libpostgresqlplugin.so` + `libpq.so.5`, walked with
`ldd` inside the runtime image. Libraries in the closure that are **not** already
in cc-debian13 (so they are COPY'd by the `WITH_POSTGRES=1` path):

```
libpq.so.5  libldap.so.2  liblber.so.2  libsasl2.so.2
libgssapi_krb5.so.2  libkrb5.so.3  libk5crypto.so.3  libkrb5support.so.0
libcom_err.so.2  libkeyutils.so.1  libz.so.1  libzstd.so.1
```

The plan's PG list additionally named `libgnutls`, `libp11-kit`, `libidn2`,
`libunistring`, `libtasn1`, `libnettle`, `libhogweed`, `libgmp`, `libffi` —
those are NOT in the measured closure for this image (this `libpq` links
GSSAPI/SASL/LDAP, not gnutls) and are omitted.

## Verified result (PostgreSQL variant) — END TO END

Built and run on 2026-06-29 against a real PostgreSQL 16 container (schema from
`szcore-schema-postgresql-create.sql`, default config registered):

```bash
docker build --build-arg WITH_POSTGRES=1 --build-arg WITH_MSSQL=0 \
  -t sz_rabbit_consumer_rust:pg .            # image ~579 MB (dominated by libSz)
docker run -e SENZING_ENGINE_CONFIGURATION_JSON='{...postgresql...}' \
  sz_rabbit_consumer_rust:pg
```

Observed (engine init + consume loop + graceful shutdown all work):

```
INFO Thread pool configured: 2 workers
INFO Senzing environment initialized
INFO Waiting for messages from RabbitMQ...
WARN Graceful shutdown requested            # on SIGTERM (docker stop)
INFO Completed: 0 records processed, 0 errors
INFO Final engine stats: {"workload":{...}} # get_stats() on exit
```

A missing lib in the COPY manifest = a build/run failure here, NOT a silent
production dlopen crash. (Note: SIGTERM handling requires the ctrlc crate's
`termination` feature, which is enabled in Cargo.toml; without it `docker stop`
would SIGKILL with no graceful shutdown.)

## RabbitMQ / TLS dependencies (lapin + rustls)

`sz_rabbit_consumer` links **lapin** (the RabbitMQ AMQP 0-9-1 client) with
**rustls** as its TLS backend. Both are pure-Rust crates — they carry no
additional system shared libraries. The binary is fully self-contained with
respect to TLS; no `libssl`/`libcrypto` copy is needed beyond what cc-debian13
already provides for libSz.

## SQL Server (MSSQL) — partially verified, BLOCKED locally

`libmssqlplugin.so` reaches SQL Server through the unixODBC stack and the
dlopen'd Microsoft ODBC driver `libmsodbcsql-18*.so`. These are NOT in the
senzingsdk-runtime base, so the `WITH_MSSQL=1` path installs `msodbcsql18` +
`unixodbc` from the Microsoft apt repo in the `backend-libs` stage, then stages
`libodbc.so.2`, `libodbcinst.so.2`, `libltdl.so.7`, the driver `.so`, the
krb5/GSSAPI/LDAP closure (same family as PG) into er/lib, copies the driver tree
under `/opt/microsoft`, and writes `/etc/odbcinst.ini` + `/etc/odbc.ini`.

### KNOWN BLOCKER (current environment): Microsoft apt repo SHA1 signing key

Installing `msodbcsql18` from `packages.microsoft.com` currently FAILS on
Debian because Microsoft's repo `InRelease` is signed with a key whose binding
signature uses SHA1, and current `apt`/`sqv` reject SHA1 as insecure (policy
cutoff 2026-02-01):

```
Err: https://packages.microsoft.com/debian/12/prod bookworm InRelease
  Signing key ... SHA1 is not considered secure since 2026-02-01
```

This is an upstream Microsoft signing issue (the Python `sz_rabbit_consumer-v4`
Dockerfile hits the same wall today). Because of it, the MSSQL variant could NOT
be built/verified locally. The MSSQL COPY closure is derived from the plan + the
verified fact that cc-debian13 already provides `libstdc++`/`libgcc_s`, plus the
two structural fixes proven on the PG path (stage into er/lib; dereference
symlinks). It must be re-measured once Microsoft refreshes the repo key, or by
fetching the `.deb` directly and verifying its `ldd` closure offline. The
`packages.microsoft.com/config/debian/12/...` URL may also need updating to a
Debian 13 path if/when Microsoft publishes one.

## Regenerating the manifest on version bumps

The PG closure can be regenerated with this `ldd` walk inside the runtime image,
then diffed against the distroless-provided set:

```bash
docker run --rm --entrypoint bash senzing/senzingsdk-runtime:<ver> -c '
  declare -A seen; queue=(/usr/lib/x86_64-linux-gnu/libpq.so.5 /opt/senzing/er/lib/libpostgresqlplugin.so)
  while [ ${#queue[@]} -gt 0 ]; do cur=${queue[0]}; queue=("${queue[@]:1}");
    [ -n "${seen[$cur]}" ] && continue; seen[$cur]=1;
    for d in $(ldd "$cur" 2>/dev/null | awk "{print \$3}" | grep ^/); do
      [ -z "${seen[$d]}" ] && queue+=("$d"); done; done
  for k in "${!seen[@]}"; do basename "$k"; done | sort -u'
```
