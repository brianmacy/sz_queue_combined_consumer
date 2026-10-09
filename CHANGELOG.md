# Changelog

Sections headed `0.4.1 — …` ship in tag `v0.4.1` (2026-10-09); all sections headed `0.4.0 — …` ship together in tag `v0.4.0` (2026-10-06); all sections headed `0.3.0 — …` shipped in tag `v0.3.0` (2026-09-23). Each keeps the date it landed on `main`.

## Unreleased

* **`--version` comes from the release tag.** Release builds set `RELEASE_VERSION`
  from the git tag (`core::VERSION`), and the release workflow fails if any
  binary's `--version` does not match the tag. Other builds report the workspace
  `Cargo.toml` version.

### CI / release

* **Prebuilt Linux release binaries.** New `.github/workflows/release.yml`
  (on a `v*` tag push, or `workflow_dispatch` with an existing `tag` to release
  retroactively) builds `sz_rabbit_combined_consumer`,
  `sz_sqs_combined_consumer` and `sz_activemq_combined_consumer` for linux
  x86_64 (`ubuntu-24.04`) and aarch64 (native `ubuntu-24.04-arm`) inside
  `senzing/senzingsdk-runtime:4.3.3`, and attaches
  `sz_queue_combined_consumer-<tag>-linux-<arch>.tar.gz` (three bins + README,
  LICENSE, CHANGELOG) plus `SHA256SUMS` to the GitHub Release, whose notes come
  from this file's section(s) for that version. Re-running for an existing
  release overwrites its assets. The binaries link only `libSz.so` + glibc +
  `libgcc_s` (enforced by a `readelf` check; TLS is rustls); `libSz` is a glibc
  shared library, so a static/musl build is not possible. See README
  "Download / release binaries".

## 0.4.1 — process exit can no longer hang; nohup honored (2026-10-09)

### Fixed

* **The forced-exit paths now terminate with `libc::_exit(2)` instead of
  `std::process::exit`** (`leak_and_exit`, and `teardown_and_exit` when
  `Sz_destroy()` overruns its grace or its thread cannot spawn; stdout/stderr
  are flushed first). **Root cause (reproduced on Linux, Senzing 4.3.3):**
  `exit(3)` runs libSz's thread-local destructor, which makes its own database
  call (`PQexec` → `poll`); libpq has no timeout, so when the database
  connection is unresponsive at exit the destructor never returns and `exit(3)`
  hangs — in the very condition these paths exist to escape. No lock held by a
  wedged worker is involved. It is intermittent because a connection that gets
  an RST/FIN fails fast. In production (2026-07-27, DB restart) 17 of 20
  consumers logged `forcing process exit` and never exited, so
  `RestartPolicy=on-failure` never fired and the fleet silently ran at 57%
  capacity for 80 minutes.
* **The normal exit path is bounded too.** After a completed teardown the driver
  still exits with `std::process::exit` (so atexit handlers and destructors run),
  but a watchdog forces `_exit` with the same code if that exit has not finished
  within `EXIT_GRACE` (2 s), so no exit path can hang on exit-time native code.
* **`nohup` is honored: an inherited ignored SIGHUP stays ignored** (#33).
  `nohup driver &` did not survive logout: queue mode always installed a tokio
  SIGHUP handler (`queue_loop::ShutdownSignals`) and file / pure-redoer mode's
  `ctrlc` `termination` feature also claims SIGHUP, either replacing the
  inherited `SIG_IGN`, so the logout SIGHUP became a graceful shutdown. The
  disposition is now read once at startup (`runtime::sighup_inherited_ignored`,
  before any handler); when it was `SIG_IGN`, queue mode installs no SIGHUP
  handler and the `ctrlc` path puts `SIG_IGN` back, with one startup info line.
  Otherwise SIGHUP is still a graceful shutdown; SIGINT/SIGTERM unchanged. New
  e2e tests (RabbitMQ, SQS, ActiveMQ queue mode; file mode via a FIFO) start the
  driver with SIGHUP ignored and require it to keep consuming after SIGHUP.
* **Operating notes.** A hung consumer looks healthy to Docker (`Up`, flat
  `RestartCount`); alert on the broker-side consumer count. **Set the container
  stop timeout to 30 s** (`docker run --stop-timeout 30`, compose
  `stop_grace_period: 30s`): the worst-case SIGTERM path is the 10 s worker-join
  grace + transport close (≤ 5 s SQS/ActiveMQ) + 5 s native teardown + 2 s exit
  watchdog, and Docker's default 10 s SIGKILLs before the forced-exit paths run. Client-side TCP keepalive /
  `tcp_user_timeout` for the database connection may bound how long an
  unresponsive connection blocks the engine (not verified that Senzing's
  connection string passes them through).

### Changed

* **`tokio-util` is pinned to `=0.7.19`** (`crates/activemq/Cargo.toml`).
  0.7.20 breaks the build of `fe2o3-amqp` 0.18.2, the latest release; the pin
  keeps `cargo update` and Dependabot from pulling it in.
* Dependencies refreshed (`cargo update`).

## 0.4.0 — core Transport loop alignment + ActiveMQ Artemis consumer (2026-10-06)

One section for everything since `v0.3.0`, describing the behavior as it
ships. All three binaries now run queue mode on one shared core loop
(`queue_loop::run` behind a `Transport` trait), so the totals line, reject
marker, poison handling, signals and shutdown are identical on every
transport.

### Added

* **`sz_activemq_combined_consumer`** (`crates/activemq`): consumes an
  **Apache ActiveMQ Artemis** ANYCAST queue over **AMQP 1.0** (`fe2o3-amqp`,
  rustls only; compiles neither `lapin` nor the AWS SDK). Flags / env:
  `-u/--url` `SENZING_ACTIVEMQ_URL` (`amqp://` / `amqps://`, credentials may
  be in the URL), `--user` / `--password` (`SENZING_ACTIVEMQ_USER` /
  `SENZING_ACTIVEMQ_PASSWORD`, each overrides the URL's; none = anonymous),
  `-q/--queue` `SENZING_ACTIVEMQ_QUEUE` (name or FQQN `address::queue`),
  `--prefetch` (default threads + 2), `--mq-recheck-secs`, plus every shared
  flag (file mode, pure redoer, transform plugin). The receiver attaches with
  the `queue` source capability (anycast); success = `accepted`; a reject =
  `rejected` with the reason in its error description, which Artemis routes
  to the address's dead-letter address (dropped if none is configured);
  shutdown `released` the unsettled deliveries (no delivery-count
  increment); long records are only logged, never dead-lettered; a message
  the receiver cannot decode is dead-lettered as malformed and the driver
  keeps running; a lost broker link is fatal (exit 255). No queue-depth probe
  (Artemis has no AMQP depth verb). Docker:
  `--build-arg BIN=sz_activemq_combined_consumer`. See README *ActiveMQ
  Artemis specifics*.
* **SQS: every DLQ copy carries a String message attribute `SzReason`** with
  the reject reason (engine error text, or `malformed record: <parse
  error>`), sanitized to SQS's allowed characters and capped at 1 KiB (less
  when the body leaves less room under 256 KiB). Receive it with
  `MessageAttributeNames=All`.
* **SQS: `--mq-recheck-secs` / `SENZING_MQ_RECHECK_SECONDS`** (default 30, as
  on RabbitMQ): cadence of the `ApproximateNumberOfMessages` depth probe and
  of the `MQ drained (depth 0)` / `MQ active (depth N)` transition log.

### Changed

* **BREAKING (stdout format): final total line.** Every mode prints
  `Processed total of N adds, M redo records (R rejected, D redo dropped, E
  errors)`. `N` is successful adds only on every transport and file mode
  (RabbitMQ used to include dead-lettered records; the `Processed N adds, …
  records per second` throughput line follows the same `N`), and `R` counts
  every dead-letter / reject-file record (RabbitMQ poison messages were
  uncounted). *Migration:* the prefix and first number are unchanged, so
  `starts_with("Processed total of ")` + first-token parsers keep working; a
  RabbitMQ figure that relied on rejects being included must add `R`; a
  regex anchored on `(D redo dropped` must allow the leading `R rejected, `.
* **BREAKING (stdout format): one reject marker on every transport:**
  `REJECTING: DATA_SOURCE : RECORD_ID -> <reason>` (an unparseable body has
  no DS/ID: `REJECTING:  :  -> malformed record: <parse error>`). It replaces
  RabbitMQ's `REJECTING due to bad data or timeout: DS : ID` and
  `REJECTING: DS : ID` (long-record give-up), and SQS's `Sending to
  deadletter: DS : ID` and `REJECTING unparseable SQS message`. A poison body
  also logs one warn `DEAD-LETTERING malformed record: <error> [<first 2048
  chars>…truncated]`. The engine-reject worker warn `REJECTING due to bad data
  or timeout [worker N]: DS : ID -> <engine error>` is unchanged; a
  transform-plugin reject now warns `REJECTING [worker N]: DS : ID -> record
  transform failed: …` / `… transformed record is invalid: …`. *Migration:*
  alert on `^REJECTING: ` and split on ` -> ` for the reason.
* **BREAKING: records still in flight at shutdown are released for
  redelivery on every transport, never dead-lettered or counted.** At
  SIGINT/SIGTERM/SIGHUP the loop drains within the 10 s grace; whatever is
  still unsettled then — queued-but-unstarted or still inside a worker — is
  left for redelivery (RabbitMQ: unacked, requeued on connection close; SQS:
  un-deleted, redelivered after the visibility timeout; ActiveMQ:
  `released`). A record still inside a worker prints `Still processing (…
  min): DS : ID`. RabbitMQ used to dead-letter those with `REJECTING
  in-flight-in-worker on shutdown (…)`; `add_record` with an existing key
  replaces the record, so that put valid records in the DLQ on every rolling
  restart. *Migration:* drop DLQ triage / alerts keyed on the shutdown reject;
  expect a redelivery (re-add) instead.
* **BREAKING: `SENZING_PREFETCH` / `--prefetch` is the TOTAL in-flight cap on
  every transport** (messages received and not yet settled). Defaults:
  RabbitMQ and ActiveMQ threads + 2, SQS 2 × threads (Senzing v4 SQS
  consumer parity; the effective SQS default is unchanged). On SQS it used to
  be the extra beyond the worker count (cap = threads + prefetch).
  *Migration:* if you set it on SQS, set your old `threads + prefetch`.
* **BREAKING: `--prefetch` below the worker count is raised to the worker
  count** (with a warning) on every transport; RabbitMQ `--prefetch 0` used to
  mean an unlimited `basic_qos`.
* **BREAKING: file mode shares redo.** `--file` follows `--redo-percent` like
  queue mode (one redo fetcher; at EOF the whole pool falls through to redo)
  and, at redo% > 0, exits 0 only once every record has an outcome and
  `get_redo_record()` is empty on 2 consecutive probes `--redo-sleep-secs`
  apart with no redo outstanding. At the default redo% (20) a file run now
  waits for that drain (≈ one `--redo-sleep-secs` tail). File mode enforces
  ≥ 2 threads for 0 < redo% < 100. *Migration:* `--redo-percent 0` for the
  old pure-loader behavior.
* **SIGHUP is a graceful shutdown in queue mode** on every transport (drain,
  exit 0, final total), like SIGINT/SIGTERM; it used to kill the process.
* **SQS shutdown is bounded by the same 10 s grace as RabbitMQ** (a worker
  stuck in an engine call used to keep the process alive until SIGKILL), and
  the stats thread is joined within that deadline on every transport.
* **SQS: 30 consecutive `ReceiveMessage` failures are fatal** (1 s apart,
  SDK-internal retries off for that call; exit 255); any success resets the
  count. It used to retry forever.
* **SQS settle:** an in-flight slot is held until the message's
  `DeleteMessageBatch` returns, so `ApproximateNumberOfMessagesNotVisible`
  never exceeds the cap. Deletes are sent at once and batch naturally under
  load (every delete settled while the previous call was in flight, up to 10
  per call; no timer); pending deletes are flushed at close, bounded at 5 s
  (an unflushed delete only redelivers that message after its visibility
  timeout).
* SQS: `All N threads are stuck on long running records` counts every record
  past `LONG_RECORD`, not only those extended on that tick.
* SQS: `--queue-url`, `--wait-time` and `--max-messages` are validated before
  engine init; `--visibility-timeout` outside 0..=43200 is a startup error
  (exit 1; the `<= --long-record` warning stays).
* Shared flags (file mode, redo%, threads, redo sleep, long record,
  transform, `--info`, `--debugTrace`) are one `config::CommonArgs` in every
  binary: same names, env vars, defaults and `--help` text.
* Dependencies: sz-rust-sdk v4.3.2 (`rev = 2787281…`; its FFI moved to the
  `sz-rust-sdk-ffi` git crate, allowed in `deny.toml`), aws-config 1.10.1,
  aws-sdk-sqs 1.105.0, clap 4.6.6, libc 0.2.189; GitHub Actions
  actions/checkout 7.0.1, actions/cache 6.1.0, docker/setup-buildx-action
  4.3.0 (hash-pinned); `fe2o3-amqp` 0.18 (ActiveMQ binary only).

### Removed

* The `redo_backlog` / `redo_backlog_slope` fields of the `Combined stats:`
  line and the redo-floor (`__REPAIR__` loop) guard warning + raw-record
  sampling: never functional once the `count_redo_records()` scan was dropped.
* Internal: `queue_run::install_signals`, `Policy::count_rejects_in_total`,
  `Policy::dead_letter_in_worker_at_shutdown`, `DeadLetterReason::Shutdown`;
  core no longer exports the RabbitMQ-named `INSTANCE_NAME` (each binary owns
  its engine instance name).

### Fixed

* SQS: the poller's hand-off to the loop is cancelable by shutdown (it could
  block on a full channel).
* SQS: a hung endpoint at shutdown can no longer hold the process in the
  close-time delete flush (bounded at 5 s; see *Changed*).

### Known limitations

* Shutdown can outlast the 10 s grace by the transport close: SQS delete
  flush ≤ 5 s, ActiveMQ link/session/connection close ≤ 5 s, RabbitMQ
  connection close unbounded (then the 5 s native-teardown bound).
* With `--prefetch` > 2 × threads, a signal can wait for a worker to finish
  while the loop hands a delivery to a full worker channel; it cannot happen
  at the defaults.

### CI / tests

* `ci.yml` `integration` job: `apache/artemis:2.57.0` service (credentials
  enforced, Jolokia exposed, bash `/dev/tcp` health check), and the ActiveMQ
  e2e suite after the RabbitMQ and SQS ones; Docker matrix row for
  `sz_activemq_combined_consumer` (`both` DB closure).
* `IT_REQUIRE_INFRA=1` (set in CI) turns every e2e `SKIP` into a failure on
  all three suites; the truth-set tests now find the workspace-root
  `truth-sets` submodule (they silently skipped in CI before).
* New real-infra e2e: ActiveMQ suite (redo% 0/20/100, Data and JMS-text
  bodies, dead-letter address incl. an undecodable message, stuck-worker
  deadline, SIGHUP, link loss, bad credentials/URL, in-flight cap); RabbitMQ
  rejects counted + dead-lettered, in-worker record requeued at shutdown,
  in-flight cap, SIGKILL of one of two drivers mid-load loses nothing; SQS
  `SzReason` + in-flight cap. The pure-redoer e2e tests (RabbitMQ, ActiveMQ)
  seed a redo backlog and assert it is non-zero before the run. The whole
  e2e run stays well under the 500-record EVAL license cap on a fresh
  repository.
* README: *Tests and CI* and *Running the e2e tests locally*.

## 0.3.0 — SQS dead-letter queue restored + sibling parity (2026-09-23)

Audit of the combined driver against the standalone drivers it subsumed
(`sz_sqs_consumer-v4`, `sz_rabbit_consumer_rust`, `sz_simple_redoer_rust`).

* **FIX (data loss): SQS rejects go to the dead-letter queue again.** The
  combined SQS backend `DeleteMessage`d rejected records; an explicit delete
  never triggers the redrive policy, so bad records were destroyed. Restored
  v4 behaviour: resolve the DLQ at startup (`--dead-letter-queue-url` /
  `SENZING_SQS_DEAD_LETTER_QUEUE_URL`, else the source queue's `RedrivePolicy`
  → `deadLetterTargetArn` → `GetQueueUrl` with the owner account id), print
  `DeadLetter: <url>`, then on reject `SendMessage` the body verbatim to the
  DLQ and only then delete the source message (`Sending to deadletter: DS : ID`).
  A failed DLQ send leaves the source message for redelivery. **No DLQ =
  refuse to start** (exit 255) unless `--allow-no-dlq` / `SENZING_SQS_ALLOW_NO_DLQ`
  is given, which logs every deleted reject with its body. FIFO DLQs get
  `MessageGroupId` + `MessageDeduplicationId`.
* **FIX (correctness): SQS visibility heartbeat.** Records processing past
  `LONG_RECORD × (n+1)` are extended to `(n+2) × LONG_RECORD` via
  `ChangeMessageVisibility` (v4 cadence, clamped at 12 h), so a slow record is
  no longer redelivered mid-`add_record`. `All N threads are stuck …` warning
  restored.
* **SQS operational parity:** `DeleteMessageBatch` (10 per call, 1 s flush),
  `--prefetch` / `SENZING_PREFETCH` overshoot (in-flight cap = threads +
  prefetch, default threads), receive size capped by free room, throughput
  line every 10 000 adds, `Engine stats:` + `Combined stats:` status line with
  `mq_depth` = `ApproximateNumberOfMessages`, redo in-flight long-record
  monitor in mixed mode, `Still processing (… min): DS : ID` shutdown dump.
* **core:** stats thread (`stats::stats_loop`, `StatsPayload`) and the
  throughput line (`stats::ThroughputTicker`) moved from the RabbitMQ crate into
  core so both async backends share them (RabbitMQ output unchanged).
* **pure redoer:** now prints the final `Stats: N redo records processed,
  R/sec, runtime: Ts` and the `Processed total of 0 adds, N redo records (…)`
  stdout line every other mode emits.
* **tests:** `crates/sqs/tests/sqs_e2e.rs` against ElasticMQ (new CI service
  container): both a parse reject and an engine reject land verbatim in the
  discovered DLQ with the source drained; no-DLQ refuses to start, `--allow-no-dlq`
  starts and names the deleted reject. Unit tests for ARN parsing, redrive
  policy parsing, FIFO detection, the heartbeat thresholds, and the new args.
* **docs:** README SQS section (DLQ semantics, FIFO, heartbeat, IAM, env rows),
  exit-code convention (1 config / 255 fatal) documented; `DOCKER_NOTES.md`
  restored from `sz_rabbit_consumer_rust` (the Dockerfile referenced it).
* **deps / supply chain:** `sz-rust-sdk` pinned by commit
  (`rev = b6be5cb…`, = tag v4.3.1) instead of a movable tag. `cargo update`:
  h2 0.4.19, rustls 0.23.45 (+ aws-lc-rs 1.18.1 / aws-lc-sys 0.45.0 /
  rustls-webpki 0.103.15) clearing RUSTSEC-2026-0258 and RUSTSEC-2026-0285;
  `cargo audit` and `cargo deny check` are both clean and `deny.toml` carries
  no advisory ignores (the three stale ones were removed).

## 0.3.0 — file mode writes rejected records to a JSONL reject file (2026-09-23)

* **`--reject-file` / `SENZING_REJECT_FILE` (file mode).** A file has no DLQ, so
  engine rejects (bad input, `SENZ0010` retry timeout, `SENZ0082`) and
  unparseable lines were previously only *counted* in file mode — the record was
  gone and, because the resume watermark advanced past it, `--skip-lines` could
  not recover it either. Every rejected line is now appended verbatim to a JSONL
  side file (default `<input>.rejected.jsonl`, created lazily, append mode) for
  reprocessing with `--file`. The end-of-run summary reports how many were
  written and where. If any reject could not be written (unwritable path) the
  run exits non-zero, since those records then exist only in the log.
* **Rejects are now logged (all backends).** `worker::process_load` logs
  `REJECTING due to bad data or timeout [worker N]: DS : ID -> <engine error>` —
  previously the `BadInputOrTimeout` branch produced no log line at all, so the
  engine error text for a rejected record was never recorded anywhere.
* e2e `e2e_file_loader` now also feeds an unregistered data source (engine
  bad-input path) and asserts both rejects appear verbatim in the reject file.
* **FIX (data loss): DB-connection-lost / DB-transient errors are FATAL again.**
  `classify_error` had widened the drop set from `RetryTimeoutExceeded` (the
  sibling drivers' policy) to the SDK's whole `is_retryable()` family, which
  also covers `DatabaseConnectionLost` / `DatabaseTransient`. During a DB outage
  that dead-lettered every record un-added at full consume rate (RabbitMQ: DLQ;
  SQS: deleted outright; redo: dropped) and exited 0. Restored to
  `is_bad_input() || is(RetryTimeoutExceeded) || SENZ0082`; the inverted unit
  test now asserts Fatal, and the sibling `bad_input_family` /
  `configuration_license_and_init_errors_are_fatal` tests are restored.
* **FIX (data loss): `LONG_RECORD` / `--long-record` must be >= 1** (clap range
  validation). At 0 the long-record monitor treated every in-flight delivery as
  stuck and dead-lettered the whole queue within one tick. The sibling drivers
  fell back to 300 on 0/garbage.
* Redo-drop log line now includes the engine error text (`-> {e}`), redoer
  parity; previously only the record id was logged.

## 0.3.0 — bump MSRV to 1.94.1 + modern AWS TLS (drops advisory ignores) (2026-07-21)

* **MSRV `1.88` → `1.94.1`** (`rust-version`, CI toolchain pins, Dockerfile
  `rust:1.94.1`). The MSRV-aware resolver now selects the current AWS SDK
  (`aws-sdk-sqs 1.103`, `aws-config 1.9`, `aws-smithy-http-client 1.2`).
* **Modern TLS only — vulnerability cleared.** The SQS SDK crates are pulled with
  `default-features = false` + `default-https-client` (the `rustls-aws-lc` stack:
  rustls 0.23 / `rustls-webpki 0.103.13`), dropping the SDK's legacy `rustls`
  feature (hyper-0.14 + rustls 0.21 → the vulnerable `rustls-webpki 0.101.7`).
  The whole legacy stack (`rustls 0.21`, `hyper 0.14`, `aws-sdk-sso`, ...) leaves
  the graph, so **RUSTSEC-2026-0098/-0099/-0104 no longer apply and their
  `deny.toml` ignores are removed** — `cargo deny` is clean with no suppressions.
* Side effect: aws-config's `sso` + `credentials-process` providers are dropped
  (they re-pull the legacy stack). The standard provider chain (env vars, `~/.aws`
  profile, IMDS/ECS role, web-identity) is unaffected; re-add those features if
  SSO / `credential_process` auth is required.
* Verified on the 1.94.1 toolchain: build + clippy `-D warnings` + fmt + full
  `cargo deny` (no ignores) + workspace unit tests, both bins' dep isolation.

## 0.3.0 — multi-backend workspace + Amazon SQS driver (2026-07-21)

* **Cargo workspace.** Split the single crate into `crates/core`
  (`sz-combined-consumer-core`, lib — worker pool, redo fetcher, stats, live
  config reload, file loader, shared `runtime` bring-up/shutdown) plus one thin
  binary crate per message backend. Compile-time backend selection: each bin
  pulls ONLY its own client — `cargo build -p sz_rabbit_combined_consumer` never
  compiles the AWS SDK; `cargo build -p sz_sqs_combined_consumer` never compiles
  `lapin` (verified via `cargo tree`). No feature flags, no runtime switch.
* **`crates/sqs` (`sz_sqs_combined_consumer`, new) — Amazon SQS backend**
  (standard queues). Long-poll `ReceiveMessage` → shared worker pool →
  `DeleteMessage` on success; bad data is deleted (drop), fatal/shutdown leaves
  messages un-deleted so the visibility timeout redelivers (at-least-once).
  Receipt-handle strings map to synthetic `u64` correlation ids for the shared
  worker code. `--queue-url`, `--visibility-timeout` (must exceed worst-case
  processing time), `--wait-time`, `--max-messages`. Credentials/region via the
  standard AWS provider chain.
* **`crates/rabbit` (`sz_rabbit_combined_consumer`)** — the AMQP loop moved here
  from core; behavior unchanged. Both bins share the file loader and pure redoer.
* Workspace uses **resolver "3"** (MSRV-aware) so the AWS SDK resolves to the
  latest versions compatible with `rust-version = 1.88`.
* `deny.toml`: the MSRV-1.88-pinned AWS TLS stack (`aws-smithy-http-client 1.1.9`
  → rustls 0.21 → `rustls-webpki 0.101.7`) trips RUSTSEC-2026-0098/-0099/-0104;
  ignored with justification (0104 N/A — no CRLs; 0098/0099 low risk vs AWS
  endpoints). Drop these when the workspace MSRV moves to >= 1.94 (which lets the
  patched rustls stack resolve). SQS binary only.
* CI: workspace build/test; the Docker matrix builds BOTH binaries (`BIN` arg) ×
  the DB-driver closure. Integration tests run against `sz_rabbit_combined_consumer`.
* Repository being renamed to `sz_queue_combined_consumer` (binaries keep their
  per-backend names).

## 0.3.0 — file-input load mode + --skip-lines (2026-07-20)

* **`src/file_loader.rs` (new) — load JSONL records from a single file instead of
  RabbitMQ.** Selected by `--file`/`SENZING_INPUT_FILE` (mutually exclusive with
  `--url`/`--queue`). Reuses the shared `worker::worker_loop` pool (load-preferring,
  no redo): a reader thread feeds each line as a `LoadItem` keyed by absolute line
  number; a consumer thread counts outcomes. Blank lines are skipped; unparseable
  lines are dead-lettered (logged + counted) without aborting. Runs to EOF and exits
  0. Pure `std::thread` shape (no AMQP, no tokio), same graceful-shutdown/bounded-
  teardown exit path as the pure redoer.
* **`--skip-lines N`/`SENZING_SKIP_LINES` — resume an interrupted load.** Skips the
  first N physical lines. Because workers complete out of order, the driver tracks a
  contiguous-completion watermark and prints a SAFE `--skip-lines` offset at
  shutdown, so a resume never skips an unprocessed line (at most a few in-flight
  lines past the watermark are reprocessed — `add_record` is idempotent).
* Tests: `ResumeTracker` unit tests (watermark advance / out-of-order / stale) plus
  `e2e_file_loader` (spawns the binary in file mode, asserts clean EOF exit, correct
  add count, and dead-lettering of a malformed line).

## 0.3.0 — remove count_redo anti-pattern; config/license diagnostics (2026-07-17)

* **`src/combined.rs`, `src/pure_redoer.rs` — removed `count_redo_records()`.** It issued
  `COUNT(*) FROM SYS_EVAL_QUEUE` (a full table scan) once per stats interval, which dominated
  DB user-CPU at scale. The redo-backlog gauge now reports `None` with a
  `TODO(reporting)` to restore it via a cheap source (engine redo counters or a DB-side
  estimate) rather than a full scan.
* **`tests/integration_test.rs` — send SIGTERM via the `kill(2)` syscall, not the `kill`
  binary (the actual reason Integration Tests had never gone green).** The
  `senzing/senzingsdk-runtime` CI container ships NO `kill` executable on PATH, so the
  `sigterm()` helper's `Command::new("kill")` failed with ENOENT — and the swallowed
  `let _ = …` meant SIGTERM was silently never sent, so all four spawn-a-binary-and-SIGTERM
  e2e tests hung to the 30s SIGKILL. `sigterm()` now calls `libc::kill()` directly and asserts
  the syscall succeeds (no silent failure). This — not native teardown — was the root cause;
  the `teardown_and_exit` hardening below (merged from `main`, PR #5) is retained as defensive
  belt-and-suspenders for a genuinely wedging teardown in production.
* **`src/config_reload.rs` — `reconcile()` keys off the registered DEFAULT changing**
  (`get_default_config_id()` vs an adopted-default sentinel), **not** `get_active_config_id()`.
  On the settings-JSON init path the engine returns `get_active_config_id()==0` even after a
  valid init AND after `reinitialize(default)` succeeds (engine defect **GDEV-4313**), so keying
  on the active id caused a ~60s reinit storm → connection churn → prepared-statement 8179 storm
  (**GDEV-4314**). The adopted default is seeded at startup to the default the engine loaded, so
  reinit fires exactly once per real registered-default change. The active id is still logged for
  GDEV-4313 visibility but is not used for the reload decision. (Supersedes the earlier
  `get_active_config_id`-based reconcile; the `LAST_APPLIED`-style intent-tracking is restored,
  now justified by the GDEV-4313 evidence.)
* **Config/license diagnostics** — added `log_startup_config()` (logs `CONFIG AT INIT:
  active=… default=…` once per process after init), called from `combined.rs` and
  `pure_redoer.rs`; plus `LICENSE AFTER INIT` / `LICENSE AFTER REINIT` logging to detect a
  license drop across `reinitialize`.
* **CI — `.github/workflows/ci.yml`: fix Integration Tests submodule checkout.** The
  `integration` job runs inside `senzing/senzingsdk-runtime`, which ships without `git`, so
  `actions/checkout` fell back to the REST tarball API and could not fetch the `truth-sets`
  submodule (job failed at checkout in ~34s). Added an `Install git` step before checkout so the
  submodule is fetched via native git.

## 0.3.0 — live config auto-reload (2026-07-16)

* **`src/config_reload.rs` (new)** — adopt a new registered DEFAULT engine config WITHOUT a
  process restart, so an operator can bump the config (e.g. apply a
  `setGenericThreshold ... "behavior":"NAME" ... "sendToRedo":"No"` tweak) and have every worker in
  every process converge within ~one poll interval.
  * `poll(&env)` — PERIODIC trigger called per-record; a PROCESS-GLOBAL throttle collapses all callers
    to one `get_default_config_id` select per `SENZING_CONFIG_RELOAD_SECS` (default 60) per process.
  * `reinit_if_stale(&env)` — ERROR-DRIVEN trigger: on an `add_record`/`process_redo_record` error, if
    the active config drifted from the default the engine is reinitialized and the caller RETRIES once;
    if active == default the error is genuine and propagates unchanged.
  * `reconcile()` — double-checked reinit behind a process-global `Mutex` (re-checks `active != default`
    inside the lock) so concurrent stale-config errors do not stack `reinitialize` calls.
  * Logs every refresh with both IDs: `CONFIG REFRESHED: engine reinitialized from config {old} -> {new}`.
* Wired into `src/worker.rs` (`process_load`, `process_redo`) and `src/redo.rs` (fetcher loop).
* Uses `SzEnvironment::reinitialize` (documented thread-safe; existing engine handles stay valid).
* No new dep; env knob `SENZING_CONFIG_RELOAD_SECS` (default 60, `0` disables periodic; error-path stays on).

## 0.3.0 — bound native teardown on shutdown (issue #4, merged from main / PR #5)

* **`src/main.rs` — bound the native teardown and guarantee a prompt process exit
  on every shutdown path.** The four e2e integration tests hung >30s on SIGTERM
  and were SIGKILLed (never exiting 0). Root cause: on the CLEAN shutdown path
  (all engine threads joined) both `run_combined` and `run_pure_redoer` called
  `SzEnvironmentCore::destroy_global_instance()`, which invokes `Sz_destroy()` —
  an uninterruptible native FFI call with no timeout that BLOCKS indefinitely once
  the worker threads that made engine calls have exited (the engine's per-thread
  DB connections / native state outlive them). This teardown was never exercised
  in CI before: the sibling drivers have no spawn-binary + SIGTERM e2e tests, and
  this suite only began running once PR #3 fixed the submodule checkout. The fix
  runs `destroy_global_instance()` on a dedicated thread bounded by
  `TEARDOWN_GRACE` (5s), then `std::process::exit(code)` with the correct code
  (0 on clean success). Because we exit rather than return, the tokio-runtime drop
  and `Arc<env>` drops (other candidate wedges called out in the issue) are also
  bypassed. stdout is flushed first so the e2e-scraped "Processed total ..." line
  is never lost. The not-joined leak-on-exit path likewise hard-exits with the
  correct code.

## 0.1.0 (unreleased)

Initial scaffold implementing the combined load+redo design
(`~/.claude/plans/dbperf_combined_consumer_design.md`):

* One process = one `Sz_init`; N `std::thread` engine workers (default 12),
  each with its own engine handle.
* `SENZING_REDO_PERCENT` (default **20**) splits the pool into
  redo-preferring / load-preferring classes with non-blocking cross-over
  fallback dispatch; endpoints branch cleanly (0 = pure consumer, no redo
  calls; 100 = pure redoer, no AMQP/tokio).
* tokio + lapin AMQP layer inherited from `sz_rabbit_consumer_rust`
  (`prefetch = threads + 2` overshoot to mask the ack round-trip); single
  serial redo fetcher + tiny bounded channel inherited from
  `sz_simple_redoer_rust`, with a 2 s drain-tail re-probe for redo cascades.
* `Engine stats:`-prefixed get_stats logging plus a `Combined stats:` JSON
  status line (rates, MQ depth, `count_redo_records` backlog + EWMA slope,
  measured `redo_share_effective`, derived mode).
* Redo-floor (`__REPAIR__` loop) guard with raw-record sampling.
* Hardened shutdown carried over from both siblings: durable fatal signaling,
  bounded 10 s grace, DLQ vs leave-unacked split for remaining deliveries,
  skip-destroy-on-stuck-worker (leak-on-exit over use-after-free).
* Distroless-cc Dockerfile with the canonical Senzing staging section and the
  `WITH_POSTGRES`/`WITH_MSSQL` build args, matching the sibling repos.
