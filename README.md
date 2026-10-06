# sz_rabbit_combined_consumer

Combined Senzing **load + redo** driver in Rust. One binary runs both roles —
the load role of [`sz_rabbit_consumer_rust`](../sz_rabbit_consumer_rust)
(queue → `add_record`) and the redo role of
[`sz_simple_redoer_rust`](../sz_simple_redoer_rust) (`get_redo_record` →
`process_redo_record`) — in a single worker pool, governed by a single
`SENZING_REDO_PERCENT` knob. It does **not** replace those standalone drivers;
it combines their two roles into one process so capacity can flow between load
and redo. Like its siblings, the container is **distroless** (no interpreter,
no shell) and glue-layer errors surface at compile time.

Design document: `~/.claude/plans/dbperf_combined_consumer_design.md`.

## Workspace / backends

This is a Cargo **workspace** so the shared engine-processing core is written
once and each message backend is a separate binary that pulls **only** its own
client (compile-time backend selection — no runtime switch, no feature flags):

| Crate | Kind | Backend | Backend dep |
|---|---|---|---|
| `sz-combined-consumer-core` | lib | — (worker pool, redo, stats, config reload, file loader) | none |
| `sz_rabbit_combined_consumer` | bin | RabbitMQ | `lapin` |
| `sz_sqs_combined_consumer` | bin | Amazon SQS (standard queues) | `aws-sdk-sqs` |
| `sz_activemq_combined_consumer` | bin | Apache ActiveMQ Artemis (AMQP 1.0, anycast queues) | `fe2o3-amqp` (rustls) |
| `sz-record-transform` | lib | — (record-transform plugin C ABI + Rust export macro) | none |
| `sz-record-transform-example` | cdylib | — (example plugin; used by tests) | none |

`cargo build -p sz_rabbit_combined_consumer` never compiles the AWS SDK, and
`cargo build -p sz_sqs_combined_consumer` never compiles `lapin`. All three binaries
also support the shared **file-input** mode (`--file`, below) and the pure
redoer (`--redo-percent 100`). The SQS binary takes `--queue-url` /
`SENZING_SQS_QUEUE_URL` (plus `--visibility-timeout`, `--wait-time`,
`--max-messages`, `--prefetch`, `--dead-letter-queue-url`); credentials/region
come from the standard AWS provider chain. See [SQS specifics](#sqs-specifics).
The ActiveMQ binary takes `--url` / `SENZING_ACTIVEMQ_URL` and `--queue` /
`SENZING_ACTIVEMQ_QUEUE` and compiles neither `lapin` nor the AWS SDK; see
[ActiveMQ Artemis specifics](#activemq-artemis-specifics).

> NOTE: the repository is being renamed to `sz_queue_combined_consumer` to
> reflect the multi-backend scope (the binaries keep their per-backend names).

## Why combined

One worker pool = one DB-connection pool. In the split consumer+redoer topology
the two pools are sized at launch and cannot borrow from each other: the redoer
pool lags during load (SYS_EVAL_QUEUE grows) and the consumer pool idles during
the redo tail. Here the same capacity flows to whichever work exists:

| redo% | Behavior | Same work as |
|---|---|---|
| 0 | Pure loader. No redo fetcher, zero redo-related calls. | `sz_rabbit_consumer` |
| 100 | Pure redoer. No broker connection is opened and no tokio runtime is built; the broker URL/queue settings may be unset. | `sz_simple_redoer` |
| (0,100) | While the MQ is busy, redo gets exactly \|B\|/N of the pool (a hard share — size it for load-phase keep-up). When the MQ drains, ALL workers fall into redo automatically; a new publish flips them back instantly (push-based, no polling). | both |

## Concurrency model

* **One `Sz_init` per process**; every thread derives its own engine handle
  (`env.get_engine()` is a zero-sized shim over the shared native engine, and
  DB connections are owned per-OS-thread inside libSz). Default **12 worker
  threads**; scale by processes, never threads (unixODBC driver-manager convoy
  above that — see the dbperf-faq).
* **AMQP layer** (redo% < 100): tokio + lapin, one connection/channel, single
  async consumer, `basic_qos(prefetch)` (default threads + 2), acks/rejects only on the
  async task. The +2 prefetch overshoot keeps a standing buffer so the workers'
  non-blocking dispatch never stalls per-record waiting on an ack round-trip.
* **Redo fetcher** (redo% > 0): ONE thread serially calling `get_redo_record()`
  into a tiny bounded channel (\|B\| + 2). Backpressure alone gates redo fetch
  to actual redo consumption; workers never poll the DB or the broker.
* **Scheduler**: `|B| = clamp(round(N × redo% / 100), 1, N−1)` workers are
  redo-preferring, the rest load-preferring. Each worker `try_recv`s its
  preferred channel, then the other, then backs off briefly — both dequeues are
  **non-blocking** (a blocking recv on the preferred channel would defeat the
  cross-over fallback). No mode state machine: full-drain and snap-back are
  emergent.
* **get_stats** runs on a dedicated thread and is emitted with the mandatory
  `Engine stats:` prefix. A machine-parseable `Combined stats: {...}` JSON line
  (adds/redos rates, MQ depth, measured `redo_share_effective`, derived mode)
  is emitted each stats interval. No redo-backlog field is reported (the
  `count_redo_records()` table scan is never issued).

## Configuration

Precedence: CLI argument > environment variable > default. Env names are
verbatim-compatible with the sibling drivers.

| Env (CLI) | Default | Meaning |
|---|---|---|
| `SENZING_ENGINE_CONFIGURATION_JSON` | required | engine init JSON (validated as JSON at startup) |
| `SENZING_REDO_PERCENT` (`--redo-percent`) | **20** | ∈ [0,100]; see table above. Size UP until SYS_EVAL_QUEUE stays flat during load. |
| `SENZING_THREADS_PER_PROCESS` (`--threads-per-process`) | **12** | worker pool size (0 → CPU count, compat foot-gun) |
| `SENZING_AMQP_URL` (`-u`/`--url`) | required iff redo% < 100 | RabbitMQ URL |
| `SENZING_RABBITMQ_QUEUE` (`-q`/`--queue`) | required iff redo% < 100 | source queue (must exist; passive declare) |
| `SENZING_INPUT_FILE` (`-f`/`--file`) | none | load JSONL (one JSON record per line) from a single file instead of RabbitMQ; mutually exclusive with `--url`/`--queue`. redo% applies as in queue mode. Exits 0 at EOF (redo% = 0) or once the file is loaded and redo is drained (redo% > 0; see *File input*). |
| `SENZING_SKIP_LINES` (`--skip-lines`) | 0 | file mode only: skip the first N physical lines. Resumes an interrupted load — the driver prints a safe `--skip-lines` offset (contiguous-completion watermark) at shutdown. |
| `SENZING_REJECT_FILE` (`--reject-file`) | `<input>.rejected.jsonl` | file mode only: JSONL file that receives every rejected input line **verbatim** (unparseable, engine bad input, retry timeout, SENZ0082). Created lazily on the first reject, opened in append mode. Reprocess with `--file <reject file>`. |
| `SENZING_RECORD_TRANSFORM_PLUGIN` (`--record-transform-plugin`) | none | shared library that rewrites every load record before `add_record` (all backends). See [Record transform plugins](#record-transform-plugins). |
| `SENZING_RECORD_TRANSFORM_CONFIG` (`--record-transform-config`) | empty | opaque string passed to the plugin's init (e.g. JSON) |
| `SENZING_PREFETCH` (`--prefetch`) | RabbitMQ/ActiveMQ: threads + 2; SQS: 2 × threads | queue mode, every binary: the **total** in-flight cap — messages received but not yet settled (RabbitMQ: the `basic_qos` prefetch; SQS: received and not yet deleted; ActiveMQ: AMQP link credit kept at cap − unsettled). A value below threads is raised to threads (with a warning). |
| `SENZING_SQS_QUEUE_URL` (`-q`/`--queue-url`) | required iff redo% < 100 | SQS binary only: source queue URL |
| `SENZING_SQS_DEAD_LETTER_QUEUE_URL` (`--dead-letter-queue-url`) | discovered | SQS binary only: where rejected records are sent. Default: the source queue's `RedrivePolicy` → `deadLetterTargetArn` → `GetQueueUrl`. Printed at startup as `DeadLetter: <url>`. |
| `SENZING_SQS_ALLOW_NO_DLQ` (`--allow-no-dlq`) | off | SQS binary only: start even when no DLQ can be resolved. Rejects are then **deleted** (lost); the log names them with their body. Without it, no DLQ = refuse to start. |
| `SENZING_SQS_VISIBILITY_TIMEOUT` (`--visibility-timeout`) | 2 × LONG_RECORD | SQS binary only: initial visibility on receive, 0..=43200 (the SQS 12 h max; outside = startup error, ≤ LONG_RECORD = warning). Long records are extended automatically (below). |
| `SENZING_SQS_WAIT_TIME` (`--wait-time`) | 20 | SQS binary only: long-poll seconds (0..=20) |
| `SENZING_SQS_MAX_MESSAGES` (`--max-messages`) | 10 | SQS binary only: receive batch size (1..=10); further capped by free in-flight room |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_ENDPOINT_URL`, … | provider chain | SQS binary only: standard AWS SDK resolution (env, `~/.aws`, IMDS/ECS role, web identity). SSO / `credential_process` are not compiled in (see Cargo.toml). |
| `SENZING_ACTIVEMQ_URL` (`-u`/`--url`) | required iff redo% < 100 | ActiveMQ binary only: `amqp://` or `amqps://` broker URL; credentials may be embedded (`amqp://user:pass@host:5672`, percent-encoded) |
| `SENZING_ACTIVEMQ_USER` / `SENZING_ACTIVEMQ_PASSWORD` (`--user` / `--password`) | from URL | ActiveMQ binary only: SASL PLAIN credentials; each overrides the URL's. None at all = anonymous. |
| `SENZING_ACTIVEMQ_QUEUE` (`-q`/`--queue`) | required iff redo% < 100 | ActiveMQ binary only: ANYCAST queue name, or an FQQN `address::queue` |
| `SENZING_MQ_RECHECK_SECONDS` (`--mq-recheck-secs`) | 30 | every binary, queue mode: diagnostic MQ depth probe cadence (RabbitMQ: passive declare; SQS: `ApproximateNumberOfMessages`; ActiveMQ: no AMQP depth verb, depth reported unknown) and the `MQ drained (depth 0)` / `MQ active (depth N)` transition log. Not a correctness poll. |
| `SENZING_REDO_SLEEP_TIME_IN_SECONDS` (`--redo-sleep-secs`) | 60 | fetcher pause on empty redo queue (auto-shortened to 2 s while redo is still in flight, for cascade drain) |
| `LONG_RECORD` (`--long-record`) | 300 | long-record threshold, seconds; stats cadence = LONG_RECORD/2 |
| `SENZING_LOG_LEVEL` | info | log level (`RUST_LOG` overrides) |
| `-i`/`--info` | off | print WithInfo payloads (engine-level no-op; print gating only) |
| `-t`/`--debugTrace` | off | engine debug trace |

Validation is loud (exit 1) and completes before the Senzing engine is
initialized, in all three binaries: redo% ∉ [0,100]; redo% < 100 without URL/queue
(queue mode; SQS: `--queue-url`); 0 < redo% < 100 with fewer than 2 threads
(queue and file mode); `LONG_RECORD` < 1; SQS `--wait-time` ∉ 0..=20,
`--max-messages` ∉ 1..=10, `--visibility-timeout` ∉ 0..=43200; ActiveMQ URL
not `amqp://` / `amqps://` with a host, or a password without a user. Exit codes:
**1** = configuration/validation failure at startup, **255** = fatal runtime
error (engine/DB/broker) after an orderly teardown, **0** = clean shutdown or
file EOF. (The standalone drivers disagreed with each other here; the combined
driver uses this one convention for all three binaries.)

## SQS specifics

* **Dead-letter queue.** SQS has no reject verb, and an explicit `DeleteMessage`
  never triggers the redrive policy. So a rejected record (engine bad input,
  `SENZ0010` retry timeout, `SENZ0082`, unparseable body) is **`SendMessage`d
  to the DLQ verbatim, then deleted** from the source (`sz_sqs_consumer-v4`
  parity). The DLQ copy carries a String message attribute **`SzReason`**: the
  reject reason (engine error text, or `malformed record: <parse error>`),
  sanitized to SQS's allowed characters and capped at 1 KiB (less if the body
  leaves less room under 256 KiB). Read it with
  `ReceiveMessage --message-attribute-names All`. The DLQ is resolved once
  at startup: `--dead-letter-queue-url` if set, else discovered from the source
  queue's `RedrivePolicy`. **No DLQ = refuse to start** unless `--allow-no-dlq`.
  If the DLQ send fails the source message is left alone (visibility expiry
  redelivers it); nothing is deleted that was not preserved first.
* **FIFO queues.** A `.fifo` DLQ gets `MessageGroupId` (the source message's
  group, else `DATA_SOURCE`) and `MessageDeduplicationId` (the source
  `MessageId`, else `DS-ID`).
* **Long records.** Every `LONG_RECORD/2` seconds a record still processing past
  `LONG_RECORD × (n+1)` has its visibility extended to `(n+2) × LONG_RECORD`
  (`ChangeMessageVisibility`, capped at the SQS 12 h maximum) and is logged as
  `Extended visibility (… min, extended n times): DS : ID`. When every worker
  is on such a record (every record past `LONG_RECORD` counts, not only those
  extended on that tick): `All N threads are stuck on long running records`.
  SQS never dead-letters a record for running long (Senzing v4 SQS consumer
  parity); only RabbitMQ gives up at `2 × LONG_RECORD`.
* **Receive errors.** A failed `ReceiveMessage` is retried every second; 30
  consecutive failures (≈ 30 s of an unreachable or denying endpoint) are
  fatal — orderly shutdown, exit 255. Any successful receive resets the count.
* **In-flight cap.** At most `--prefetch` messages (default 2 × threads, v4
  parity) are received and not yet deleted; a slot frees when its
  `DeleteMessageBatch` returns, and the poller refills it immediately.
* **Settle.** Deletes are batched (`DeleteMessageBatch`, up to 10 per call:
  every delete settled while the previous call was in flight; a lone delete
  is sent at once, and the rest flushed at shutdown). A fatal engine error leaves the message
  un-deleted so SQS redelivers it. At shutdown (SIGINT / SIGTERM / SIGHUP,
  bounded by the same 10 s grace as RabbitMQ) messages still in — or queued
  for — a worker are left un-deleted for redelivery after their visibility
  timeout (never dead-lettered; see *Failure handling*).
* **Stats.** Same `Processed N adds, R records per second`, `Engine stats:` and
  `Combined stats:` lines as the RabbitMQ binary; `mq_depth` is the source
  queue's `ApproximateNumberOfMessages`, probed every `--mq-recheck-secs`.
* **IAM.** On the source queue: `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes`. On the DLQ:
  `sqs:SendMessage`, `sqs:GetQueueUrl` (discovery only).

## ActiveMQ Artemis specifics

* **Queue semantics.** The receiver link's source carries the AMQP
  **`queue` capability**, so Artemis binds it to an **ANYCAST** queue
  (point-to-point, competing consumers). `--queue` is a queue name or an FQQN
  (`address::queue`). An address that exists only as MULTICAST refuses the
  attach (startup failure, exit 255). With Artemis' default
  `auto-create-queues`, a missing queue is **created** on attach — a typo
  makes a new empty queue, not an error (disable auto-create to get one).
  Producers should also send with the `queue` target capability (or to a
  pre-created anycast address): a plain AMQP sender auto-creates a MULTICAST
  address the driver cannot attach to.
* **Settle.** Success = `accepted`. Reject (engine bad input, `SENZ0010`
  retry timeout, `SENZ0082`, unparseable body) = **`rejected`**, with the
  reason in its error description (`senzing:rejected-record`). Artemis moves a
  rejected message to the address's **dead-letter address** (the
  `artemis create` default is `DLQ` for `#`, anycast), annotated
  `x-opt-ORIG-QUEUE` / `x-opt-ORIG-ADDRESS`. **An address with no
  dead-letter address drops rejected messages** (like RabbitMQ without a DLX);
  the `REJECTING:` stdout marker still names them. The driver never sends to
  the DLQ itself.
* **Shutdown / release.** At shutdown (SIGINT / SIGTERM / SIGHUP, same 10 s
  grace) link credit drops to 0 and every unsettled delivery is `released`:
  immediately redeliverable and, on Artemis, **not** counted as a delivery
  attempt. Verified against Artemis 2.57: release and an unsettled connection
  close both redeliver with the delivery count unchanged.
* **Long records.** Artemis has no ack timeout and the engine call cannot be
  interrupted, so a long record is only logged (`Still processing …`, and the
  all-threads-stuck line); it is never dead-lettered (SQS parity).
* **In-flight cap.** Manual link credit: credit is re-granted on every settle
  as `--prefetch` − unsettled, so the broker never has more than `--prefetch`
  deliveries outstanding to the driver (default threads + 2).
* **Receive errors.** Per-message errors leave the link usable and are **not**
  fatal: a message the receiver cannot decode (e.g. invalid UTF-8 in an
  `AmqpValue` string) logs `undecodable AMQP message (dead-lettering it): …`
  and is `rejected` like any unparseable body (`REJECTING:  :  -> malformed
  record: …`, counted as rejected); a message over the link's max message size
  (none is set today) is rejected by the AMQP library itself and counted the
  same way. Every other receive error — connection, session or link gone (a
  60 s idle timeout catches a silent broker), or a protocol violation — is
  **fatal**: orderly shutdown, final totals, exit 255. Unsettled deliveries
  are redelivered by the broker.
* **Bodies.** An AMQP `Data` section (bytes; multiple sections are
  concatenated) or an `AmqpValue` string (what a JMS `TextMessage` sends) is
  the record JSON; an `AmqpValue` binary is accepted too. Anything else
  (including an undecodable message) is dead-lettered as malformed.
* **Credentials / TLS.** SASL PLAIN from the URL userinfo or
  `SENZING_ACTIVEMQ_USER` / `SENZING_ACTIVEMQ_PASSWORD`; the password is never
  logged. `amqps://` uses rustls with the webpki root store.
* **Depth.** Artemis exposes no queue depth over AMQP (only via its
  management API), so v1 reports none: the status line has no `mq_depth` and
  the drained/active transition log does not fire.
* **ActiveMQ Classic.** Classic (5.x) also implements AMQP 1.0, but only
  Artemis is tested (e2e against `apache/artemis:2.57.0`).

## Record transform plugins

`--record-transform-plugin <lib.so>` loads a shared library (any language) that
rewrites each load record before `add_record` — e.g. to add derived features.
It is `dlopen`ed once at startup (a load/init failure exits 1, before engine
init) and called concurrently from every worker thread, so the plugin's
transform MUST be thread-safe on one handle. It runs on the worker side, so it
applies identically to RabbitMQ, SQS, ActiveMQ and file mode, and scales with
`SENZING_THREADS_PER_PROCESS`. Redo records are never transformed.

- **Unchanged** → the original body is loaded (no copy).
- **Replaced** → the new body is re-parsed and loaded under ITS
  `DATA_SOURCE`/`RECORD_ID` (a plugin may change either).
- **Error**, or a replaced body that does not parse → the record is rejected
  without requeue (DLQ / reject file receives the ORIGINAL body) and the
  plugin's message is logged.

The C ABI (version 1) is documented in
[`crates/transform-abi/src/lib.rs`](crates/transform-abi/src/lib.rs). Rust
plugins depend on `sz-record-transform`, implement `RecordTransform`, and call
`export_record_transform!(Type, Type::new)`; see
[`crates/transform-example`](crates/transform-example/src/lib.rs).

```bash
sz_rabbit_combined_consumer --file records.jsonl \
    --record-transform-plugin ./libmy_transform.so \
    --record-transform-config '{"ADDED_FIELD":"Y"}'
```

## Failure handling

* **Poison MQ record** (bad JSON / missing DATA_SOURCE/RECORD_ID / non-UTF-8 /
  undecodable AMQP message (ActiveMQ) / engine BadInput / SENZ0082 /
  long-record give-up — the last RabbitMQ only; SQS extends visibility and
  ActiveMQ only logs instead) → dead-letter (RabbitMQ: `basic_reject`, no
  requeue — AMQP 0-9-1 cannot carry the reason; SQS: `SendMessage` to the DLQ
  with `SzReason`, then delete; ActiveMQ: the `rejected` outcome with the
  reason in its error description, routed by Artemis to the dead-letter
  address), keep running. Identical on every transport:
  * every dead-lettered record prints ONE stdout marker
    `REJECTING: DATA_SOURCE : RECORD_ID -> <reason>` (an unparseable body has
    no DS/ID: `REJECTING:  :  -> malformed record: <parse error>`); an engine
    reject's worker also warns `REJECTING due to bad data or timeout [worker N]:
    DS : ID -> <engine error>`;
  * an unparseable body additionally logs one warn
    `DEAD-LETTERING malformed record: <error> [<body, first 2048 chars>…truncated]`;
  * every dead-lettered record (unparseable ones included) counts as
    `rejected` in the final total line (below), never as an add.
* **Poison redo record** (BadInput / retry timeout / SENZ0082) → warn (with the
  engine error text) + drop (no queue to reject to; counted as
  `redos_dropped`). SENZ0082 on a redo record is dropped rather than fatal on
  purpose: a DQM-rejected value can never succeed, and a fatal would wedge the
  redo queue on that one record.
* **Final total line** (every mode, on stdout at exit):
  `Processed total of N adds, M redo records (R rejected, D redo dropped, E errors)`.
  `N` is successful adds ONLY on every transport and file mode (the
  `Processed N adds, … records per second` throughput line uses the same `N`);
  `R` counts dead-lettered / reject-file records. Scrapers should keep reading
  the first number after `Processed total of `.
* **File mode** has no queue: rejects go verbatim to the JSONL reject file
  (`--reject-file`, see above). **SQS** sends them to the dead-letter queue
  (see [SQS specifics](#sqs-specifics)); **ActiveMQ** leaves that to Artemis'
  dead-letter address (see
  [ActiveMQ Artemis specifics](#activemq-artemis-specifics)).
* **Database connection lost / transient DB error** → **fatal** (orderly
  shutdown, exit 255), never dead-lettered: the database is unhealthy, not the
  record. Deliveries stay unacked / un-deleted / unsettled so the broker
  redelivers them once the process is restarted.
* **Fatal errors** (Database, NotInitialized, License, …; SQS also 30
  consecutive `ReceiveMessage` failures; ActiveMQ also a lost broker link) →
  orderly teardown, non-zero exit.
  Graceful shutdown (SIGINT, SIGTERM, or — queue mode — SIGHUP) drains
  in-flight work within a 10 s grace; whatever is still unsettled then is
  **released for redelivery on every transport, never dead-lettered or counted
  as rejected** — queued-but-unstarted deliveries and those still inside a
  worker alike (RabbitMQ: left unacked, requeued when the connection closes;
  SQS: left un-deleted, redelivered after the visibility timeout; ActiveMQ:
  `released`, immediately redeliverable). Each one
  still inside a worker is printed as `Still processing (… min): DS : ID`.
  Redelivery is safe: `add_record` with an existing key replaces the record
  (idempotent) and the engine handles same-key contention itself; the
  in-worker call may still complete before the process exits (and a container
  SIGKILL after its grace aborts it anyway), so dead-lettering would put a
  valid record in the DLQ on every rolling restart. If a worker is still inside an
  uninterruptible engine call after the grace (the same deadline also bounds
  the stats-thread join), the native environment destroy is skipped
  (leak-on-exit over use-after-free).
* A redo record fetched but not yet processed at crash/shutdown is lost from
  the redo queue's perspective (dequeued at fetch) — the tiny redo channel
  bounds this, and the harness's DB-side `SYS_EVAL_QUEUE` check remains the
  completion authority.

### Known limitations

* **Shutdown can outlast the 10 s grace** by the transport close that follows
  it: the SQS close flushes pending deletes (bounded at 5 s; an unflushed
  delete only redelivers that message after its visibility timeout), the
  ActiveMQ link/session/connection close is bounded at 5 s, and the RabbitMQ
  connection close is unbounded. Size the container stop timeout for grace +
  close (+ the 5 s native-teardown bound).
* **A signal can wait on a busy pool when `--prefetch` > 2 × threads.** The
  loop hands each delivery to a worker channel of `threads` slots; with every
  worker busy and that channel full, the hand-off waits for a worker to finish
  before the loop sees SIGINT/SIGTERM/SIGHUP. At the defaults (threads + 2,
  SQS 2 × threads) the in-flight cap never exceeds what workers plus the
  channel hold, so this cannot happen.

## Memory under sustained load

Under long, high-volume loads (esp. datasets with large "giant-component"
regions on high-core hosts), the process **RSS balloons** far beyond the
engine's live footprint — e.g. an individual process reaching 20–70 GB while
`get_stats` reports ~1 GB live. This is **glibc arena high-water retention**:
the compare/scoring path allocates large transient buffers per giant-component
resolution, frees them, but glibc parks the freed memory on its arena free-lists
and never returns it to the OS (no auto-trim; the dynamic mmap threshold ratchets
up so large allocations land in the arena rather than being `mmap`'d). RSS pins
at the high-water mark until the process restarts. It is **not** a leak (live
memory stays bounded) and **not** an arena-*count* problem (`MALLOC_ARENA_MAX=2`
does not bound it — a single process still ballooned to 69 GB).

**The real fix is in the engine** — a per-thread `mmap`-backed arena for the
compare/scoring buffers with `MADV_DONTNEED` on release, tracked in
**[GDEV-4294]** (Senzing G2Dev). Until that ships:

- **Mitigation (validated, default-on):** the reference `Dockerfile` sets
  `MALLOC_MMAP_THRESHOLD_=131072` and `MALLOC_TRIM_THRESHOLD_=131072`. This
  forces large allocations through `mmap` (returned to the OS on free), so RSS
  tracks the live working set instead of pinning. In an A/B on a 330 M-record
  load, a host with these set held free memory steadily / recovered under load,
  while an unmodified host ballooned to OOM and required periodic restarts. It is
  a **stopgap, not a cure**: it applies bluntly to *every* >128 KB allocation
  (some throughput cost) and does not reclaim retention living in ≤128 KB chunks.
  Unset them (or raise the threshold) if that per-allocation `mmap` cost
  outweighs the RSS benefit for your workload.
- **Do NOT `LD_PRELOAD` jemalloc/tcmalloc.** Empirically this **SIGSEGVs libSz**
  at startup (verified with jemalloc 5.3.0, exit 139) — the engine does not
  tolerate an interposed allocator. Swapping the process allocator is not a
  viable deployment-level mitigation.

## Build

```console
# needs libSz at SENZING_LIB_PATH (default /opt/senzing/er/lib)
cargo build --release --workspace                       # everything
cargo build --release -p sz_rabbit_combined_consumer    # RabbitMQ bin only (no AWS SDK)
cargo build --release -p sz_sqs_combined_consumer       # SQS bin only (no lapin)
cargo build --release -p sz_activemq_combined_consumer  # ActiveMQ Artemis bin only (no lapin, no AWS SDK)
cargo test  --workspace --lib --bins                    # unit tests (no infra)

# Docker: BIN selects the backend binary; WITH_POSTGRES/WITH_MSSQL the DB closure.
docker build --build-arg BIN=sz_rabbit_combined_consumer -t brian/sz_rabbit_combined_consumer .        # both DB backends
docker build --build-arg BIN=sz_sqs_combined_consumer    -t brian/sz_sqs_combined_consumer .
docker build --build-arg BIN=sz_activemq_combined_consumer -t brian/sz_activemq_combined_consumer .
docker build --build-arg BIN=sz_rabbit_combined_consumer --build-arg WITH_MSSQL=0 -t brian/sz_rabbit_combined_consumer:pg .
```

## Run

```console
docker run --rm \
  -e SENZING_ENGINE_CONFIGURATION_JSON \
  -e SENZING_AMQP_URL=amqp://user:pw@192.168.6.100:5672 \
  -e SENZING_RABBITMQ_QUEUE=sz_records \
  -e SENZING_REDO_PERCENT=20 \
  -e SENZING_THREADS_PER_PROCESS=12 \
  brian/sz_rabbit_combined_consumer:mssql
```

`SENZING_REDO_PERCENT=0` reproduces the pure consumer, `=100` the pure redoer
(no AMQP settings needed) — useful for A/B-ing the combined scheduler against
the split topology on the same binary. **Benchmark parity:** the driver is part
of the measured system; never compare engine versions across different drivers
— validate at 0%/100% against the siblings first, then re-baseline.

### File input (no RabbitMQ)

Load a JSONL file directly — one JSON record per line — instead of consuming a
queue:

```console
sz_rabbit_combined_consumer --file /data/records.jsonl
```

The point of this driver is to do redo in parallel with load and then switch
to all redo when the load is done, and file input is no exception. File mode
shares redo exactly like queue mode: `--redo-percent` gives \|B\|
workers a redo preference while the file loads (one redo fetcher per process),
and at end-of-file the load channel closes so the whole pool falls through to
redo. At redo% > 0 the process exits 0 once every record has completed AND
`get_redo_record()` comes back empty on **2 consecutive probes
`--redo-sleep-secs` apart** with no redo outstanding in the process — so the
drain tail costs about one `--redo-sleep-secs` (default 60 s) after redo first
reads empty. Queue mode never self-exits on idle; this rule is file-mode only.
At redo% = 0 it is a pure loader and exits at end-of-file. Blank lines are
skipped; unparseable lines go to the reject file (below) without aborting
the load. On completion — or on SIGTERM — it prints a safe resume
offset; restart with `--skip-lines N` to continue where it stopped
(`add_record` is idempotent, so an interrupted run is safe to resume).

**Rejects.** A file has no dead-letter queue, so every rejected line —
unparseable JSON, engine bad input, retry timeout (`SENZ0010`), `SENZ0082` — is
appended verbatim to a JSONL reject file (`--reject-file`, default
`<input>.rejected.jsonl`) and counted, without aborting the load. The file is
created only when something is rejected. Each reject is logged with its
`DATA_SOURCE : RECORD_ID`, the line number and the engine error text, so the
application log says *why* and the reject file holds *what*. Reprocess later by
pointing `--file` at the reject file (give it its own `--reject-file` so the
second pass does not append to its own input):

```console
sz_rabbit_combined_consumer --file /data/records.jsonl.rejected.jsonl \
    --reject-file /data/records.still-rejected.jsonl
```

## Tests and CI

| Suite | Command | Needs |
|---|---|---|
| unit (all crates) | `cargo test --workspace --lib --bins` | libSz only |
| RabbitMQ e2e | `cargo test -p sz_rabbit_combined_consumer --test integration_test -- --nocapture --test-threads=1` | RabbitMQ + engine + `truth-sets` submodule |
| SQS e2e | `cargo test -p sz_sqs_combined_consumer --test sqs_e2e -- --nocapture --test-threads=1` | ElasticMQ + engine |
| ActiveMQ e2e | `cargo test -p sz_activemq_combined_consumer --test activemq_e2e -- --nocapture --test-threads=1` | Artemis (AMQP + Jolokia) + engine |

`.github/workflows/ci.yml` runs fmt + clippy, the release build + unit tests,
the three e2e suites in one `integration` job (Postgres, RabbitMQ, ElasticMQ
and Artemis service containers) and the Docker matrix (every `BIN`; the
postgres / mssql DB-closure variants on the RabbitMQ and SQS images).
`--test-threads=1` is required: the engine is a process-global singleton.
An e2e test whose infrastructure is missing prints `SKIP` and passes, unless
`IT_REQUIRE_INFRA=1` (set in CI), which turns every skip into a failure.

### Running the e2e tests locally

```console
# Brokers + Postgres (image tags match ci.yml). Artemis AMQP is mapped to
# host 5673 because RabbitMQ owns 5672.
docker run -d --name sz-e2e-postgres -p 55432:5432 \
  -e POSTGRES_USER=senzing -e POSTGRES_PASSWORD=senzing -e POSTGRES_DB=G2 postgres:16
docker run -d --name sz-e2e-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name sz-e2e-elasticmq -p 9324:9324 softwaremill/elasticmq-native:1.6.11
docker run -d --name sz-e2e-artemis -p 5673:5672 -p 8161:8161 \
  -e ARTEMIS_USER=artemis -e ARTEMIS_PASSWORD=artemis -e ANONYMOUS_LOGIN=false \
  -e EXTRA_ARGS="--http-host 0.0.0.0 --relax-jolokia --no-autotune" \
  apache/artemis:2.57.0

# macOS (Homebrew Senzing); on Linux use /opt/senzing/er and LD_LIBRARY_PATH.
export SENZING_ROOT="$(brew --prefix)/opt/senzing/er"
export DYLD_LIBRARY_PATH="$SENZING_ROOT/lib"
export SENZING_LIB_PATH="$SENZING_ROOT/lib"
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"'"$SENZING_ROOT"'/etc","RESOURCEPATH":"'"$SENZING_ROOT"'/resources","SUPPORTPATH":"'"$(brew --prefix)"'/opt/senzing/data"},"SQL":{"CONNECTION":"postgresql://senzing:senzing@localhost:55432:G2"}}'
export IT_PG_DSN=postgresql://senzing:senzing@localhost:55432/G2
export SENZING_AMQP_URL='amqp://guest:guest@localhost:5672/%2F' SENZING_RABBITMQ_QUEUE=senzing-rabbitmq-queue
export AWS_ENDPOINT_URL=http://localhost:9324 AWS_ACCESS_KEY_ID=elasticmq AWS_SECRET_ACCESS_KEY=elasticmq AWS_REGION=elasticmq
export SENZING_ACTIVEMQ_URL=amqp://artemis:artemis@localhost:5673
export IT_ARTEMIS_JOLOKIA_URL=http://artemis:artemis@localhost:8161/console/jolokia
export IT_REQUIRE_INFRA=1
```

Initialize a **fresh** Senzing repository before each full run (the truth-set
and file-mode redo tests need an empty one): apply
`$SENZING_ROOT/resources/schema/szcore-schema-postgresql-create.sql` with
`psql "$IT_PG_DSN"`, install the default config (`sz_setup_config --auto`)
and register the data sources `CUSTOMERS REFERENCE WATCHLIST TEST SEARCH`
(`sz_configtool -f -C "addDataSource TEST" … -C save`) — the same steps as
the `integration` job's init step. Then run the three e2e commands above.
Queues are created by the tests (unique names per test); Artemis's default
`DLQ` address receives the ActiveMQ dead-lettered records.

## License

Apache-2.0
