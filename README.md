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
| `sz_activemq_publisher` | bin | Artemis **publisher**: JSONL files -> the queue the ActiveMQ consumer reads (no Senzing) | `fe2o3-amqp` (rustls), `flate2`, `bzip2` |
| `sz-activemq-amqp` | lib | — (Artemis URL/credentials, connection, anycast terminus, record body encode/decode; shared by the ActiveMQ consumer + publisher) | `fe2o3-amqp` |
| `sz-record-transform` | lib | — (record-transform plugin C ABI + Rust export macro) | none |
| `sz-record-transform-example` | cdylib | — (example plugin; used by tests) | none |
| `sz-info-sink` | lib | — (INFO sink plugin C ABI + Rust export macro) | none |
| `sz-info-sink-file` | cdylib | — (INFO sink plugin writing sealed JSONL files) | none |

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
| `SENZING_REDO_DRAIN_MAX_SECS` (`--redo-drain-max-secs`) | `0` (off) | file mode, redo% > 0: maximum seconds to keep draining redo after the input file is read to its end; `0` = no cap (default: the cap is opt-in; set it together with the caller's job/task timeout). On expiry one final redo probe is made; if redo is drained the run ends normally (exit 0), otherwise redo fetching stops, the remaining redo count is logged and the process exits **3**. A cap below 2 × `--redo-sleep-secs` logs a startup warning. Not applicable in queue mode or the pure redoer (no end of input; accepted and ignored). See *File input*. |
| `SENZING_INFO_SINK` (`--info-sink`) | none | shared library implementing the INFO sink ABI; forces WITH_INFO on add and redo and receives every payload (all modes). See [INFO sink plugins](#info-sink-plugins). |
| `SENZING_INFO_SINK_CONFIG` (`--info-sink-config`) | empty | config string passed to the sink's `sz_is_create` (the file sink takes JSON) |
| `SENZING_INFO_SINK_FLUSH_SECS` (`--info-sink-flush-secs`) | 30 | seconds between flushes (seal the current output) issued by a timer (file mode: its own thread, so it also runs during the load); ≥ 1 |
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
| `SENZING_REDO_STATS_INTERVAL` (`--redo-stats-interval`) | 1000 | log a redo throughput line (`Stats: N redo records processed, R/sec`) every this many processed redo records; ≥ 1 |
| `LONG_RECORD` (`--long-record`) | 300 | long-record threshold, seconds; stats cadence = LONG_RECORD/2 |
| `SENZING_LOG_LEVEL` | info | log level (`RUST_LOG` overrides) |
| `-i`/`--info` | off | print WithInfo payloads to stdout (passes WITH_INFO). Independent of `--info-sink`; both may be set. |
| `-t`/`--debugTrace` | off | engine debug trace |

Validation is loud (exit 1) and completes before the Senzing engine is
initialized, in all three binaries: redo% ∉ [0,100]; redo% < 100 without URL/queue
(queue mode; SQS: `--queue-url`); 0 < redo% < 100 with fewer than 2 threads
(queue and file mode); `LONG_RECORD` < 1; SQS `--wait-time` ∉ 0..=20,
`--max-messages` ∉ 1..=10, `--visibility-timeout` ∉ 0..=43200; ActiveMQ URL
not `amqp://` / `amqps://` with a host, or a password without a user. Exit codes:
**1** = configuration/validation failure at startup, **255** = fatal runtime
error (engine/DB/broker/INFO sink, including a sink close stuck past 5 s)
after an orderly teardown, **3** = file mode stopped at the redo drain cap
(`--redo-drain-max-secs`) with redo still queued (nothing failed; the backlog
was left behind), **0** = clean shutdown or file EOF. A fatal error outranks
the cap (255). A SIGTERM received BEFORE the cap expires is a clean stop (0),
never a cap expiry; once the cap HAS expired (the fetcher stopped and the run
is finishing already-fetched records) the exit is decided and a SIGTERM still
exits 3. (The standalone drivers disagreed with each other here; the combined
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

### Loading the queue: `sz_activemq_publisher`

Bulk publisher for the queue above (it links no Senzing library, so it runs on
any host that can read the input and reach the broker). It shares the
consumer's URL / credential / queue handling (`SENZING_ACTIVEMQ_URL`,
`_USER`, `_PASSWORD`, `_QUEUE`, same override rules) and its wire encoding:
each record is one AMQP `Data` section holding the line's bytes verbatim, sent
with the `queue` target capability (ANYCAST), header `durable=true` by default.

```console
# A Spark output directory (its part-* files) plus a single file, 4 files at a time
sz_activemq_publisher -u amqp://artemis:artemis@broker:5672 -q senzing-input \
    -p 4 /public_data/spark_out/ /data/extra.jsonl.bz2
```

| Flag | Default | Meaning |
|---|---|---|
| `INPUT...` | required | files (plain / gzip / bzip2, detected from content) or directories (their `part-*` files, sorted; `.crc` / `_SUCCESS` ignored). Blank lines are skipped. |
| `-u, --url` | `SENZING_ACTIVEMQ_URL` | `amqp://` only (the broker's max-frame-size is read off the plaintext Open frame), credentials may be embedded |
| `--user`, `--password` | `SENZING_ACTIVEMQ_USER` / `_PASSWORD` | SASL PLAIN; override URL userinfo |
| `-q, --queue` | `SENZING_ACTIVEMQ_QUEUE` | queue or FQQN `address::queue` |
| `-m, --max-pending` | 500 | records sent but not yet settled, per file |
| `-p, --parallel` | 1 | files published concurrently (one connection each) |
| `--report-interval` | 100000 | progress line every N settled records (0 = off) |
| `--retry-delay` | 3 | seconds before retrying a non-accepted record / failed connect |
| `--max-attempts` | 10 | `rejected`/`released` outcomes per record before it is FAILED (0 = unlimited) |
| `--reconnect-timeout` | 300 | give up (exit 1) after the broker is unreachable this long (0 = forever) |
| `--blocked-log-interval` | 30 | seconds between `blocked on broker credit` lines |
| `--transient` | off | `durable=false` (lost on broker restart) |
| `--min-max-frame-size` | 0 | refuse to start (exit 1) if the broker advertises a smaller AMQP max-frame-size (0 = no floor) |
| `--max-frame-size` | fe2o3 default | AMQP max-frame-size this client advertises (the negotiated frame is the smaller of the two) |
| `--side-max-bytes` | 209715200 (`SIDE_MAX_BYTES`) | records larger than this are not published (see `--side-file`) |
| `--side-file` | none (`SIDE_FILE`) | append oversized records here, logged `sided <file> line <n> bytes <b>`; without it they are FAILED |
| `--resume` | none (`RESUME`) | `<file name> <records already published>` per line (this publisher's `fileprogress` lines): those leading records are skipped |
| `--progress-every` | 100000 | `fileprogress <file> <n>` every N records per file: the first n non-blank records are settled, FAILED or sided |

* **Multi-frame transfers (no broker setting needed).** A record larger than the
  negotiated max-frame-size (Artemis default 131072) goes out as several transfer
  frames. Upstream fe2o3-amqp (0.18.2/0.18.3) numbered deliveries with the
  per-frame transfer-id, so a record split over k frames skipped k-1
  delivery-ids and Artemis closed the connection (`Expected delivery-id N, got
  N+k-1`), its AMQP thread then spinning in proton-j. The workspace depends on
  the fork `brianmacy/fe2o3-amqp` (branch `fix/per-delivery-delivery-id`,
  `[patch.crates-io]` in `Cargo.toml`) which allocates delivery-ids per
  delivery; the e2e suite proves 150 MB records through a 64 KiB frame.
* **Oversized records and resume.** Records over `--side-max-bytes` are diverted
  to `--side-file` (loaded later in the consumer's file mode) or FAILED without
  one. `fileprogress <file> <n>` lines are a safe resume point (the oldest
  pending record bounds n); feed them back with `--resume`.
* **Memory-bounded.** Files are streamed (never loaded whole); per file at most
  ~3 x `--max-pending` records are in memory (read-ahead channel, in flight,
  retries), times `--parallel`.
* **Settlement.** A record counts as `settled` only when Artemis returns
  `accepted`. `rejected` / `released` / `modified` are retried after
  `--retry-delay`; after `--max-attempts` the record is **FAILED**: logged as
  `FAILED record <file> line <n> (...)`, counted, and the run exits **2**.
  A message that can never be sent (over the link's max message size) fails
  the same way immediately.
* **Full address (BLOCK).** Under `address-full-policy=BLOCK` Artemis
  withholds link credit; the publisher **waits indefinitely** (no send or
  settlement timeout, so no reconnect and no republish/duplicates) and logs
  `blocked on broker credit for Ns` every `--blocked-log-interval`. A dead
  broker is still detected by the 60 s connection idle timeout (Artemis
  heartbeats while merely blocking). Artemis grants producer credit in batches
  (`amqpCredits`, default 1000), so up to that many messages can land past
  `max-size-bytes` before the block bites. **Also BLOCK:** Artemis's
  `max-disk-usage` (default 90% of the journal's filesystem) blocks every
  address with `Disk Full!` in the broker log — the publisher then shows the
  same `blocked on broker credit` line.
* **Link loss.** A send or settlement error reconnects (retrying for up to
  `--reconnect-timeout`) and re-sends every unsettled record:
  **at-least-once**, so a record the broker accepted just before the loss can
  be queued twice (counted as `resent`; `add_record` is idempotent per
  `DATA_SOURCE` + `RECORD_ID`). Bad credentials / a bad URL fail immediately.
* **Exit codes.** 0 every record settled; 1 fatal (arguments, unreadable or
  corrupt input, broker unreachable, bad credentials — remaining files are
  aborted); 2 finished with FAILED records. Each file prints
  `file <path>: read=… settled=… rejected=… released=… resent=… reconnects=… failed=…`
  and the run ends with `Publish summary: files=…/… …`.

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

## INFO sink plugins

`--info-sink <lib.so>` loads a shared library that receives every WITH_INFO
payload (`{"DATA_SOURCE":…,"RECORD_ID":…,"AFFECTED_ENTITIES":[{"ENTITY_ID":…}]}`,
kept raw — a redo INFO may legitimately carry an empty key and no affected
entities). It works exactly like the transform plugin: `dlopen`ed once at
startup with an ABI version check (load/create failure exits 1, before engine
init), shared by every worker, never `dlclose`d. With a sink configured:

- `add_record` and `process_redo_record` run WITH_INFO; each worker writes the
  payload right after the engine call (kind 0 ADD, 2 REDO; kind 1 DELETE is
  reserved for a delete program and never written by this consumer). A
  blocking write is backpressure.
- A timer calls `sz_is_flush` every `--info-sink-flush-secs` (file mode: on
  its own thread from before the first read, so files are sealed during the
  load too; queue mode: the event loop; pure redoer: the monitor loop).
- `sz_is_close` (final seal) is called EXPLICITLY once the workers have joined
  — or the shutdown grace ran out — and before the engine is torn down, on the
  SIGTERM path too. Nothing relies on `Drop`: the forced-exit paths skip
  destructors. The close runs on its own thread and is bounded by **5 s**: a
  close that does not return is fatal (exit 255, the stuck plugin's path is
  logged) and the process takes the leak-on-exit path (no native teardown).
- **Durability contract.** `sz_is_write` returning 0 must mean the line has
  left the plugin's process memory (handed to the OS): the consumer acks the
  delivery / advances the resume watermark right after it, so an in-process
  buffer would lose INFO of acknowledged records on SIGKILL / OOM. A seal
  (`sz_is_flush` / `sz_is_close`) makes the output complete.
- **Every sink error (create, write, flush, close) is fatal** — never a reject:
  orderly shutdown, exit 255 (create: exit 1). A record whose INFO could not
  be written is never reported as loaded: file mode does not advance the
  `--skip-lines` watermark past it, queue mode leaves the delivery unsettled
  for redelivery (re-adding is idempotent and re-emits the INFO). A redo record
  is dequeued at fetch, so its INFO is lost if that one write fails; it is
  then not counted (or printed) as processed.
- **Resume point with a sink (file mode).** The printed `--skip-lines N` is the
  completion watermark captured just before the last SUCCESSFUL seal, so
  every line up to N has its INFO in a sealed file. After a write, flush or
  close failure the run exits 255 and N stays at that last seal (never past
  it). Queue mode has no resume point: deliveries acked before a later seal
  failure stay acked (their lines were handed to the OS; the run exits 255).
- **Reader contract.** Read the INFO output only after the consuming job has
  exited successfully (0, or 3 for the drain cap). The files of a failed run
  are discarded by the caller (or, in file mode, kept only when the caller
  resumes from the printed `--skip-lines`, whose lines are all in sealed
  files). To pick files up while the job runs, use the file sink's
  `seal_marker`.
- The ABI version is checked first, before any other symbol is resolved, so a
  plugin built for another version fails with a version-mismatch error.
- `--info` printing is unchanged and independent.

C ABI (version 1, `crates/info-sink-abi/src/lib.rs`; Rust plugins implement
`InfoSink` and call `export_info_sink!(Type, Type::new)`):

```c
uint32_t sz_is_abi_version(void);
void*    sz_is_create(const char* cfg_utf8, char** err_out);     // NULL = failure
int32_t  sz_is_write(void* h, uint32_t kind /*0 ADD,1 DELETE,2 REDO*/,
                     const char* info, size_t len, char** err_out); // thread-safe, may block; 0 ok / -1 fatal
int32_t  sz_is_flush(void* h, char** err_out);  // seal current file; next write opens a NEW one
int32_t  sz_is_close(void* h, char** err_out);  // final seal, called once
void     sz_is_free(char* p);                   // frees plugin-owned strings
void     sz_is_destroy(void* h);                // no dlclose
```

### File sink (`sz-info-sink-file`)

Writes each payload as one JSONL line into NEW files only (`create_new`;
never append, never rename, no temp files — stage volumes reject those),
named `<prefix><seq06>.jsonl` (from `000001`). Each line is written with ONE
unbuffered `write(2)` (no in-process buffer; see the durability contract). A
file is sealed — `fsync` (`EINVAL`/`ENOTSUP`/`EOPNOTSUPP`/`ENODEV`/unsupported
tolerated, for volumes without it) then an explicit, error-checked `close(2)` — after
`rotate_records` lines, when a write finds it older than `rotate_secs`, on
every consumer flush and on close; no empty files are created. Any write,
create, fsync or close error marks the sink FAILED: the open file is dropped
unsealed (it may end in a partial line) and every later call fails. At create
the directory is scanned and numbering continues after the highest existing
`<prefix>NNNNNN.jsonl`, so a restart never collides. A payload containing a
line break is rejected (fatal) rather than altered.

A file exists under its FINAL name from its first line (no rename at seal), so
a listing taken mid-run can show a file still being written; see the reader
contract above. With `"seal_marker": true` an empty `<file>.sealed` is created
after each successful seal (only marked files are complete; a crash can leave
a sealed file without its marker, never the reverse). File names are
`<seq06>`: after file 999999 the number grows to 7 digits
(`…1000000.jsonl`), where lexical order no longer matches sequence order —
readers must sort by the parsed number, not by name.

Cost (measured on a local APFS volume): an unbuffered line write is ~1.9 µs
(~530k lines/s through the plugin's mutex) vs ~0.14 µs buffered; with the
default rotation the consumer's records/s with the sink is unchanged within
run-to-run noise. Each seal costs one `fsync` (macOS: `F_FULLFSYNC`, several
ms), so a very small `rotate_records` is slower (rotating every 4 records:
9–30 % fewer records/s in the same test).

| Key | Default | Meaning |
|---|---|---|
| `dir` | required | output directory; must exist |
| `prefix` | required | file-name prefix, no `/` |
| `rotate_records` | 10000 | lines per file, ≥ 1 |
| `rotate_secs` | 30 | maximum file age, ≥ 1 (checked at write; the consumer's flush seals idle files) |
| `seal_marker` | `false` | create an empty `<file>.sealed` after each successful seal |

```bash
cargo build --release -p sz-info-sink-file   # target/release/libsz_info_sink_file.so
sz_rabbit_combined_consumer --file /work/in/p0/records.json \
    --info-sink ./libsz_info_sink_file.so \
    --info-sink-config '{"dir":"/work/info","prefix":"info-c0-","rotate_records":10000,"rotate_secs":30}'
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
  Graceful shutdown (SIGINT, SIGTERM, or SIGHUP) drains
  in-flight work within a 10 s grace; whatever is still unsettled then is
  **released for redelivery on every transport, never dead-lettered or counted
  as rejected** — queued-but-unstarted deliveries and those still inside a
  worker alike (RabbitMQ: left unacked, requeued when the connection closes;
  SQS: left un-deleted, redelivered after the visibility timeout; ActiveMQ:
  `released`, immediately redeliverable). Each one
  still inside a worker is printed as `Still processing (… min): DS : ID`.
  **`nohup` is honored:** if SIGHUP is already ignored when the process starts
  (as `nohup` sets it), it stays ignored in every mode (queue, file, pure
  redoer) and a startup line says so, so `nohup … &` survives logout; stop it
  with SIGTERM/SIGINT. SIGINT/SIGTERM behavior is unchanged.
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
  close + the 5 s native-teardown bound + the 2 s exit watchdog: use **30 s**
  (`docker run --stop-timeout 30`, compose `stop_grace_period: 30s`). Docker's
  default 10 s SIGKILLs exactly when the worker-join grace ends.
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

## Download / release binaries

Each [GitHub Release](https://github.com/brianmacy/sz_queue_combined_consumer/releases) carries prebuilt Linux binaries, built by
[`.github/workflows/release.yml`](.github/workflows/release.yml) inside the
`senzing/senzingsdk-runtime:4.3.3` image for that arch:

| Asset | Contents |
|---|---|
| `sz_queue_combined_consumer-<tag>-linux-x86_64.tar.gz` | `sz_rabbit_combined_consumer`, `sz_sqs_combined_consumer`, `sz_activemq_combined_consumer`, `sz_activemq_publisher`, `README.md`, `LICENSE`, `CHANGELOG.md` |
| `sz_queue_combined_consumer-<tag>-linux-aarch64.tar.gz` | same, for arm64 |
| `SHA256SUMS` | SHA-256 of both archives |

```console
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf sz_queue_combined_consumer-v0.4.1-linux-x86_64.tar.gz
LD_LIBRARY_PATH=/opt/senzing/er/lib \
  ./sz_queue_combined_consumer-v0.4.1-linux-x86_64/sz_rabbit_combined_consumer --version
```

**Runtime requirements.** The binaries are not fully static, and cannot be:
the Senzing engine (`libSz.so`) is a glibc shared library, linked as a dynamic
dependency (no static or musl build exists). Everything else is compiled in:
TLS is rustls (no OpenSSL), and the release build fails if a binary needs any
library other than `libSz.so`, `libc`, `libm`, `libgcc_s` and the glibc loader.
You need:

- The **Senzing SDK runtime 4.x** installed (`/opt/senzing/er`), with
  `LD_LIBRARY_PATH=/opt/senzing/er/lib` (the binaries carry no RPATH). libSz's
  own dependencies (libstdc++, OpenSSL 3, zlib, zstd) and the DB-plugin
  dependencies for your database (libpq or the MS ODBC driver) are needed as
  for any Senzing application; the Senzing packages pull them in.
- **glibc >= the build image's** (Debian 13, glibc 2.41) — i.e. the same distro
  baseline as the Senzing 4.3.x runtime packages. Running inside
  `senzing/senzingsdk-runtime` always works.

The INFO file sink (`crates/info-sink-file`) is not in the archive yet;
build it with `cargo build --release -p sz-info-sink-file`. The
record-transform example plugin (`crates/transform-example`) is not
shipped: it is a reference implementation for writing your own plugin, not a
useful transform, and a plugin must be built against the same
`sz-record-transform` ABI version as the driver anyway — build it with
`cargo build --release -p sz-record-transform-example`.

## Build

```console
# needs libSz at SENZING_LIB_PATH (default /opt/senzing/er/lib)
cargo build --release --workspace                       # everything
cargo build --release -p sz_rabbit_combined_consumer    # RabbitMQ bin only (no AWS SDK)
cargo build --release -p sz_sqs_combined_consumer       # SQS bin only (no lapin)
cargo build --release -p sz_activemq_combined_consumer  # ActiveMQ Artemis bin only (no lapin, no AWS SDK)
cargo build --release -p sz_activemq_publisher          # Artemis publisher (no Senzing, no libSz needed)
cargo test  --workspace --lib --bins                    # unit tests (no infra)

# Docker: BIN selects the backend binary; WITH_POSTGRES/WITH_MSSQL the DB closure.
docker build --build-arg BIN=sz_rabbit_combined_consumer -t brian/sz_rabbit_combined_consumer .        # both DB backends
docker build --build-arg BIN=sz_sqs_combined_consumer    -t brian/sz_sqs_combined_consumer .
docker build --build-arg BIN=sz_activemq_combined_consumer -t brian/sz_activemq_combined_consumer .
docker build --build-arg BIN=sz_rabbit_combined_consumer --build-arg WITH_MSSQL=0 -t brian/sz_rabbit_combined_consumer:pg .
# The image is a Senzing-runtime image for ONE driver bin; sz_activemq_publisher
# is not imaged (it needs no Senzing) — ship the release binary instead.
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

**Redo drain cap.** The drain tail is bounded by `--redo-drain-max-secs`
(opt-in: default `0` = no cap; set it together with the caller's own job or
task timeout so the cap fires first). Its clock starts when the reader reaches the
end of the file (a slow load never eats into it). Whichever comes first wins:
the two-empty-probe rule above (exit 0) or the cap. The cap can expire while
the rule is still waiting for its second empty probe (a cap below
2 × `--redo-sleep-secs` always can, and logs a startup warning), so at expiry
the fetcher makes ONE final probe when every record has completed and no redo
it fetched is outstanding: empty means redo is drained and the run exits 0.
Otherwise (adds or redo still running, or the probe returned a record, which
is still processed) the fetcher stops fetching (a record it already fetched is
still processed), in-flight work finishes, the INFO sink is closed, `REDO
DRAIN CAP EXPIRED: … stopped fetching redo with N redo record(s) remaining in
the queue` is logged (counted once after the pool stopped) and the process
exits **3**. A SIGTERM before the cap expires is a normal graceful stop (exit
0); a SIGTERM after it expired still exits 3; a fatal error exits 255.

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
| ActiveMQ publisher e2e | `cargo test -p sz_activemq_publisher --test publisher_e2e -- --nocapture --test-threads=1` | Artemis (AMQP + Jolokia); no engine |
| ActiveMQ e2e | `cargo test -p sz_activemq_combined_consumer --test activemq_e2e -- --nocapture --test-threads=1` | Artemis (AMQP + Jolokia) + engine; run after the publisher suite (its round-trip test runs `target/debug/sz_activemq_publisher`) |
| INFO sink + drain cap e2e | `cargo test -p sz_rabbit_combined_consumer --test info_sink_e2e -- --nocapture --test-threads=1` | engine only (creates its own throwaway SQLite repository from `PIPELINE` in `SENZING_ENGINE_CONFIGURATION_JSON`); the queue-mode cases also need `SENZING_AMQP_URL` |

`.github/workflows/ci.yml` runs fmt + clippy, the release build + unit tests,
the e2e suites in one `integration` job (Postgres, RabbitMQ, ElasticMQ
and Artemis service containers) and the Docker matrix (every `BIN`; the
postgres / mssql DB-closure variants on the RabbitMQ and SQS images).
`--test-threads=1` is required: the engine is a process-global singleton.
An e2e test whose infrastructure is missing prints `SKIP` and passes, unless
`IT_REQUIRE_INFRA=1` (set in CI), which turns every skip into a failure.

The `info_sink_e2e` failure cases use only real components and real faults,
and all of them run as root (CI containers): a sink that fails or wedges is
the test-fixture plugin `sz-info-sink-fault-fixture` (a real cdylib behind
the real loader; its config string names the entry points that fail, block
forever or sleep); a database failure is a table dropped from a PRIVATE
throwaway SQLite copy of the repository while the driver runs; an engine that
cannot initialize is a repository path that cannot be opened; a sink
directory that disappears mid-load is replaced by a regular file (ENOTDIR).

Coverage: `cargo llvm-cov` (with Homebrew rustc, point `LLVM_COV` and
`LLVM_PROFDATA` at a matching LLVM's `bin/`). Coverage builds (`--cfg
coverage`) write the profile before a forced `_exit`, so leak-on-exit runs
are measured too. Two kinds of lines stay uncovered: an OS thread-spawn
failure (`std::thread::Builder::spawn` returning `Err`), which no real limit
reproduces reliably (macOS has no per-process thread rlimit; Linux
`RLIMIT_NPROC` counts every task of the user and does not apply to root),
and the `export_info_sink!` entry points when several plugins are measured in
one report: their `#[no_mangle]` symbols share names across binaries, so
their profile records collide (`cargo llvm-cov -p sz-info-sink` alone shows
them covered).

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
# On a host whose disk is >90% full, Artemis blocks ALL producers ("Disk
# Full!"); for a throwaway test broker add
#   -e JAVA_ARGS_APPEND="-Dbrokerconfig.maxDiskUsage=-1"

# macOS (Homebrew Senzing); on Linux use /opt/senzing/er and LD_LIBRARY_PATH.
export SENZING_ROOT="$(brew --prefix)/opt/senzing/er"
export DYLD_LIBRARY_PATH="$SENZING_ROOT/lib"
export SENZING_LIB_PATH="$SENZING_ROOT/lib"
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"'"$SENZING_ROOT"'/etc","RESOURCEPATH":"'"$SENZING_ROOT"'/resources","SUPPORTPATH":"'"$(brew --prefix)"'/opt/senzing/data"},"SQL":{"CONNECTION":"postgresql://senzing:senzing@127.0.0.1:55432:G2"}}'
export IT_PG_DSN=postgresql://senzing:senzing@127.0.0.1:55432/G2
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
