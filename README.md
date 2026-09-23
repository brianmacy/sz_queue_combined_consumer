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

`cargo build -p sz_rabbit_combined_consumer` never compiles the AWS SDK, and
`cargo build -p sz_sqs_combined_consumer` never compiles `lapin`. Both binaries
also support the shared **file-input** mode (`--file`, below) and the pure
redoer (`--redo-percent 100`). The SQS binary takes `--queue-url` /
`SENZING_SQS_QUEUE_URL` (plus `--visibility-timeout`, `--wait-time`,
`--max-messages`, `--prefetch`, `--dead-letter-queue-url`); credentials/region
come from the standard AWS provider chain. See [SQS specifics](#sqs-specifics).

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
| 100 | Pure redoer. AMQP never opened; tokio runtime never built; `SENZING_AMQP_URL`/queue may be unset. | `sz_simple_redoer` |
| (0,100) | While the MQ is busy, redo gets exactly \|B\|/N of the pool (a hard share — size it for load-phase keep-up). When the MQ drains, ALL workers fall into redo automatically; a new publish flips them back instantly (push-based, no polling). | both |

## Concurrency model

* **One `Sz_init` per process**; every thread derives its own engine handle
  (`env.get_engine()` is a zero-sized shim over the shared native engine, and
  DB connections are owned per-OS-thread inside libSz). Default **12 worker
  threads**; scale by processes, never threads (unixODBC driver-manager convoy
  above that — see the dbperf-faq).
* **AMQP layer** (redo% < 100): tokio + lapin, one connection/channel, single
  async consumer, `basic_qos(prefetch = threads + 2)`, acks/rejects only on the
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
  (adds/redos rates, MQ depth, `count_redo_records` backlog + EWMA slope,
  measured `redo_share_effective`, derived mode) is emitted each stats interval.
* **Redo-floor guard**: if redo keeps progressing while the backlog stays flat
  at a small value and the MQ is empty (vacuously true at redo% = 100) for 5
  consecutive intervals, it warns `redo-floor suspected (possible __REPAIR__
  loop)` and starts sampling raw redo records so the trigger reason is visible.
  Stopping remains the operator's call.

## Configuration

Precedence: CLI argument > environment variable > default. Env names are
verbatim-compatible with the sibling drivers.

| Env (CLI) | Default | Meaning |
|---|---|---|
| `SENZING_ENGINE_CONFIGURATION_JSON` | required | engine init JSON (validated as JSON at startup) |
| `SENZING_REDO_PERCENT` (`--redo-percent`) | **20** | ∈ [0,100]; see table above. Size UP until SYS_EVAL_QUEUE stays flat during load (`redo_backlog_slope` ≤ 0). |
| `SENZING_THREADS_PER_PROCESS` (`--threads-per-process`) | **12** | worker pool size (0 → CPU count, compat foot-gun) |
| `SENZING_AMQP_URL` (`-u`/`--url`) | required iff redo% < 100 | RabbitMQ URL |
| `SENZING_RABBITMQ_QUEUE` (`-q`/`--queue`) | required iff redo% < 100 | source queue (must exist; passive declare) |
| `SENZING_INPUT_FILE` (`-f`/`--file`) | none | load JSONL (one JSON record per line) from a single file instead of RabbitMQ. Pure loader (redo% ignored); mutually exclusive with `--url`/`--queue`. Exits 0 at EOF. |
| `SENZING_SKIP_LINES` (`--skip-lines`) | 0 | file mode only: skip the first N physical lines. Resumes an interrupted load — the driver prints a safe `--skip-lines` offset (contiguous-completion watermark) at shutdown. |
| `SENZING_REJECT_FILE` (`--reject-file`) | `<input>.rejected.jsonl` | file mode only: JSONL file that receives every rejected input line **verbatim** (unparseable, engine bad input, retry timeout, SENZ0082). Created lazily on the first reject, opened in append mode. Reprocess with `--file <reject file>`. |
| `SENZING_PREFETCH` (`--prefetch`) | RabbitMQ: threads + 2; SQS: threads | RabbitMQ: `basic_qos` prefetch. SQS: extra messages held beyond the worker count (in-flight cap = threads + prefetch) |
| `SENZING_SQS_QUEUE_URL` (`-q`/`--queue-url`) | required iff redo% < 100 | SQS binary only: source queue URL |
| `SENZING_SQS_DEAD_LETTER_QUEUE_URL` (`--dead-letter-queue-url`) | discovered | SQS binary only: where rejected records are sent. Default: the source queue's `RedrivePolicy` → `deadLetterTargetArn` → `GetQueueUrl`. Printed at startup as `DeadLetter: <url>`. |
| `SENZING_SQS_ALLOW_NO_DLQ` (`--allow-no-dlq`) | off | SQS binary only: start even when no DLQ can be resolved. Rejects are then **deleted** (lost); the log names them with their body. Without it, no DLQ = refuse to start. |
| `SENZING_SQS_VISIBILITY_TIMEOUT` (`--visibility-timeout`) | 2 × LONG_RECORD | SQS binary only: initial visibility on receive. Long records are extended automatically (below). |
| `SENZING_SQS_WAIT_TIME` (`--wait-time`) | 20 | SQS binary only: long-poll seconds (0..=20) |
| `SENZING_SQS_MAX_MESSAGES` (`--max-messages`) | 10 | SQS binary only: receive batch size (1..=10); further capped by free in-flight room |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_ENDPOINT_URL`, … | provider chain | SQS binary only: standard AWS SDK resolution (env, `~/.aws`, IMDS/ECS role, web identity). SSO / `credential_process` are not compiled in (see Cargo.toml). |
| `SENZING_MQ_RECHECK_SECONDS` (`--mq-recheck-secs`) | 30 | diagnostic MQ depth probe cadence (not a correctness poll) |
| `SENZING_REDO_SLEEP_TIME_IN_SECONDS` (`--redo-sleep-secs`) | 60 | fetcher pause on empty redo queue (auto-shortened to 2 s while redo is still in flight, for cascade drain) |
| `LONG_RECORD` (`--long-record`) | 300 | long-record threshold, seconds; stats cadence = LONG_RECORD/2 |
| `SENZING_LOG_LEVEL` | info | log level (`RUST_LOG` overrides) |
| `-i`/`--info` | off | print WithInfo payloads (engine-level no-op; print gating only) |
| `-t`/`--debugTrace` | off | engine debug trace |

Validation is loud (exit 1): redo% ∉ [0,100]; redo% < 100 without URL/queue;
0 < redo% < 100 with fewer than 2 threads; `LONG_RECORD` < 1. Exit codes:
**1** = configuration/validation failure at startup, **255** = fatal runtime
error (engine/DB/broker) after an orderly teardown, **0** = clean shutdown or
file EOF. (The standalone drivers disagreed with each other here; the combined
driver uses this one convention for both binaries.)

## SQS specifics

* **Dead-letter queue.** SQS has no reject verb, and an explicit `DeleteMessage`
  never triggers the redrive policy. So a rejected record (engine bad input,
  `SENZ0010` retry timeout, `SENZ0082`, unparseable body) is **`SendMessage`d
  to the DLQ verbatim, then deleted** from the source — `sz_sqs_consumer-v4`
  parity, `Sending to deadletter: DS : ID` on stdout. The DLQ is resolved once
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
  is on such a record: `All N threads are stuck on long running records`.
* **Settle.** Deletes are batched (`DeleteMessageBatch`, 10 per call, flushed
  every second and at shutdown). A fatal engine error leaves the message
  un-deleted so SQS redelivers it. Messages still in a worker at SIGTERM are
  printed as `Still processing (… min): DS : ID` and left for redelivery.
* **Stats.** Same `Processed N adds, R records per second`, `Engine stats:` and
  `Combined stats:` lines as the RabbitMQ binary; `mq_depth` is the source
  queue's `ApproximateNumberOfMessages`.
* **IAM.** On the source queue: `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes`. On the DLQ:
  `sqs:SendMessage`, `sqs:GetQueueUrl` (discovery only).

## Failure handling

* **Poison MQ record** (bad JSON / missing DATA_SOURCE/RECORD_ID / non-UTF-8 /
  engine BadInput / SENZ0082 / long-record give-up) → dead-letter
  (`basic_reject`, no requeue) + loud warn, keep running.
* **Poison redo record** (BadInput / retry timeout / SENZ0082) → warn (with the
  engine error text) + drop (no queue to reject to; counted as
  `redos_dropped`). SENZ0082 on a redo record is dropped rather than fatal on
  purpose: a DQM-rejected value can never succeed, and a fatal would wedge the
  redo queue on that one record.
* **File mode** has no queue: rejects go verbatim to the JSONL reject file
  (`--reject-file`, see above). **SQS** sends them to the dead-letter queue
  (see [SQS specifics](#sqs-specifics)).
* **Database connection lost / transient DB error** → **fatal** (orderly
  shutdown, exit 255), never dead-lettered: the database is unhealthy, not the
  record. Deliveries stay unacked / un-deleted so the broker redelivers them
  once the process is restarted.
* **Fatal errors** (Database, NotInitialized, License, …) → orderly teardown,
  non-zero exit. Graceful shutdown drains in-flight work within a 10 s grace;
  deliveries still inside a worker are dead-lettered (the engine call may still
  complete — requeue would double-process), queued-but-unstarted deliveries are
  left unacked for broker requeue. If a worker is still inside an
  uninterruptible engine call after the grace, the native environment destroy
  is skipped (leak-on-exit over use-after-free).
* A redo record fetched but not yet processed at crash/shutdown is lost from
  the redo queue's perspective (dequeued at fetch) — the tiny redo channel
  bounds this, and the harness's DB-side `SYS_EVAL_QUEUE` check remains the
  completion authority.

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
cargo test  --workspace --lib --bins                    # unit tests (no infra)

# Docker: BIN selects the backend binary; WITH_POSTGRES/WITH_MSSQL the DB closure.
docker build --build-arg BIN=sz_rabbit_combined_consumer -t brian/sz_rabbit_combined_consumer .        # both DB backends
docker build --build-arg BIN=sz_sqs_combined_consumer    -t brian/sz_sqs_combined_consumer .
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

File mode is a pure loader (no redo processing; drain redo separately with a
`--redo-percent 100` run). It runs to end-of-file and exits 0. Blank lines are
skipped. On completion — or on SIGTERM — it prints a safe resume offset;
restart with `--skip-lines N` to continue where it stopped (`add_record` is
idempotent, so an interrupted run is safe to resume).

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

## License

Apache-2.0
