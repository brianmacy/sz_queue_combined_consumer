# sz_rabbit_combined_consumer

Combined Senzing **load + redo** driver in Rust. One binary subsumes both
[`sz_rabbit_consumer_rust`](../sz_rabbit_consumer_rust) (RabbitMQ →
`add_record`) and [`sz_simple_redoer_rust`](../sz_simple_redoer_rust)
(`get_redo_record` → `process_redo_record`), governed by a single
`SENZING_REDO_PERCENT` knob. Like its siblings, the container is **distroless**
(no interpreter, no shell) and glue-layer errors surface at compile time.

Design document: `~/.claude/plans/dbperf_combined_consumer_design.md`.

## Why combined

One worker pool = one DB-connection pool. In the split consumer+redoer topology
the two pools are sized at launch and cannot borrow from each other: the redoer
pool lags during load (SYS_EVAL_QUEUE grows) and the consumer pool idles during
the redo tail. Here the same capacity flows to whichever work exists:

| redo% | Behavior | Subsumes |
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
| `SENZING_PREFETCH` (`--prefetch`) | threads + 2 | `basic_qos` prefetch |
| `SENZING_MQ_RECHECK_SECONDS` (`--mq-recheck-secs`) | 30 | diagnostic MQ depth probe cadence (not a correctness poll) |
| `SENZING_REDO_SLEEP_TIME_IN_SECONDS` (`--redo-sleep-secs`) | 60 | fetcher pause on empty redo queue (auto-shortened to 2 s while redo is still in flight, for cascade drain) |
| `LONG_RECORD` (`--long-record`) | 300 | long-record threshold, seconds; stats cadence = LONG_RECORD/2 |
| `SENZING_LOG_LEVEL` | info | log level (`RUST_LOG` overrides) |
| `-i`/`--info` | off | print WithInfo payloads (engine-level no-op; print gating only) |
| `-t`/`--debugTrace` | off | engine debug trace |

Validation is loud (exit 1): redo% ∉ [0,100]; redo% < 100 without URL/queue;
0 < redo% < 100 with fewer than 2 threads.

## Failure handling

* **Poison MQ record** (bad JSON / missing DATA_SOURCE/RECORD_ID / non-UTF-8 /
  engine BadInput / SENZ0082 / long-record give-up) → dead-letter
  (`basic_reject`, no requeue) + loud warn, keep running.
* **Poison redo record** (BadInput / retry timeout) → warn + drop (no queue to
  reject to; counted as `redos_dropped`).
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

## Build

```console
cargo build --release           # needs libSz at SENZING_LIB_PATH (default /opt/senzing/er/lib)
cargo test                      # unit tests
docker build -t brian/sz_rabbit_combined_consumer .                      # both DB backends
docker build --build-arg WITH_MSSQL=0    -t brian/sz_rabbit_combined_consumer:pg .
docker build --build-arg WITH_POSTGRES=0 -t brian/sz_rabbit_combined_consumer:mssql .
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

## License

Apache-2.0
