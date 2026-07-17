# Changelog

## Unreleased — remove count_redo anti-pattern; shutdown wedge fix; config/license diagnostics (2026-07-17)

* **`src/combined.rs`, `src/pure_redoer.rs` — removed `count_redo_records()`.** It issued
  `COUNT(*) FROM SYS_EVAL_QUEUE` (a full table scan) once per stats interval, which dominated
  DB user-CPU at scale. The redo-backlog gauge now reports `None` with a
  `TODO(reporting)` to restore it via a cheap source (engine redo counters or a DB-side
  estimate) rather than a full scan.
* **`src/main.rs` — shutdown wedge fix (both `run_combined` and `run_pure_redoer`).** On the
  leak-on-exit path (a worker still stuck in an uninterruptible libSz FFI call at shutdown),
  force `std::process::exit(255)` instead of returning `ExitCode`. Returning would run the
  tokio-runtime and `Arc<env>` drops, which can wedge on the stuck thread so the process never
  exits and the container never restarts; a hard exit lets the OS reclaim everything and
  restart-on-failure bring up a clean instance.
* **`src/config_reload.rs` — `reconcile()` now keys off `get_active_config_id()`** (the engine's
  real state) instead of the process-local `LAST_APPLIED` sentinel, which has been removed
  along with its `AtomicI64`. Refresh logging now records the active id before and after the
  reinitialize so a no-op reinit is visible.
* **Config/license diagnostics** — added `log_startup_config()` (logs `CONFIG AT INIT:
  active=… default=…` once per process after init), called from `combined.rs` and
  `pure_redoer.rs`; plus `LICENSE AFTER INIT` / `LICENSE AFTER REINIT` logging to detect a
  license drop across `reinitialize`.

## Unreleased — live config auto-reload (2026-07-16)

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
