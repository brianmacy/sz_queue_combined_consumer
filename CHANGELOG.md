# Changelog

## Unreleased — remove count_redo anti-pattern; config/license diagnostics (2026-07-17)

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

## Unreleased — bound native teardown on shutdown (issue #4, merged from main / PR #5)

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
