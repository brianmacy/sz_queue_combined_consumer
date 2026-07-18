# Changelog

## Unreleased — fix SIGTERM shutdown hang, take 2 (issue #4, reopened)

* **`src/main.rs` — unblock SIGTERM/SIGINT/SIGHUP after engine init so the
  shutdown handlers can fire (the actual root cause).** PR #5's bounded teardown
  did not help because the driver never observed SIGTERM at all: the captured
  child shutdown log (CI run 88091597511) shows every failing driver running its
  normal loop right up to the 30s SIGKILL with **no `shutting down` line ever
  logged** — the `RUNNING` flag was never flipped, in BOTH the tokio combined
  path and the pure-std redoer path. `SzEnvironmentCore::get_instance` (native
  `Sz_init`) leaves SIGTERM/SIGINT/SIGHUP **blocked in the process signal mask**;
  a blocked signal is never delivered, so neither the tokio `signal::unix`
  handler nor the `ctrlc` handler (both installed after engine init) ever ran.
  The fix calls `pthread_sigmask(SIG_UNBLOCK, …)` for those three signals on the
  main thread immediately after `get_instance`, before any worker / runtime /
  handler thread is spawned (they inherit the unblocked mask). Senzing's own
  `signal_handler.py` example installs its handler *before* the factory for the
  same reason.

* **`src/main.rs` — terminate via `_exit(2)` so the native library's atexit /
  C++ static-destructor teardown can never wedge the exit (complementary
  hardening).** Even once the signal is observed, `std::process::exit` calls the
  C library `exit(3)`, which runs `atexit` handlers and the C++ static
  destructors registered by libSz — those perform the same engine teardown PR #5
  found can wedge (the `Sz_destroy`-equivalent cleanup of per-thread native/DB
  state). PR #5 bounded the EXPLICIT `destroy_global_instance()` call but then
  still called `std::process::exit`, which would relocate that hang into
  `exit()`'s static-destructor phase (nothing bounds it). The fix introduces
  `hard_exit(code)`: flush Rust's stdout (preserving the e2e-scraped
  "Processed total ..." line), then `libc::_exit(code)` — the raw `_exit(2)`
  syscall wrapper, which returns the process to the OS AT ONCE without running
  any atexit handler or static destructor. All terminal exits
  (`teardown_and_exit` and the leak-on-exit branches in both `run_combined` and
  `run_pure_redoer`) go through it. The best-effort bounded
  `destroy_global_instance()` attempt is retained for a clean DB checkpoint when
  it returns promptly; when it wedges we `_exit` after the grace — the same
  leak-on-exit trade already accepted for a stuck worker. Exit code is preserved
  (0 on clean success), so `status.success()` in the e2e tests holds.

* **`tests/integration_test.rs` — dump the captured child stdout on every
  SIGTERM-timeout failure** (`unwrap_or_else(|| panic!(... "\n{stdout}"))`),
  matching the existing `\n{stdout}` diagnostic pattern, so a future shutdown
  regression shows the driver's own shutdown log instead of a bare message.

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
