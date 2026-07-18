# Changelog

## Unreleased — fix SIGTERM shutdown hang, take 2 (issue #4, reopened)

* **`src/main.rs` — terminate via `_exit(2)` so the native library's atexit /
  C++ static-destructor teardown can never wedge the exit.** PR #5 bounded the
  EXPLICIT `destroy_global_instance()` call to `TEARDOWN_GRACE` and then called
  `std::process::exit(code)` — but the four e2e tests STILL hung the full 30s
  SIGTERM grace in every mode (tokio combined AND pure-std redoer). Root cause:
  `std::process::exit` calls the C library `exit(3)`, which runs `atexit`
  handlers and the **C++ static destructors registered by the native Senzing
  library (libSz)**. Those perform the very same engine teardown that PR #5 found
  wedges — so bounding the explicit destroy and then calling `process::exit`
  merely RELOCATED the identical hang into `exit()`'s static-destructor phase,
  which nothing bounds. Every shutdown path funnels through the terminal exit,
  which is why every mode hung regardless of signal mechanism.

  The fix introduces `hard_exit(code)`: flush Rust's stdout (preserving the
  e2e-scraped "Processed total ..." line), then `libc::_exit(code)` — the raw
  `_exit(2)` syscall wrapper, which returns the process to the OS AT ONCE without
  running any atexit handler or static destructor. All terminal exits
  (`teardown_and_exit` and the leak-on-exit branches in both `run_combined` and
  `run_pure_redoer`) now go through it. The best-effort bounded
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
