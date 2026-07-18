# Changelog

## Unreleased — fix SIGTERM shutdown hang (issue #4)

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
