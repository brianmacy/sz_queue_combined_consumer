//! Shared binary runtime helpers used by every backend driver (RabbitMQ, SQS).
//!
//! Each backend's `main()` parses its own CLI args and dispatches; the AMQP /
//! SQS load loops live in their respective bins. But everything here — logging
//! init, environment bring-up, bounded native teardown, and the two
//! backend-agnostic run modes (pure redoer, file loader) — is identical across
//! backends, so it lives here to avoid duplication.

use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use sz_rust_sdk::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Config;
use crate::stats::{self, RUNNING};
use crate::{file_loader, pure_redoer};

/// Upper bound on the native environment teardown at shutdown.
///
/// `SzEnvironmentCore::destroy()` calls `Sz_destroy()`, an
/// uninterruptible native FFI call with no timeout that can BLOCK indefinitely on
/// the SIGTERM shutdown path once the worker threads that made engine calls have
/// exited. We run it on a dedicated thread and wait only up to this bound; past
/// it the process is exiting anyway, so we hard-exit and let the OS reclaim
/// native resources (no use-after-free — the whole process is gone). Kept well
/// under the sibling drivers' 10s worker-join grace and the 30s e2e test grace.
const TEARDOWN_GRACE: Duration = Duration::from_secs(5);

/// How long a NORMAL exit (`std::process::exit`, run after a completed
/// `Sz_destroy()`) may take before a watchdog forces `_exit`. A clean exit finishes in
/// milliseconds; this only bounds exit-time destructors that block (libSz's
/// thread-local destructor makes a database call with no timeout). Keep small: it adds
/// to the worker-join and teardown graces on the shutdown path.
const EXIT_GRACE: Duration = Duration::from_secs(2);

/// Installs the tracing subscriber (SENZING_LOG_LEVEL default, RUST_LOG override)
/// and anchors the process start time for throughput lines. Call once at startup.
pub fn init_logging() {
    let log_level = std::env::var("SENZING_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(map_log_level(&log_level)));
    fmt().with_env_filter(env_filter).with_target(false).init();
    let _ = stats::start_time();
}

/// Maps the Python-style SENZING_LOG_LEVEL names onto tracing levels.
fn map_log_level(level: &str) -> &'static str {
    match level.to_lowercase().as_str() {
        "notset" | "debug" => "debug",
        "warning" | "warn" => "warn",
        "error" => "error",
        "fatal" | "critical" => "error",
        _ => "info",
    }
}

/// Initializes the Senzing environment singleton (exactly one `Sz_init` per
/// process; every thread derives its own engine handle). On failure prints a loud
/// message and returns the process exit code the caller should return.
pub fn init_environment(
    instance_name: &str,
    config: &Config,
) -> Result<Arc<SzEnvironmentCore>, ExitCode> {
    match SzEnvironmentCore::get_instance(instance_name, &config.engine_config, config.debug_trace)
    {
        Ok(e) => Ok(e),
        Err(e) => {
            eprintln!("Failed to initialize Senzing environment: {e}");
            Err(ExitCode::from(255))
        }
    }
}

/// Installs the graceful-shutdown signal handler (SIGINT/SIGTERM/SIGHUP via the
/// ctrlc `termination` feature) that flips `RUNNING` to false.
pub fn install_shutdown_handler() {
    if let Err(e) = ctrlc::set_handler(|| {
        tracing::warn!("Graceful shutdown requested");
        RUNNING.store(false, Ordering::Relaxed);
    }) {
        tracing::warn!("Could not install signal handler: {e}");
    }
}

/// Tears the native Senzing environment down (best effort, time-bounded) and then
/// terminates the process with `code`, GUARANTEEING a prompt exit within the
/// SIGTERM grace regardless of whether `Sz_destroy()` / runtime drops would wedge.
///
/// Only ever called AFTER the workers/fetcher have joined (or been deliberately
/// detached) and stdout has the final totals, so exiting here loses nothing.
pub fn teardown_and_exit(code: u8) -> ! {
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let spawned = std::thread::Builder::new()
        .name("sz-teardown".to_string())
        .spawn(move || {
            // Ownership-based teardown (replaces the removed
            // `destroy_global_instance()`): reacquire the process singleton and
            // consume it. `destroy()` first removes the global reference, then
            // `Arc::try_unwrap`s — native `Sz_destroy()` only runs when this is
            // the SOLE remaining reference. We are only ever called AFTER all
            // worker/fetcher threads (and their `Arc<SzEnvironmentCore>` clones)
            // have joined, so this clone plus the singleton are the only two refs
            // and try_unwrap succeeds. If a clone somehow survived, destroy()
            // returns an error (logged) and safely skips the native call rather
            // than risking a use-after-free.
            match SzEnvironmentCore::get_existing_instance() {
                Ok(env) => {
                    if let Err(e) = env.destroy() {
                        tracing::warn!("error destroying Senzing environment: {e}");
                    }
                }
                Err(e) => tracing::warn!("no Senzing environment to destroy: {e}"),
            }
            let _ = done_tx.send(());
        });

    match spawned {
        // recv_timeout errors on both timeout AND a dropped sender (teardown
        // thread panicked); either way we have waited long enough.
        Ok(_) => match done_rx.recv_timeout(TEARDOWN_GRACE) {
            Ok(()) => {
                tracing::info!("Senzing environment destroyed; exiting cleanly");
                // Sz_destroy() completed and no worker is inside an engine call on
                // this branch, so a normal exit (atexit handlers, thread-local and
                // static destructors) is safe and preferred.
                flush_and_exit(code)
            }
            Err(_) => {
                tracing::warn!(
                    "native teardown did not complete within {TEARDOWN_GRACE:?}; forcing \
                     process exit (OS reclaims native resources)"
                );
                // The teardown thread is still inside Sz_destroy(): MUST NOT run atexit.
                flush_and_force_exit(code)
            }
        },
        Err(e) => {
            tracing::warn!("could not spawn teardown thread ({e}); forcing process exit");
            flush_and_force_exit(code)
        }
    }
}

/// Hard-exit WITHOUT attempting the native teardown (leak-on-exit): used when a
/// worker is still in an uninterruptible engine call, so `Sz_destroy()` would
/// risk a use-after-free / wedge. The OS reclaims everything.
pub fn leak_and_exit(code: u8) -> ! {
    flush_and_force_exit(code)
}

/// Normal exit: flush stdout, then `std::process::exit` (runs libc atexit handlers
/// and destructors) — bounded by a watchdog. Wait for a clean exit for
/// [`EXIT_GRACE`], then force `_exit` with the same code. Only for paths where
/// no native thread is stuck in the engine.
fn flush_and_exit(code: u8) -> ! {
    // Flush stdout so the final "Processed total ..." line the e2e tests scrape is
    // never lost to process::exit skipping Rust's buffered-writer drop.
    let _ = std::io::stdout().flush();
    // If spawning fails we fall through to the plain exit: the watchdog is a
    // safety net, not a precondition.
    let _ = std::thread::Builder::new()
        .name("sz-exit-watchdog".to_string())
        .spawn(move || {
            std::thread::sleep(EXIT_GRACE);
            tracing::warn!(
                "normal process exit did not finish within {EXIT_GRACE:?}; forcing _exit"
            );
            flush_and_force_exit(code)
        });
    std::process::exit(code as i32);
}

/// Exit that CANNOT be blocked by native-library teardown code.
///
/// ⚠ `std::process::exit` calls libc `exit(3)`, which runs atexit handlers and
/// thread-local / static destructors. Reproduced on Linux (Senzing 4.3.3, glibc
/// 2.41, workers wedged by a paused database): `exit` → `__call_tls_dtors` →
/// libSz's thread-local destructor on the exiting thread → its own database call
/// (`PQexec` → `PQgetResult` → `poll`). libpq has no timeout, so when the database
/// connection is unresponsive the destructor never returns, and `exit(3)` hangs in
/// exactly the situation these forced-exit paths exist to escape. No lock held by a
/// wedged worker is involved. Exiting with workers still inside the engine is also
/// a use-after-free path (the destructor ran SQL with corrupted text after the
/// database came back).
///
/// Observed in production 2026-07-27: 17 of 20 consumers on one host logged
/// "skipping Senzing environment destroy … forcing process exit" during a database
/// restart and then **never exited**. Because the process never terminated,
/// `RestartPolicy=on-failure` never fired: the containers stayed `Up`, RestartCount
/// stayed flat, their logs went silent, and the last-emitted stats blob kept
/// reporting a healthy `adds_rate`. The fleet ran at 57% capacity for 80 minutes and
/// every container-level health check reported it healthy — the only observable was
/// the host's AMQP consumer count (3 instead of 20). It is intermittent because a
/// database connection that receives an RST/FIN fails fast and `exit` completes; it
/// hangs only when the connection is unresponsive at exit. (Why production's
/// connections stayed unresponsive for 80 minutes is not established.)
///
/// `_exit(2)` terminates immediately without running atexit handlers or
/// destructors, so it cannot be blocked by them. We flush our own stdout/stderr
/// first (stdout carries the final "Processed total ..." line the e2e tests
/// scrape); those flushes cannot block here because the WARN line preceding a
/// forced exit has already been written through the same writers.
fn flush_and_force_exit(code: u8) -> ! {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    // SAFETY: `_exit` is async-signal-safe and never returns. It deliberately skips
    // atexit handlers / static destructors — that is the entire point of this path.
    unsafe { libc::_exit(code as i32) }
}

/// Applies the shared use-after-free exit discipline given `(workers_clean,
/// result)` from a run: destroy + exit on a clean join, else leak-on-exit.
fn finish(workers_clean: bool, result: anyhow::Result<()>) -> ! {
    let code = match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e:#}");
            255
        }
    };
    if workers_clean {
        teardown_and_exit(code);
    } else {
        tracing::warn!(
            "worker still in an uninterruptible engine call at shutdown; skipping \
             native teardown and forcing process exit (restart-on-failure restarts clean)"
        );
        leak_and_exit(code);
    }
}

/// redo% = 100: pure `std::thread` redoer (no AMQP/SQS, no tokio). Never returns.
pub fn run_pure_redoer(config: &Config, env: Arc<SzEnvironmentCore>) -> ! {
    install_shutdown_handler();
    let (workers_clean, result) = pure_redoer::run(config, env);
    finish(workers_clean, result)
}

/// File-input mode: pure `std::thread` loader reading JSONL from a single file
/// (no AMQP/SQS, no tokio). Never returns.
pub fn run_file_loader(config: &Config, env: Arc<SzEnvironmentCore>) -> ! {
    install_shutdown_handler();
    let (workers_clean, result) = file_loader::run(config, env);
    finish(workers_clean, result)
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    /// Set in the re-executed child to select its role.
    const CHILD_ENV: &str = "SZ_FORCED_EXIT_TEST_CHILD";
    /// Printed WITHOUT a trailing newline, so it is still sitting in Rust's
    /// line-buffered stdout when the exit helper runs: seeing it proves the flush.
    const MARKER: &str = "Processed total of 42 (forced-exit marker)";
    const EXIT_CODE: u8 = 7;
    /// Far longer than a real `_exit` takes; a hang is "never", not "slow".
    const DEADLINE: Duration = Duration::from_secs(20);

    /// Stands in for exit-time native teardown that never returns (in production:
    /// libSz's thread-local destructor waiting forever on an unresponsive database
    /// connection): an atexit handler that never returns.
    extern "C" fn wedged_atexit_handler() {
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }

    /// `CHILD_ENV` values selecting which exit helper the child takes.
    const ROLE_FORCED: &str = "forced";
    const ROLE_NORMAL: &str = "normal";

    /// Child role: register the never-returning atexit handler, leave the marker
    /// unflushed, then take the exit path selected by `role`.
    fn run_child(role: &str) -> ! {
        // SAFETY: registers a plain `extern "C" fn()` with libc; no captured state.
        assert_eq!(unsafe { libc::atexit(wedged_atexit_handler) }, 0);
        print!("{MARKER}");
        match role {
            ROLE_NORMAL => super::flush_and_exit(EXIT_CODE),
            _ => super::leak_and_exit(EXIT_CODE),
        }
    }

    /// Waits up to `DEADLINE` for the child; kills it and returns `None` on a hang.
    fn wait_with_deadline(child: &mut std::process::Child) -> Option<std::process::ExitStatus> {
        let start = Instant::now();
        while start.elapsed() < DEADLINE {
            if let Some(status) = child.try_wait().expect("try_wait") {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = child.kill();
        let _ = child.wait();
        None
    }

    /// Re-runs this test binary as a child on `test_name` with `role`, and asserts it
    /// exits with `EXIT_CODE` and the unflushed marker on stdout within `DEADLINE`.
    fn assert_child_exits_despite_wedged_atexit(test_name: &str, role: &str) {
        let mut child = Command::new(std::env::current_exe().expect("current_exe"))
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, role)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child test process");
        let status = wait_with_deadline(&mut child);
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .expect("child stdout")
            .read_to_string(&mut stdout)
            .expect("read child stdout");
        let status = status.unwrap_or_else(|| {
            panic!("exit hung >{DEADLINE:?} on a wedged atexit handler; stdout: {stdout}")
        });
        assert_eq!(
            status.code(),
            Some(i32::from(EXIT_CODE)),
            "stdout: {stdout}"
        );
        assert!(
            stdout.contains(MARKER),
            "stdout not flushed before exit: {stdout}"
        );
    }

    /// The forced-exit path must terminate even when an atexit handler would block
    /// forever (`std::process::exit` runs it and hangs; `_exit(2)` skips it), with
    /// the caller's exit code and stdout flushed.
    #[test]
    fn leak_and_exit_is_not_blocked_by_a_wedged_atexit_handler() {
        if std::env::var_os(CHILD_ENV).is_some() {
            run_child(ROLE_FORCED);
        }
        assert_child_exits_despite_wedged_atexit(
            "runtime::tests::leak_and_exit_is_not_blocked_by_a_wedged_atexit_handler",
            ROLE_FORCED,
        );
    }

    /// The NORMAL exit path (after a completed teardown) waits for a clean exit, then
    /// the watchdog forces `_exit` if an exit-time destructor blocks.
    #[test]
    fn normal_exit_is_bounded_by_the_watchdog() {
        if std::env::var_os(CHILD_ENV).is_some() {
            run_child(ROLE_NORMAL);
        }
        assert_child_exits_despite_wedged_atexit(
            "runtime::tests::normal_exit_is_bounded_by_the_watchdog",
            ROLE_NORMAL,
        );
    }
}
