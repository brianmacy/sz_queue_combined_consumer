//! Entry point for the combined RabbitMQ load + redo Senzing driver.
//!
//! Branches on the `redo%` endpoints per the design (§1.1):
//! * redo% = 100 — the tokio runtime is never BUILT and no AMQP connection is
//!   opened; the pure `std::thread` redoer path runs instead.
//! * redo% < 100 — the consumer-shaped tokio/lapin path runs (which itself
//!   skips all redo machinery at redo% = 0).
//!
//! Both paths share the use-after-free guard from the sibling drivers: the
//! global Senzing environment is destroyed ONLY when every engine thread
//! finished within the shutdown grace; otherwise we leak-on-exit deliberately.

use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use clap::Parser;
use sz_rust_sdk::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use sz_rabbit_combined_consumer::config::{Args, Config};
use sz_rabbit_combined_consumer::stats::RUNNING;
use sz_rabbit_combined_consumer::{INSTANCE_NAME, combined, pure_redoer, stats};

/// Upper bound on the native environment teardown at shutdown.
///
/// `SzEnvironmentCore::destroy_global_instance()` calls `Sz_destroy()`, an
/// uninterruptible native FFI call with no timeout. On the SIGTERM shutdown path
/// exercised by the e2e integration tests it can BLOCK indefinitely: once the
/// worker threads that made engine calls have exited, the engine's per-thread DB
/// connections / native state outlive them and `Sz_destroy()` wedges cleaning
/// them up, so the process never exits and overruns `docker stop`'s SIGTERM
/// grace (issue #4: "driver hangs >30s on SIGTERM shutdown"). We therefore run
/// the teardown on a dedicated thread and wait only up to this bound; past it the
/// process is exiting anyway, so we hard-exit and let the OS reclaim native
/// resources (no use-after-free — the whole process is gone). Kept well under the
/// 30s test grace and the sibling drivers' 10s worker-join grace.
const TEARDOWN_GRACE: Duration = Duration::from_secs(5);

/// Tear the native Senzing environment down (best effort, time-bounded) and then
/// terminate the process with `code` — GUARANTEEING a prompt exit within the
/// SIGTERM grace regardless of whether `Sz_destroy()`, the tokio runtime drop, or
/// any `Arc<env>` drop would otherwise wedge (issue #4).
///
/// `destroy_global_instance()` is attempted on a dedicated thread; if it has not
/// returned within [`TEARDOWN_GRACE`] we stop waiting and hard-exit. `Sz_destroy`
/// may still be running on that detached thread, but `process::exit` reclaims it
/// along with the rest of the process — the same leak-on-exit trade the drivers
/// already accept when a worker is stuck in an uninterruptible engine call.
///
/// Only ever called AFTER the workers/fetcher have been joined (or deliberately
/// detached) and stdout has the final totals, so exiting here loses nothing.
fn teardown_and_exit(code: u8) -> ! {
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let spawned = std::thread::Builder::new()
        .name("sz-teardown".to_string())
        .spawn(move || {
            if let Err(e) = SzEnvironmentCore::destroy_global_instance() {
                tracing::warn!("error destroying Senzing environment: {e}");
            }
            let _ = done_tx.send(());
        });

    match spawned {
        // recv_timeout returns Err on both timeout AND a dropped sender (e.g. the
        // teardown thread panicked); either way we have waited long enough.
        Ok(_) => match done_rx.recv_timeout(TEARDOWN_GRACE) {
            Ok(()) => tracing::info!("Senzing environment destroyed; exiting cleanly"),
            Err(_) => tracing::warn!(
                "native teardown did not complete within {TEARDOWN_GRACE:?}; forcing \
                 process exit (OS reclaims native resources)"
            ),
        },
        Err(e) => tracing::warn!("could not spawn teardown thread ({e}); forcing process exit"),
    }

    // Flush stdout so the final "Processed total ..." line the e2e tests scrape is
    // never lost to process::exit skipping Rust's buffered-writer drop.
    let _ = std::io::stdout().flush();
    std::process::exit(code as i32);
}

fn main() -> ExitCode {
    // Logging: SENZING_LOG_LEVEL controls the default level (parity with the
    // sibling drivers), RUST_LOG (env-filter) can override for finer control.
    let log_level = std::env::var("SENZING_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(map_log_level(&log_level)));
    fmt().with_env_filter(env_filter).with_target(false).init();

    // Anchor the process start time for throughput lines.
    let _ = stats::start_time();

    let args = Args::parse();
    let config = match Config::resolve(args) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(1); // loud validation failure (design §5)
        }
    };

    // Initialize the Senzing environment singleton: exactly ONE Sz_init per
    // process; every thread derives its own engine handle from it.
    let env: Arc<SzEnvironmentCore> = match SzEnvironmentCore::get_instance(
        INSTANCE_NAME,
        &config.engine_config,
        config.debug_trace,
    ) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to initialize Senzing environment: {e}");
            return ExitCode::from(255);
        }
    };

    if config.redo_percent == 100 {
        run_pure_redoer(config, env)
    } else {
        run_combined(config, env)
    }
}

/// redo% = 100: pure std::thread shape; graceful shutdown via ctrlc
/// (termination feature covers SIGTERM/SIGHUP as well as SIGINT).
fn run_pure_redoer(config: Config, env: Arc<SzEnvironmentCore>) -> ExitCode {
    if let Err(e) = ctrlc::set_handler(|| {
        tracing::warn!("Graceful shutdown requested");
        RUNNING.store(false, Ordering::Relaxed);
    }) {
        tracing::warn!("Could not install signal handler: {e}");
    }

    let (workers_clean, result) = pure_redoer::run(&config, env);

    let code = match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e:#}");
            255
        }
    };

    // Use-after-free guard (redoer FIX-3): only destroy the global Senzing
    // environment if EVERY worker finished within the shutdown grace window.
    // Either way we terminate via `process::exit` so a wedged native teardown can
    // never overrun the SIGTERM grace (issue #4).
    if workers_clean {
        teardown_and_exit(code);
    } else {
        tracing::warn!(
            "worker still in an uninterruptible engine call at shutdown; skipping \
             native teardown and forcing process exit (restart-on-failure restarts clean)"
        );
        let _ = std::io::stdout().flush();
        std::process::exit(code as i32);
    }
}

/// redo% < 100: tokio runtime for the AMQP I/O layer only.
///
/// worker_threads is pinned to 2 (not the num_cpus default): this runtime only
/// drives AMQP consume/ack + a few timers and hands every record to the
/// dedicated `sz-worker` OS threads via channels — no Senzing/libSz FFI ever
/// runs on a tokio worker. On a high-core host the default (one worker per core,
/// e.g. 64) spawns dozens of idle runtime threads per process, each able to seed
/// its own glibc malloc arena; 2 is ample for the I/O layer.
fn run_combined(config: Config, env: Arc<SzEnvironmentCore>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build tokio runtime: {e}");
            return ExitCode::from(255);
        }
    };

    let result = runtime.block_on(combined::run(config, env));

    // Use-after-free guard (consumer FIX-2): only tear down the Senzing
    // environment when EVERY engine thread actually finished. A startup `Err`
    // from `run()` is also treated conservatively as "do not destroy". In every
    // case we terminate via `process::exit` rather than returning: returning would
    // drop the tokio runtime and `Arc<env>`, either of which can wedge on a stuck
    // native thread and overrun the SIGTERM grace (issue #4).
    match &result {
        Ok(outcome) => {
            let code: u8 = match &outcome.fatal {
                None => 0,
                Some(msg) => {
                    eprintln!("Shutting down due to error: {msg}");
                    255
                }
            };
            if outcome.all_workers_joined {
                teardown_and_exit(code);
            } else {
                tracing::warn!(
                    "skipping Senzing environment destroy: a worker may still be in an \
                     engine call (leak-on-exit to avoid use-after-free); forcing process exit"
                );
                let _ = std::io::stdout().flush();
                std::process::exit(code as i32);
            }
        }
        Err(e) => {
            eprintln!("{e:#}");
            tracing::warn!(
                "run() failed; skipping native teardown (leak-on-exit) and forcing process exit"
            );
            let _ = std::io::stdout().flush();
            std::process::exit(255);
        }
    }
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
