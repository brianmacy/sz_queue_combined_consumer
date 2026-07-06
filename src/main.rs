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

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use clap::Parser;
use sz_rust_sdk::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use sz_rabbit_combined_consumer::config::{Args, Config};
use sz_rabbit_combined_consumer::stats::RUNNING;
use sz_rabbit_combined_consumer::{INSTANCE_NAME, combined, pure_redoer, stats};

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

    // Use-after-free guard (redoer FIX-3): only destroy the global Senzing
    // environment if EVERY worker finished within the shutdown grace window;
    // otherwise skip teardown and let process exit reclaim resources.
    if workers_clean {
        if let Err(e) = SzEnvironmentCore::destroy_global_instance() {
            tracing::warn!("Error during Senzing environment teardown: {e}");
        }
    } else {
        tracing::warn!(
            "Skipping Senzing environment teardown: a worker is still running an \
             uninterruptible engine call; letting process exit reclaim resources"
        );
    }

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e:#}");
            ExitCode::from(255)
        }
    }
}

/// redo% < 100: tokio runtime for the AMQP I/O layer only.
fn run_combined(config: Config, env: Arc<SzEnvironmentCore>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
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
    // from `run()` is also treated conservatively as "do not destroy".
    match &result {
        Ok(outcome) => {
            if outcome.all_workers_joined {
                if let Err(e) = SzEnvironmentCore::destroy_global_instance() {
                    tracing::warn!("error destroying Senzing environment: {e}");
                }
            } else {
                tracing::warn!(
                    "skipping Senzing environment destroy: a worker may still be in an \
                     engine call (leak-on-exit to avoid use-after-free)"
                );
            }
            match &outcome.fatal {
                None => ExitCode::SUCCESS,
                Some(msg) => {
                    eprintln!("Shutting down due to error: {msg}");
                    ExitCode::from(255)
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "skipping Senzing environment destroy after run() error \
                 (leak-on-exit to avoid use-after-free)"
            );
            eprintln!("{e:#}");
            ExitCode::from(255)
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
