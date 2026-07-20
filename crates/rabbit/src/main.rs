//! Entry point for the combined RabbitMQ load + redo Senzing driver.
//!
//! Branches on the `redo%` endpoints per the design (§1.1):
//! * `--file` — pure file loader (no AMQP, no tokio).
//! * redo% = 100 — the tokio runtime is never BUILT and no AMQP connection is
//!   opened; the pure `std::thread` redoer path runs instead.
//! * redo% < 100 — the consumer-shaped tokio/lapin path runs (which itself
//!   skips all redo machinery at redo% = 0).
//!
//! Shared bring-up / shutdown / redoer / file-loader logic lives in
//! `sz_combined_consumer_core::runtime`; only the AMQP load loop is local.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{Args, Config};
use sz_combined_consumer_core::{INSTANCE_NAME, runtime};

/// RabbitMQ ingestion loop (AMQP-specific; lives in this bin so `lapin` never
/// compiles into the SQS binary).
mod combined;

fn main() -> ExitCode {
    runtime::init_logging();

    let config = match Config::resolve(Args::parse()) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(1); // loud validation failure (design §5)
        }
    };

    let env = match runtime::init_environment(INSTANCE_NAME, &config) {
        Ok(e) => e,
        Err(code) => return code,
    };

    if config.input_file.is_some() {
        runtime::run_file_loader(&config, env)
    } else if config.redo_percent == 100 {
        runtime::run_pure_redoer(&config, env)
    } else {
        run_combined(config, env)
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
fn run_combined(config: Config, env: Arc<SzEnvironmentCore>) -> ! {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build tokio runtime: {e}");
            runtime::leak_and_exit(255);
        }
    };

    let result = runtime.block_on(combined::run(config, env));

    // Use-after-free guard (consumer FIX-2): only tear down the Senzing
    // environment when EVERY engine thread actually finished. A startup `Err`
    // from `run()` is also treated conservatively as "do not destroy". In every
    // case we terminate via `process::exit` rather than returning: returning would
    // drop the tokio runtime and `Arc<env>`, either of which can wedge on a stuck
    // native thread and overrun the SIGTERM grace (issue #4).
    match result {
        Ok(outcome) => {
            let code: u8 = match &outcome.fatal {
                None => 0,
                Some(msg) => {
                    eprintln!("Shutting down due to error: {msg}");
                    255
                }
            };
            if outcome.all_workers_joined {
                runtime::teardown_and_exit(code);
            } else {
                tracing::warn!(
                    "skipping Senzing environment destroy: a worker may still be in an \
                     engine call (leak-on-exit to avoid use-after-free); forcing process exit"
                );
                runtime::leak_and_exit(code);
            }
        }
        Err(e) => {
            eprintln!("{e:#}");
            tracing::warn!("run() failed; skipping native teardown (leak-on-exit), forcing exit");
            runtime::leak_and_exit(255);
        }
    }
}
