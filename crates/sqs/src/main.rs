//! Entry point for the combined Amazon SQS load + redo Senzing driver.
//!
//! The SQS analogue of `sz_rabbit_combined_consumer`: it ingests records from an
//! SQS queue instead of RabbitMQ, sharing the entire engine-processing core
//! (worker pool, redo fetcher, stats, live config reload, file loader) via
//! `sz_combined_consumer_core`. `lapin` is never compiled into this binary.
//!
//! Modes:
//! * `--file` — pure file loader (shared core path; no SQS, no tokio).
//! * redo% = 100 — pure `std::thread` redoer (shared core path; no SQS).
//! * redo% < 100 — the SQS ingestion loop (this bin's `sqs` module).
//!
//! Credentials/region come from the standard AWS provider chain (env,
//! `~/.aws`, IMDS, ...) resolved by `aws-config`.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{
    Config, DEFAULT_LONG_RECORD_SECS, DEFAULT_REDO_PERCENT, engine_config_from_env,
};
use sz_combined_consumer_core::runtime;

mod sqs;

const INSTANCE_NAME: &str = "sz_sqs_combined_consumer";

/// Default SQS receive long-poll wait (seconds); 20 is the SQS maximum.
const DEFAULT_WAIT_TIME_SECS: i32 = 20;
/// Default SQS receive batch size; 10 is the SQS maximum.
const DEFAULT_MAX_MESSAGES: i32 = 10;
/// Default SQS visibility timeout (seconds). MUST exceed the worst-case record
/// processing time or SQS redelivers a record still being processed (duplicate
/// add). Defaults to 2x the long-record threshold.
const DEFAULT_VISIBILITY_TIMEOUT_SECS: i32 = (DEFAULT_LONG_RECORD_SECS as i32) * 2;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_sqs_combined_consumer",
    version,
    about = "Combined Senzing driver (Amazon SQS): add_record from an SQS queue and \
             process_redo_record, split by one redo%% knob (0 = pure loader, 100 = pure redoer)",
    long_about = None
)]
struct Args {
    /// SQS queue URL (required when redo% < 100 and --file is not set).
    #[arg(short = 'q', long = "queue-url", env = "SENZING_SQS_QUEUE_URL")]
    queue_url: Option<String>,

    /// SQS visibility timeout in seconds. MUST exceed the worst-case record
    /// processing time or SQS redelivers an in-progress record.
    #[arg(
        long = "visibility-timeout",
        env = "SENZING_SQS_VISIBILITY_TIMEOUT",
        default_value_t = DEFAULT_VISIBILITY_TIMEOUT_SECS
    )]
    visibility_timeout: i32,

    /// SQS receive long-poll wait time in seconds (0..=20).
    #[arg(long = "wait-time", env = "SENZING_SQS_WAIT_TIME", default_value_t = DEFAULT_WAIT_TIME_SECS)]
    wait_time: i32,

    /// SQS ReceiveMessage batch size (1..=10).
    #[arg(long = "max-messages", env = "SENZING_SQS_MAX_MESSAGES", default_value_t = DEFAULT_MAX_MESSAGES)]
    max_messages: i32,

    /// Load records from a single JSONL file instead of SQS (pure loader).
    #[arg(short = 'f', long = "file", env = "SENZING_INPUT_FILE")]
    input_file: Option<String>,

    /// File mode: skip the first N physical lines (resume an interrupted load).
    #[arg(long = "skip-lines", env = "SENZING_SKIP_LINES", default_value_t = 0)]
    skip_lines: u64,

    /// Share (%) of worker capacity preferring redo, in [0, 100].
    #[arg(long = "redo-percent", env = "SENZING_REDO_PERCENT", default_value_t = DEFAULT_REDO_PERCENT)]
    redo_percent: u8,

    /// Worker thread count. 0 auto-detects via available CPUs.
    #[arg(
        long = "threads-per-process",
        env = "SENZING_THREADS_PER_PROCESS",
        default_value_t = 12
    )]
    threads_per_process: usize,

    /// Seconds the redo fetcher pauses when no redo records are available.
    #[arg(
        long = "redo-sleep-secs",
        env = "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        default_value_t = 60
    )]
    redo_sleep_secs: u64,

    /// Seconds before a record is considered long-running.
    #[arg(long = "long-record", env = "LONG_RECORD", default_value_t = DEFAULT_LONG_RECORD_SECS)]
    long_record: u64,

    /// Print the WithInfo response for each processed record.
    #[arg(short = 'i', long = "info", default_value_t = false)]
    info: bool,

    /// Output Senzing engine debug trace information.
    #[arg(short = 't', long = "debugTrace", default_value_t = false)]
    debug_trace: bool,
}

/// SQS-specific ingestion parameters (validated), handed to the SQS run loop.
pub struct SqsParams {
    pub queue_url: String,
    pub visibility_timeout: i32,
    pub wait_time: i32,
    pub max_messages: i32,
}

fn main() -> ExitCode {
    runtime::init_logging();

    let args = Args::parse();
    let config = match build_config(&args) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(1);
        }
    };

    let env = match runtime::init_environment(INSTANCE_NAME, &config) {
        Ok(e) => e,
        Err(code) => return code,
    };

    // Shared modes reuse the core runtime; the SQS load loop is local.
    if config.input_file.is_some() {
        runtime::run_file_loader(&config, env)
    } else if config.redo_percent == 100 {
        runtime::run_pure_redoer(&config, env)
    } else {
        let params = match sqs_params(&args) {
            Ok(p) => p,
            Err(msg) => {
                eprintln!("{msg}");
                return ExitCode::from(1);
            }
        };
        run_sqs(config, params, env)
    }
}

/// Builds a core `Config` from the SQS binary's own args. url/queue stay `None`
/// (SQS has no AMQP topology); prefetch/mq-recheck are AMQP-only and left at inert
/// defaults. Shared validation (engine JSON, redo% range) is applied here.
fn build_config(args: &Args) -> Result<Config, String> {
    let engine_config = engine_config_from_env()?;
    if args.redo_percent > 100 {
        return Err(format!(
            "SENZING_REDO_PERCENT must be within [0, 100], got {}",
            args.redo_percent
        ));
    }
    let threads = if args.threads_per_process == 0 {
        num_cpus_get()
    } else {
        args.threads_per_process
    };
    if args.redo_percent > 0 && args.redo_percent < 100 && threads < 2 {
        return Err(format!(
            "0 < redo% < 100 requires at least 2 worker threads (got {threads})"
        ));
    }
    Ok(Config {
        engine_config,
        url: None,
        queue: None,
        input_file: args.input_file.clone().filter(|s| !s.is_empty()),
        skip_lines: args.skip_lines,
        redo_percent: args.redo_percent,
        threads,
        prefetch: 0,
        mq_recheck_secs: 0,
        redo_sleep_secs: args.redo_sleep_secs,
        long_record_secs: args.long_record,
        info: args.info,
        debug_trace: args.debug_trace,
    })
}

/// Validates and extracts the SQS ingestion parameters (SQS load path only).
fn sqs_params(args: &Args) -> Result<SqsParams, String> {
    let queue_url = args.queue_url.clone().filter(|s| !s.is_empty()).ok_or(
        "No SQS queue URL provided (use --queue-url or SENZING_SQS_QUEUE_URL); \
             required when redo% < 100",
    )?;
    if !(0..=20).contains(&args.wait_time) {
        return Err(format!(
            "--wait-time must be 0..=20 (SQS long-poll max), got {}",
            args.wait_time
        ));
    }
    if !(1..=10).contains(&args.max_messages) {
        return Err(format!(
            "--max-messages must be 1..=10 (SQS batch max), got {}",
            args.max_messages
        ));
    }
    if args.visibility_timeout <= args.long_record as i32 {
        eprintln!(
            "warning: --visibility-timeout ({}) <= --long-record ({}); SQS may redeliver a \
             record still being processed. Set it above the worst-case processing time.",
            args.visibility_timeout, args.long_record
        );
    }
    Ok(SqsParams {
        queue_url,
        visibility_timeout: args.visibility_timeout,
        wait_time: args.wait_time,
        max_messages: args.max_messages,
    })
}

fn num_cpus_get() -> usize {
    // Small local shim so the bin need not depend on num_cpus directly; the core
    // already uses it, but Config construction here just needs a sane default.
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// redo% < 100: build a tokio runtime for the SQS I/O layer only (engine calls
/// run on the dedicated `sz-worker` OS threads, never on tokio workers) and run
/// the SQS ingestion loop, then apply the shared use-after-free exit discipline.
fn run_sqs(config: Config, params: SqsParams, env: Arc<SzEnvironmentCore>) -> ! {
    let rt = match tokio::runtime::Builder::new_multi_thread()
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

    match rt.block_on(sqs::run(&config, &params, env)) {
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
                     engine call (leak-on-exit); forcing process exit"
                );
                runtime::leak_and_exit(code);
            }
        }
        Err(e) => {
            eprintln!("{e:#}");
            tracing::warn!("SQS run() failed; leak-on-exit, forcing process exit");
            runtime::leak_and_exit(255);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse the SQS binary's args from a CLI vector (clap), so these tests also
    /// exercise the flag wiring. Only SQS-specific flags need setting; the rest
    /// take their declared defaults.
    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["sz_sqs_combined_consumer"];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    #[test]
    fn sqs_params_ok_with_queue_url_and_defaults() {
        let p = sqs_params(&args(&["--queue-url", "https://sqs.example/q"]))
            .expect("valid queue url + default bounds should parse");
        assert_eq!(p.queue_url, "https://sqs.example/q");
        assert_eq!(p.wait_time, DEFAULT_WAIT_TIME_SECS);
        assert_eq!(p.max_messages, DEFAULT_MAX_MESSAGES);
    }

    #[test]
    fn sqs_params_requires_queue_url() {
        assert!(
            sqs_params(&args(&[])).is_err(),
            "missing --queue-url must be a loud error on the SQS load path"
        );
    }

    #[test]
    fn sqs_params_rejects_out_of_range_wait_time() {
        assert!(sqs_params(&args(&["--queue-url", "u", "--wait-time", "21"])).is_err());
        assert!(sqs_params(&args(&["--queue-url", "u", "--wait-time", "0"])).is_ok());
    }

    #[test]
    fn sqs_params_rejects_out_of_range_max_messages() {
        assert!(sqs_params(&args(&["--queue-url", "u", "--max-messages", "0"])).is_err());
        assert!(sqs_params(&args(&["--queue-url", "u", "--max-messages", "11"])).is_err());
        assert!(sqs_params(&args(&["--queue-url", "u", "--max-messages", "10"])).is_ok());
    }
}
