//! Entry point for the combined Amazon SQS load + redo Senzing driver.
//!
//! The SQS analogue of `sz_rabbit_combined_consumer`: it ingests records from an
//! SQS queue instead of RabbitMQ, sharing the entire engine-processing core
//! (worker pool, redo fetcher, stats, live config reload, file loader) via
//! `sz_combined_consumer_core`. `lapin` is never compiled into this binary.
//!
//! Modes:
//! * `--file` — file loader + redo share (shared core path; no SQS, no tokio).
//! * redo% = 100 — pure `std::thread` redoer (shared core path; no SQS).
//! * redo% < 100 — the shared core queue loop with this bin's `SqsTransport`.
//!
//! Credentials/region come from the standard AWS provider chain (env,
//! `~/.aws`, IMDS, ...) resolved by `aws-config`.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{
    CommonArgs, Config, DEFAULT_LONG_RECORD_SECS, DEFAULT_MQ_RECHECK_SECS, engine_config_from_env,
};
use sz_combined_consumer_core::{queue_run, runtime};

mod sqs;

/// Instance/module name passed to the Senzing environment.
const INSTANCE_NAME: &str = "sz_sqs_combined_consumer";

/// Default SQS receive long-poll wait (seconds); 20 is the SQS maximum.
const DEFAULT_WAIT_TIME_SECS: i32 = 20;
/// Default SQS receive batch size; 10 is the SQS maximum.
const DEFAULT_MAX_MESSAGES: i32 = 10;
/// Default SQS visibility timeout (seconds). MUST exceed the worst-case record
/// processing time or SQS redelivers a record still being processed (duplicate
/// add). Defaults to 2x the long-record threshold.
const DEFAULT_VISIBILITY_TIMEOUT_SECS: i32 = (DEFAULT_LONG_RECORD_SECS as i32) * 2;
/// SQS's hard visibility-timeout ceiling (12 h), seconds.
const MAX_VISIBILITY_TIMEOUT_SECS: i32 = 43_200;

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

    /// Dead-letter queue URL for rejected records (bad data / retry timeout /
    /// unparseable). Default: discovered from the source queue's RedrivePolicy.
    #[arg(
        long = "dead-letter-queue-url",
        env = "SENZING_SQS_DEAD_LETTER_QUEUE_URL"
    )]
    dead_letter_queue_url: Option<String>,

    /// Start even if no dead-letter queue can be resolved. Rejected records are
    /// then DELETED (lost); only the log names them. Off by default on purpose.
    #[arg(
        long = "allow-no-dlq",
        env = "SENZING_SQS_ALLOW_NO_DLQ",
        default_value_t = false
    )]
    allow_no_dlq: bool,

    /// Extra messages to hold beyond the worker count so workers never idle on
    /// a receive round-trip (in-flight cap = threads + prefetch). Default:
    /// threads (sz_sqs_consumer-v4 parity).
    #[arg(long = "prefetch", env = "SENZING_PREFETCH")]
    prefetch: Option<usize>,

    /// Cadence of the diagnostic queue-depth probe (GetQueueAttributes
    /// ApproximateNumberOfMessages) and its drained/active transition log.
    /// NOT a correctness poll — the receive long-poll picks up a refill itself.
    #[arg(
        long = "mq-recheck-secs",
        env = "SENZING_MQ_RECHECK_SECONDS",
        default_value_t = DEFAULT_MQ_RECHECK_SECS
    )]
    mq_recheck_secs: u64,

    #[command(flatten)]
    common: CommonArgs,
}

/// SQS-specific ingestion parameters (validated), handed to the SQS run loop.
pub struct SqsParams {
    pub queue_url: String,
    pub visibility_timeout: i32,
    pub wait_time: i32,
    pub max_messages: i32,
    /// Explicit DLQ URL; `None` = discover from the source RedrivePolicy.
    pub dead_letter_queue_url: Option<String>,
    /// Run without any DLQ (rejects are deleted). Loud opt-in.
    pub allow_no_dlq: bool,
    /// In-flight overshoot beyond the worker count.
    pub prefetch: usize,
}

fn main() -> ExitCode {
    runtime::init_logging();

    // Every check (engine JSON, shared + SQS params) runs before Sz_init.
    let args = Args::parse();
    let (config, params) = match engine_config_from_env().and_then(|ec| build(&args, ec)) {
        Ok(built) => built,
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
    match params {
        Some(params) => run_sqs(config, params, env),
        None if config.input_file.is_some() => runtime::run_file_loader(&config, env),
        None => runtime::run_pure_redoer(&config, env),
    }
}

/// Resolves and validates everything before engine init: the shared config via
/// [`Config::from_common`] plus the depth-probe cadence (url/queue/prefetch
/// stay inert — SQS has no AMQP topology), and the SQS params when the SQS
/// load path will run (redo% < 100, not file mode). Pure (no engine).
fn build(args: &Args, engine_config: String) -> Result<(Config, Option<SqsParams>), String> {
    let mut config = Config::from_common(&args.common, engine_config)?;
    config.mq_recheck_secs = args.mq_recheck_secs;
    let params = if config.input_file.is_none() && config.redo_percent < 100 {
        Some(sqs_params(args, config.threads)?)
    } else {
        None
    };
    Ok((config, params))
}

/// Validates and extracts the SQS ingestion parameters (SQS load path only).
/// `threads` is the resolved worker count (default prefetch).
fn sqs_params(args: &Args, threads: usize) -> Result<SqsParams, String> {
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
    if !(0..=MAX_VISIBILITY_TIMEOUT_SECS).contains(&args.visibility_timeout) {
        return Err(format!(
            "--visibility-timeout must be 0..={MAX_VISIBILITY_TIMEOUT_SECS} (SQS 12 h max), got {}",
            args.visibility_timeout
        ));
    }
    if i64::from(args.visibility_timeout) <= args.common.long_record as i64 {
        eprintln!(
            "warning: --visibility-timeout ({}) <= --long-record ({}); SQS may redeliver a \
             record still being processed. Set it above the worst-case processing time.",
            args.visibility_timeout, args.common.long_record
        );
    }
    Ok(SqsParams {
        queue_url,
        visibility_timeout: args.visibility_timeout,
        wait_time: args.wait_time,
        max_messages: args.max_messages,
        dead_letter_queue_url: args.dead_letter_queue_url.clone().filter(|s| !s.is_empty()),
        allow_no_dlq: args.allow_no_dlq,
        prefetch: args.prefetch.unwrap_or(threads),
    })
}

/// redo% < 100: build a tokio runtime for the SQS I/O layer only (engine calls
/// run on the dedicated `sz-worker` OS threads, never on tokio workers) and run
/// the SQS ingestion loop, then apply the shared use-after-free exit discipline.
fn run_sqs(config: Config, params: SqsParams, env: Arc<SzEnvironmentCore>) -> ! {
    queue_run::run_queue_mode(
        sqs::run(&config, &params, env),
        &queue_run::ExitLogs {
            leak: "skipping Senzing environment destroy: a worker may still be in an \
                   engine call (leak-on-exit); forcing process exit",
            run_failed: "SQS run() failed; leak-on-exit, forcing process exit",
        },
    )
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

    /// Runs the full pre-init [`build`] (shared + SQS validation) and returns
    /// the SQS params, which exist on the SQS load path (redo% < 100, no file).
    fn sqs_params(a: &Args) -> Result<SqsParams, String> {
        build(a, "{}".into()).map(|(_, p)| p.expect("SQS load path builds params"))
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
    fn sqs_params_dlq_override_and_allow_no_dlq_flags() {
        let p = sqs_params(&args(&["--queue-url", "u"])).unwrap();
        assert_eq!(
            p.dead_letter_queue_url, None,
            "default = discover from RedrivePolicy"
        );
        assert!(
            !p.allow_no_dlq,
            "running without a DLQ must be an explicit opt-in"
        );

        let p = sqs_params(&args(&[
            "--queue-url",
            "u",
            "--dead-letter-queue-url",
            "https://sqs.example/dlq",
            "--allow-no-dlq",
        ]))
        .unwrap();
        assert_eq!(
            p.dead_letter_queue_url.as_deref(),
            Some("https://sqs.example/dlq")
        );
        assert!(p.allow_no_dlq);
    }

    #[test]
    fn sqs_params_prefetch_defaults_to_thread_count() {
        let p = sqs_params(&args(&["--queue-url", "u", "--threads-per-process", "7"])).unwrap();
        assert_eq!(
            p.prefetch, 7,
            "v4 parity: in-flight cap = 2 x threads by default"
        );
        let p = sqs_params(&args(&["--queue-url", "u", "--prefetch", "0"])).unwrap();
        assert_eq!(p.prefetch, 0);
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

    #[test]
    fn sqs_params_rejects_out_of_range_visibility_timeout() {
        let err = sqs_params(&args(&[
            "--queue-url",
            "u",
            "--visibility-timeout",
            "43201",
        ]))
        .err()
        .expect("above the SQS 12 h max must fail");
        assert!(err.contains("--visibility-timeout"), "{err}");
        assert!(sqs_params(&args(&["--queue-url", "u", "--visibility-timeout=-1"])).is_err());
        assert!(
            sqs_params(&args(&[
                "--queue-url",
                "u",
                "--visibility-timeout",
                "43200"
            ]))
            .is_ok()
        );
        // 0 is legal for SQS (only warned about: <= --long-record).
        assert!(sqs_params(&args(&["--queue-url", "u", "--visibility-timeout", "0"])).is_ok());
    }

    #[test]
    fn build_validates_sqs_params_before_engine_init() {
        // build() is the whole pre-Sz_init gate and needs no engine: a bad SQS
        // value fails here, not after init_environment.
        let mut a = args(&["--max-messages", "0"]);
        a.queue_url = Some("u".into());
        let err = build(&a, "{}".into())
            .err()
            .expect("bad SQS value must fail in build");
        assert!(err.contains("--max-messages"), "{err}");
    }

    #[test]
    fn build_skips_sqs_params_off_the_sqs_load_path() {
        // File mode and the pure redoer need no queue URL and build no params.
        for extra in [&["--file", "in.jsonl"][..], &["--redo-percent", "100"][..]] {
            let mut a = args(extra);
            a.queue_url = None;
            let (_, p) = build(&a, "{}".into()).expect("valid without a queue URL");
            assert!(p.is_none());
        }
    }

    #[test]
    fn build_leaves_amqp_fields_inert() {
        let (c, _) = build(&args(&["--queue-url", "u"]), "{}".into()).unwrap();
        assert_eq!((c.url, c.queue), (None, None));
        assert_eq!(c.prefetch, 0);
    }

    #[test]
    fn build_sets_mq_recheck_secs() {
        let (c, _) = build(&args(&["--queue-url", "u"]), "{}".into()).unwrap();
        assert_eq!(c.mq_recheck_secs, DEFAULT_MQ_RECHECK_SECS);
        let (c, _) = build(
            &args(&["--queue-url", "u", "--mq-recheck-secs", "1"]),
            "{}".into(),
        )
        .unwrap();
        assert_eq!(c.mq_recheck_secs, 1);
    }
}
