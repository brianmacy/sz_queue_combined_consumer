//! Entry point for the combined RabbitMQ load + redo Senzing driver.
//!
//! Branches on the `redo%` endpoints per the design (§1.1):
//! * `--file` — file loader + redo share (no AMQP, no tokio).
//! * redo% = 100 — the tokio runtime is never BUILT and no AMQP connection is
//!   opened; the pure `std::thread` redoer path runs instead.
//! * redo% < 100 — the consumer-shaped tokio/lapin path runs (which itself
//!   skips all redo machinery at redo% = 0).
//!
//! Shared bring-up / shutdown / redoer / file-loader logic lives in
//! `sz_combined_consumer_core::runtime`, the queue-mode loop in
//! `sz_combined_consumer_core::queue_loop`; only the AMQP transport is local.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{
    CommonArgs, Config, DEFAULT_MQ_RECHECK_SECS, engine_config_from_env, resolve_prefetch,
    validate_split_threads,
};
use sz_combined_consumer_core::{queue_run, runtime};

/// RabbitMQ transport (AMQP-specific; lives in this bin so `lapin` never
/// compiles into the SQS binary).
mod combined;

/// Instance/module name passed to the Senzing environment; also the AMQP
/// consumer tag.
pub(crate) const INSTANCE_NAME: &str = "sz_rabbit_combined_consumer";

/// Combined RabbitMQ load + redo Senzing driver.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_rabbit_combined_consumer",
    version,
    about = "Combined Senzing driver: add_record from RabbitMQ and process_redo_record, \
             split by one redo%% knob (0 = pure loader, 100 = pure redoer)",
    long_about = None
)]
struct Args {
    /// RabbitMQ server URL (required when redo% < 100).
    #[arg(short = 'u', long = "url", env = "SENZING_AMQP_URL")]
    url: Option<String>,

    /// Source queue name (required when redo% < 100).
    #[arg(short = 'q', long = "queue", env = "SENZING_RABBITMQ_QUEUE")]
    queue: Option<String>,

    /// Total in-flight cap: deliveries received but not yet settled (AMQP
    /// basic_qos prefetch). Defaults to threads + 2: the +2 overshoot keeps a
    /// standing load_ch buffer that masks the ack round-trip, so the workers'
    /// non-blocking dispatch never stalls per-record (design §1.2/§2.3).
    /// Below threads is raised to threads (with a warning).
    #[arg(long = "prefetch", env = "SENZING_PREFETCH")]
    prefetch: Option<usize>,

    /// Cadence of the diagnostic MQ depth probe (passive queue_declare) and
    /// mode-transition log hysteresis. NOT a correctness poll — the consumer
    /// subscription is push-based and detects MQ refill instantly.
    #[arg(
        long = "mq-recheck-secs",
        env = "SENZING_MQ_RECHECK_SECONDS",
        default_value_t = DEFAULT_MQ_RECHECK_SECS
    )]
    mq_recheck_secs: u64,

    #[command(flatten)]
    common: CommonArgs,
}

fn main() -> ExitCode {
    runtime::init_logging();

    // Every check (engine JSON, shared + AMQP topology) runs before Sz_init.
    let args = Args::parse();
    let config = match engine_config_from_env().and_then(|ec| resolve(args, ec)) {
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

/// Resolves the shared config via [`Config::from_common`], then the AMQP
/// fields: file mode ignores `--url`/`--queue` (with a warning); queue mode
/// validates the topology. Pure (no engine), so it is unit-testable.
fn resolve(args: Args, engine_config: String) -> Result<Config, String> {
    let mut config = Config::from_common(&args.common, engine_config)?;
    let url = args.url.filter(|s| !s.is_empty());
    let queue = args.queue.filter(|s| !s.is_empty());

    if config.input_file.is_some() {
        if url.is_some() || queue.is_some() {
            eprintln!("warning: --file is set; ignoring --url/--queue (file input mode)");
        }
    } else {
        validate_topology(
            config.threads,
            config.redo_percent,
            url.as_deref(),
            queue.as_deref(),
        )?;
        config.url = url;
        config.queue = queue;
    }

    config.prefetch = resolve_prefetch(
        args.prefetch,
        config.threads,
        config.threads.saturating_add(2),
    );
    config.mq_recheck_secs = args.mq_recheck_secs;
    Ok(config)
}

/// Validates the (threads, redo%, AMQP) topology (design §5).
///
/// * redo% < 100 requires a RabbitMQ URL and queue.
/// * 0 < redo% < 100 requires at least 2 workers: a single worker cannot host
///   both a load-preferring and a redo-preferring class, and the |B| clamp
///   `clamp(round(N·redo%/100), 1, N−1)` is ill-defined at N = 1.
fn validate_topology(
    threads: usize,
    redo_percent: u8,
    url: Option<&str>,
    queue: Option<&str>,
) -> Result<(), String> {
    if redo_percent < 100 {
        if url.is_none_or(str::is_empty) {
            return Err("No RabbitMQ URL provided (use --url or SENZING_AMQP_URL); \
                 required when redo% < 100"
                .to_string());
        }
        if queue.is_none_or(str::is_empty) {
            return Err(
                "No queue provided (use --queue or SENZING_RABBITMQ_QUEUE); \
                 required when redo% < 100"
                    .to_string(),
            );
        }
    }
    validate_split_threads(threads, redo_percent)
}

/// redo% < 100: tokio runtime for the AMQP I/O layer only (worker_threads
/// pinned to 2 — see [`queue_run::run_queue_mode`]), then the shared
/// use-after-free exit discipline.
fn run_combined(config: Config, env: Arc<SzEnvironmentCore>) -> ! {
    queue_run::run_queue_mode(
        combined::run(config, env),
        &queue_run::ExitLogs {
            leak: "skipping Senzing environment destroy: a worker may still be in an \
                   engine call (leak-on-exit to avoid use-after-free); forcing process exit",
            run_failed: "run() failed; skipping native teardown (leak-on-exit), forcing exit",
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["sz_rabbit_combined_consumer"];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    #[test]
    fn topology_requires_amqp_below_100() {
        assert!(validate_topology(12, 0, None, None).is_err());
        assert!(validate_topology(12, 50, None, Some("q")).is_err());
        assert!(validate_topology(12, 50, Some("amqp://x"), None).is_err());
        assert!(validate_topology(12, 50, Some(""), Some("q")).is_err());
        assert!(validate_topology(12, 50, Some("amqp://x"), Some("q")).is_ok());
        // Pure redoer runs with the AMQP settings entirely unset (design §4).
        assert!(validate_topology(12, 100, None, None).is_ok());
    }

    #[test]
    fn topology_requires_two_threads_for_interior_percent() {
        assert!(validate_topology(1, 50, Some("amqp://x"), Some("q")).is_err());
        assert!(validate_topology(2, 50, Some("amqp://x"), Some("q")).is_ok());
        // N = 1 is valid at both endpoints.
        assert!(validate_topology(1, 0, Some("amqp://x"), Some("q")).is_ok());
        assert!(validate_topology(1, 100, None, None).is_ok());
    }

    #[test]
    fn resolve_rejects_missing_amqp_before_engine_init() {
        // resolve() is the whole pre-Sz_init gate; it needs no engine. Clear the
        // AMQP fields explicitly so an inherited SENZING_AMQP_URL cannot mask it.
        let mut a = args(&[]);
        (a.url, a.queue) = (None, None);
        let err = resolve(a, "{}".into()).expect_err("no URL at redo% < 100");
        assert!(err.contains("RabbitMQ URL"), "{err}");
    }

    #[test]
    fn resolve_queue_mode_sets_amqp_fields_and_default_prefetch() {
        let c = resolve(
            args(&["-u", "amqp://x", "-q", "q", "--threads-per-process", "4"]),
            "{}".into(),
        )
        .expect("valid queue-mode config");
        assert_eq!(c.url.as_deref(), Some("amqp://x"));
        assert_eq!(c.queue.as_deref(), Some("q"));
        assert_eq!(c.prefetch, 6, "threads + 2");
        assert_eq!(c.mq_recheck_secs, DEFAULT_MQ_RECHECK_SECS);
    }

    #[test]
    fn resolve_file_mode_ignores_amqp_and_pure_redoer_needs_none() {
        let c = resolve(args(&["-f", "in.jsonl", "-u", "amqp://x"]), "{}".into()).unwrap();
        assert_eq!((c.url, c.queue), (None, None));
        let c = resolve(
            args(&["--redo-percent", "100", "--prefetch", "13"]),
            "{}".into(),
        )
        .unwrap();
        assert_eq!(c.prefetch, 13);
    }

    #[test]
    fn resolve_prefetch_is_total_and_clamped_up_to_threads() {
        let base = ["-u", "amqp://x", "-q", "q", "--threads-per-process", "4"];
        let with = |p: &str| {
            let mut a = base.to_vec();
            a.extend(["--prefetch", p]);
            resolve(args(&a), "{}".into()).expect("valid").prefetch
        };
        assert_eq!(with("5"), 5, "the total cap, not extra beyond threads");
        assert_eq!(with("2"), 4, "below threads is raised to threads");
        assert_eq!(with("0"), 4, "0 would mean unlimited to basic_qos");
    }
}
