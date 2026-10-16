//! Entry point for the combined Apache ActiveMQ Artemis load + redo Senzing
//! driver.
//!
//! The Artemis analogue of `sz_rabbit_combined_consumer`: it ingests records
//! from an Artemis ANYCAST queue over AMQP 1.0 (`fe2o3-amqp`), sharing the
//! entire engine-processing core (worker pool, redo fetcher, stats, live config
//! reload, file loader) via `sz_combined_consumer_core`. Neither `lapin` nor
//! the AWS SDK is compiled into this binary.
//!
//! Modes:
//! * `--file` — file loader + redo share (shared core path; no AMQP, no tokio).
//! * redo% = 100 — pure `std::thread` redoer (shared core path; no AMQP).
//! * redo% < 100 — the shared core queue loop with this bin's
//!   `ActiveMqTransport`.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_activemq_amqp::{BrokerParams, parse_broker_url, validate_queue};
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{
    CommonArgs, Config, DEFAULT_MQ_RECHECK_SECS, engine_config_from_env, resolve_prefetch,
};
use sz_combined_consumer_core::{queue_run, runtime};

mod activemq;

/// Instance/module name passed to the Senzing environment; also the AMQP
/// container id and receiver link name.
pub(crate) const INSTANCE_NAME: &str = "sz_activemq_combined_consumer";

#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_activemq_combined_consumer",
    version = sz_combined_consumer_core::VERSION,
    about = "Combined Senzing driver (Apache ActiveMQ Artemis, AMQP 1.0): add_record from an \
             Artemis queue and process_redo_record, split by one redo%% knob (0 = pure loader, \
             100 = pure redoer)",
    long_about = None
)]
struct Args {
    /// Broker URL, `amqp://` or `amqps://` (required when redo% < 100 and
    /// --file is not set). Credentials may be embedded
    /// (`amqp://user:pass@host:5672`, percent-encoded).
    #[arg(short = 'u', long = "url", env = "SENZING_ACTIVEMQ_URL")]
    url: Option<String>,

    /// SASL PLAIN user name; overrides one embedded in the URL.
    #[arg(long = "user", env = "SENZING_ACTIVEMQ_USER")]
    user: Option<String>,

    /// SASL PLAIN password; overrides one embedded in the URL.
    #[arg(
        long = "password",
        env = "SENZING_ACTIVEMQ_PASSWORD",
        hide_env_values = true
    )]
    password: Option<String>,

    /// Source queue (ANYCAST), or a fully qualified `address::queue` name
    /// (required when redo% < 100 and --file is not set).
    #[arg(short = 'q', long = "queue", env = "SENZING_ACTIVEMQ_QUEUE")]
    queue: Option<String>,

    /// Total in-flight cap: deliveries received but not yet settled (the
    /// AMQP link credit is kept at this minus the unsettled count). Defaults to
    /// threads + 2 (RabbitMQ parity: the +2 masks the settle round-trip).
    /// Below threads is raised to threads (with a warning).
    #[arg(long = "prefetch", env = "SENZING_PREFETCH")]
    prefetch: Option<usize>,

    /// Cadence of the diagnostic MQ depth probe. Artemis exposes no queue
    /// depth over AMQP, so the depth is reported as unknown; kept for flag
    /// parity with the other backends.
    #[arg(
        long = "mq-recheck-secs",
        env = "SENZING_MQ_RECHECK_SECONDS",
        default_value_t = DEFAULT_MQ_RECHECK_SECS
    )]
    mq_recheck_secs: u64,

    #[command(flatten)]
    common: CommonArgs,
}

/// Artemis ingestion parameters (validated), handed to the transport.
pub type ActiveMqParams = BrokerParams;

fn main() -> ExitCode {
    runtime::init_logging();

    // Every check (engine JSON, shared + broker params) runs before Sz_init.
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

    match params {
        Some(params) => run_activemq(config, params, env),
        None if config.input_file.is_some() => runtime::run_file_loader(&config, env),
        None => runtime::run_pure_redoer(&config, env),
    }
}

/// Resolves and validates everything before engine init: the shared config,
/// the depth-probe cadence, the in-flight cap and — only on the Artemis load
/// path (redo% < 100, not file mode) — the broker params. Pure (no engine).
fn build(args: &Args, engine_config: String) -> Result<(Config, Option<ActiveMqParams>), String> {
    let mut config = Config::from_common(&args.common, engine_config)?;
    config.mq_recheck_secs = args.mq_recheck_secs;
    config.prefetch = resolve_prefetch(
        args.prefetch,
        config.threads,
        config.threads.saturating_add(2),
    );
    let load_path = config.input_file.is_none() && config.redo_percent < 100;
    if !load_path {
        if config.input_file.is_some() && (args.url.is_some() || args.queue.is_some()) {
            eprintln!("warning: --file is set; ignoring --url/--queue (file input mode)");
        }
        return Ok((config, None));
    }
    let url = args.url.as_deref().filter(|s| !s.is_empty()).ok_or(
        "No ActiveMQ URL provided (use --url or SENZING_ACTIVEMQ_URL); required when redo% < 100",
    )?;
    let queue = args.queue.as_deref().filter(|s| !s.is_empty()).ok_or(
        "No queue provided (use --queue or SENZING_ACTIVEMQ_QUEUE); required when redo% < 100",
    )?;
    let (url, credentials) = parse_broker_url(url, args.user.as_deref(), args.password.as_deref())?;
    Ok((
        config,
        Some(ActiveMqParams {
            url,
            credentials,
            queue: validate_queue(queue)?,
        }),
    ))
}

/// redo% < 100: tokio runtime for the AMQP I/O layer only (engine calls run on
/// the dedicated `sz-worker` OS threads), then the shared use-after-free exit
/// discipline.
fn run_activemq(config: Config, params: ActiveMqParams, env: Arc<SzEnvironmentCore>) -> ! {
    queue_run::run_queue_mode(
        activemq::run(&config, params, env),
        &queue_run::ExitLogs {
            leak: "skipping Senzing environment destroy: a worker may still be in an \
                   engine call (leak-on-exit); forcing process exit",
            run_failed: "ActiveMQ run() failed; leak-on-exit, forcing process exit",
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses the binary's args from a CLI vector (clap); a broker field not
    /// given on that vector is cleared so an inherited SENZING_ACTIVEMQ_* env
    /// cannot leak in.
    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["sz_activemq_combined_consumer"];
        argv.extend_from_slice(extra);
        let mut a = Args::parse_from(argv);
        let given = |flag: &str| extra.contains(&flag);
        if !given("-u") {
            a.url = None;
        }
        if !given("-q") {
            a.queue = None;
        }
        (a.user, a.password) = (None, None);
        a
    }

    fn params(extra: &[&str]) -> Result<ActiveMqParams, String> {
        build(&args(extra), "{}".into()).map(|(_, p)| p.expect("load path builds params"))
    }

    #[test]
    fn build_requires_url_and_queue_on_the_load_path() {
        let err = params(&["-q", "q"]).unwrap_err();
        assert!(err.contains("ActiveMQ URL"), "{err}");
        let err = params(&["-u", "amqp://h"]).unwrap_err();
        assert!(err.contains("No queue"), "{err}");
        let p = params(&["-u", "amqp://u:p@h", "-q", "a::q"]).unwrap();
        assert_eq!((p.url.as_str(), p.queue.as_str()), ("amqp://h", "a::q"));
    }

    #[test]
    fn build_validates_broker_params_before_engine_init() {
        let err = params(&["-u", "http://h", "-q", "q"]).unwrap_err();
        assert!(err.contains("amqp://"), "{err}");
    }

    #[test]
    fn build_skips_broker_params_off_the_load_path() {
        for extra in [&["--file", "in.jsonl"][..], &["--redo-percent", "100"][..]] {
            let (_, p) = build(&args(extra), "{}".into()).expect("valid without a URL");
            assert!(p.is_none());
        }
    }

    #[test]
    fn prefetch_is_the_total_cap_defaulting_to_threads_plus_two() {
        let prefetch = |extra: &[&str]| {
            let mut a = vec!["-u", "amqp://h", "-q", "q", "--threads-per-process", "4"];
            a.extend_from_slice(extra);
            build(&args(&a), "{}".into()).expect("valid").0.prefetch
        };
        assert_eq!(prefetch(&[]), 6, "threads + 2");
        assert_eq!(prefetch(&["--prefetch", "9"]), 9, "the total, not extra");
        assert_eq!(prefetch(&["--prefetch", "2"]), 4, "raised to threads");
    }

    #[test]
    fn build_sets_mq_recheck_secs_and_leaves_rabbit_fields_inert() {
        let (c, _) = build(
            &args(&["-u", "amqp://h", "-q", "q", "--mq-recheck-secs", "1"]),
            "{}".into(),
        )
        .unwrap();
        assert_eq!(c.mq_recheck_secs, 1);
        assert_eq!((c.url, c.queue), (None, None));
    }
}
