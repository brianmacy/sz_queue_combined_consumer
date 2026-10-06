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
use percent_encoding::percent_decode_str;
use sz_rust_sdk::prelude::*;
use url::Url;

use sz_combined_consumer_core::config::{
    CommonArgs, Config, DEFAULT_MQ_RECHECK_SECS, engine_config_from_env, resolve_prefetch,
};
use sz_combined_consumer_core::{queue_run, runtime};

mod activemq;

/// Instance/module name passed to the Senzing environment; also the AMQP
/// container id and receiver link name.
pub(crate) const INSTANCE_NAME: &str = "sz_activemq_combined_consumer";

/// Separator of an Artemis fully qualified queue name (`address::queue`).
const FQQN_SEPARATOR: &str = "::";

#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_activemq_combined_consumer",
    version,
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

/// SASL PLAIN credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    /// Never prints the password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Credentials {{ user: {:?}, password: *** }}", self.user)
    }
}

/// Artemis ingestion parameters (validated), handed to the transport.
#[derive(Debug, Clone)]
pub struct ActiveMqParams {
    /// Broker URL with any embedded credentials removed.
    pub url: Url,
    /// `None` = no SASL PLAIN (anonymous).
    pub credentials: Option<Credentials>,
    /// Queue name or FQQN, used as the receiver source address.
    pub queue: String,
}

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

/// Parses an `amqp://` / `amqps://` URL and resolves the SASL credentials:
/// `user` / `password` (env/flag) each override the URL's (percent-decoded)
/// userinfo. Returns the URL stripped of credentials (so it is safe to log).
fn parse_broker_url(
    raw: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> Result<(Url, Option<Credentials>), String> {
    let mut url = Url::parse(raw).map_err(|e| format!("invalid ActiveMQ URL: {e}"))?;
    if !matches!(url.scheme(), "amqp" | "amqps") {
        return Err(format!(
            "ActiveMQ URL must use amqp:// or amqps://, got {}://",
            url.scheme()
        ));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err("ActiveMQ URL has no host".to_string());
    }
    let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
    let url_user = Some(decode(url.username())).filter(|s| !s.is_empty());
    let url_password = url.password().map(decode);
    // Strip userinfo: credentials travel only in the SASL profile.
    let _ = url.set_username("");
    let _ = url.set_password(None);

    let user = user.map(str::to_string).or(url_user);
    let password = password.map(str::to_string).or(url_password);
    let credentials = match (user.filter(|u| !u.is_empty()), password) {
        (Some(user), password) => Some(Credentials {
            user,
            password: password.unwrap_or_default(),
        }),
        (None, Some(_)) => {
            return Err(
                "ActiveMQ password given without a user (set SENZING_ACTIVEMQ_USER or put \
                 user:password in the URL)"
                    .to_string(),
            );
        }
        (None, None) => None,
    };
    Ok((url, credentials))
}

/// Accepts a plain queue name or an Artemis FQQN (`address::queue`, both
/// parts non-empty).
fn validate_queue(queue: &str) -> Result<String, String> {
    if let Some((address, name)) = queue.split_once(FQQN_SEPARATOR)
        && (address.is_empty() || name.is_empty() || name.contains(FQQN_SEPARATOR))
    {
        return Err(format!(
            "invalid fully qualified queue name {queue:?}: expected address::queue"
        ));
    }
    Ok(queue.to_string())
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
    fn url_credentials_are_decoded_and_stripped() {
        let (url, c) = parse_broker_url("amqp://us%40er:p%3Ass@host:5673", None, None).unwrap();
        assert_eq!(url.as_str(), "amqp://host:5673");
        let c = c.expect("credentials from URL");
        assert_eq!((c.user.as_str(), c.password.as_str()), ("us@er", "p:ss"));
    }

    #[test]
    fn user_and_password_override_url_userinfo() {
        let (_, c) = parse_broker_url("amqp://a:b@h", Some("u"), None).unwrap();
        assert_eq!(
            c,
            Some(Credentials {
                user: "u".into(),
                password: "b".into()
            })
        );
        let (_, c) = parse_broker_url("amqps://a:b@h", None, Some("p")).unwrap();
        assert_eq!(c.map(|c| c.password), Some("p".into()));
        let (_, c) = parse_broker_url("amqp://h", Some("u"), Some("p")).unwrap();
        assert_eq!(c.map(|c| c.user), Some("u".into()));
    }

    #[test]
    fn anonymous_without_any_credentials() {
        let (url, c) = parse_broker_url("amqp://localhost:5672", None, None).unwrap();
        assert_eq!(
            (url.host_str(), url.port(), c),
            (Some("localhost"), Some(5672), None)
        );
    }

    #[test]
    fn password_without_user_is_an_error() {
        let err = parse_broker_url("amqp://h", None, Some("p")).unwrap_err();
        assert!(err.contains("without a user"), "{err}");
        assert!(parse_broker_url("amqp://:p@h", None, None).is_err());
    }

    #[test]
    fn url_scheme_and_host_are_validated() {
        assert!(
            parse_broker_url("http://h", None, None)
                .unwrap_err()
                .contains("amqp://")
        );
        assert!(parse_broker_url("not a url", None, None).is_err());
        assert!(parse_broker_url("amqp://", None, None).is_err());
    }

    #[test]
    fn credentials_debug_hides_the_password() {
        let c = Credentials {
            user: "u".into(),
            password: "secret".into(),
        };
        assert!(!format!("{c:?}").contains("secret"));
    }

    #[test]
    fn queue_accepts_plain_and_fqqn() {
        assert_eq!(validate_queue("q").unwrap(), "q");
        assert_eq!(validate_queue("addr::q").unwrap(), "addr::q");
        for bad in ["::q", "addr::", "a::b::c"] {
            assert!(validate_queue(bad).is_err(), "{bad}");
        }
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
