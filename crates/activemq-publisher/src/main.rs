//! `sz_activemq_publisher`: bulk-loads JSONL records onto the Apache ActiveMQ
//! Artemis ANYCAST queue that `sz_activemq_combined_consumer` reads (AMQP 1.0,
//! one `Data` section per record, durable by default).
//!
//! Broker URL / credentials / queue use the consumer's exact semantics and
//! environment variables (`SENZING_ACTIVEMQ_URL` / `_USER` / `_PASSWORD` /
//! `_QUEUE`) via the shared `sz-activemq-amqp` crate. No Senzing dependency.
//!
//! Exit codes: 0 every record settled; 1 fatal error (bad arguments, input
//! I/O, broker unreachable past `--reconnect-timeout`, bad credentials);
//! 2 the run finished but some records were FAILED (never accepted).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use sz_activemq_amqp::{BrokerParams, parse_broker_url, validate_queue};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod input;
mod publish;

use publish::{FilePublisher, FileStats, Progress, PublishConfig};

/// AMQP container id / link name prefix.
const INSTANCE_NAME: &str = "sz_activemq_publisher";
/// `--version`: the release tag in release builds (`RELEASE_VERSION`, set by
/// `.github/workflows/release.yml`), else the workspace version. Same rule as
/// `sz_combined_consumer_core::VERSION`, restated because this bin must not
/// depend on the core (it links libSz).
const VERSION: &str = match option_env!("RELEASE_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};
/// Exit code when the run completed but some records were never accepted.
const EXIT_RECORDS_FAILED: u8 = 2;

#[derive(Parser, Debug)]
#[command(
    name = "sz_activemq_publisher",
    version = VERSION,
    about = "Publish JSONL records (plain, gzip, bzip2 or zstd; files or Spark part-* directories) \
             to an Apache ActiveMQ Artemis ANYCAST queue over AMQP 1.0",
    after_help = "\
Exit codes: 0 all records settled; 1 fatal error; 2 finished with FAILED records.

Summary fields:
  read        non-blank lines read
  settled     records the broker accepted
  rejected    'rejected' outcomes (retried up to --max-attempts)
  released    'released'/'modified' outcomes (retried up to --max-attempts)
  resent      records re-sent after a broker link loss (may duplicate)
  reconnects  link losses recovered
  failed      records never accepted (logged with file and line)

A send blocked on broker credit (address full, BLOCK policy) waits
indefinitely and logs 'blocked on broker credit' every
--blocked-log-interval seconds; it is never treated as a link loss."
)]
struct Args {
    /// Input files and/or directories (a directory publishes its part-* files).
    /// Compression (gzip, bzip2, zstd) is detected from the content.
    #[arg(value_name = "INPUT", required = true, num_args = 1..)]
    inputs: Vec<PathBuf>,

    /// Broker URL, `amqp://` only. Credentials may be embedded
    /// (`amqp://user:pass@host:5672`, percent-encoded).
    #[arg(short = 'u', long = "url", env = "SENZING_ACTIVEMQ_URL")]
    url: String,

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

    /// Target queue (ANYCAST), or a fully qualified `address::queue` name.
    #[arg(short = 'q', long = "queue", env = "SENZING_ACTIVEMQ_QUEUE")]
    queue: String,

    /// Max records sent but not yet settled by the broker, per file.
    #[arg(short = 'm', long = "max-pending", default_value_t = 500)]
    max_pending: usize,

    /// Log progress every N settled records (0 = off).
    #[arg(long = "report-interval", default_value_t = 100_000)]
    report_interval: u64,

    /// Delay before retrying a non-accepted record or a failed connect (seconds).
    #[arg(long = "retry-delay", default_value_t = 3)]
    retry_delay: u64,

    /// Attempts per record before it is FAILED when the broker keeps
    /// rejecting/releasing it (0 = unlimited).
    #[arg(long = "max-attempts", default_value_t = 10)]
    max_attempts: u32,

    /// Give up (exit 1) when the broker stays unreachable this long (seconds;
    /// 0 = retry forever).
    #[arg(long = "reconnect-timeout", default_value_t = 300)]
    reconnect_timeout: u64,

    /// Interval of the 'blocked on broker credit' log line (seconds).
    #[arg(long = "blocked-log-interval", default_value_t = 30)]
    blocked_log_interval: u64,

    /// Publish non-durable messages (header durable=false). Default is
    /// durable. WARNING: non-durable messages are lost if the broker restarts.
    #[arg(long = "transient")]
    transient: bool,

    /// Files published concurrently, each on its own AMQP connection.
    /// Memory is bounded by about parallel x 3 x max-pending records.
    #[arg(short = 'p', long = "parallel", default_value_t = 1)]
    parallel: usize,

    /// Refuse to start (exit 1) when the broker's advertised AMQP
    /// max-frame-size is below this (bytes; 0 = no floor, the default). A
    /// record larger than a frame is sent as several frames (fe2o3-amqp fork
    /// with per-delivery delivery-ids, see Cargo.toml [patch.crates-io]).
    #[arg(long = "min-max-frame-size", default_value_t = 0)]
    min_max_frame_size: u32,

    /// AMQP max-frame-size this client advertises (default: fe2o3's). The
    /// negotiated frame is the smaller of the two peers'.
    #[arg(long = "max-frame-size")]
    max_frame_size: Option<u32>,

    /// Records larger than this many bytes are NOT published: they go to
    /// --side-file (or are FAILED without one).
    #[arg(
        long = "side-max-bytes",
        env = "SIDE_MAX_BYTES",
        default_value_t = 209_715_200
    )]
    side_max_bytes: usize,

    /// Append oversized records here, one per line, for a later direct load
    /// (consumer file mode); each is logged `sided <file> line <n> bytes <b>`.
    #[arg(long = "side-file", env = "SIDE_FILE")]
    side_file: Option<std::path::PathBuf>,

    /// Resume file: `<file name> <records already published>` per line (the
    /// `fileprogress` lines this publisher prints). Those leading records of
    /// each named file are skipped.
    #[arg(long = "resume", env = "RESUME")]
    resume: Option<std::path::PathBuf>,

    /// Print `fileprogress <file> <n>` every this many records per file.
    #[arg(long = "progress-every", default_value_t = 100_000)]
    progress_every: u64,

    /// Debug logging (overridden by RUST_LOG).
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
}

/// Validates the arguments into the shared publish config (no I/O).
fn build_config(args: &Args) -> Result<PublishConfig, String> {
    if args.max_pending == 0 {
        return Err("--max-pending must be at least 1".into());
    }
    if args.parallel == 0 {
        return Err("--parallel must be at least 1".into());
    }
    if args.blocked_log_interval == 0 {
        return Err("--blocked-log-interval must be at least 1".into());
    }
    if args.queue.is_empty() {
        return Err("No queue provided (use --queue or SENZING_ACTIVEMQ_QUEUE)".into());
    }
    let (url, credentials) =
        parse_broker_url(&args.url, args.user.as_deref(), args.password.as_deref())?;
    if url.scheme() != "amqp" {
        return Err(format!(
            "the publisher supports amqp:// only (got {}://): it reads the broker's \
             max-frame-size from the plaintext Open frame",
            url.scheme()
        ));
    }
    let side = match &args.side_file {
        Some(p) => Some(Arc::new(std::sync::Mutex::new(std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .map_err(|e| format!("cannot open --side-file {}: {e}", p.display()))?,
        )))),
        None => None,
    };
    let resume = match &args.resume {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| format!("cannot read --resume {}: {e}", p.display()))?;
            publish::parse_resume(&text).map_err(|e| format!("--resume {}: {e:#}", p.display()))?
        }
        None => Default::default(),
    };
    Ok(PublishConfig {
        broker: BrokerParams {
            url,
            credentials,
            queue: validate_queue(&args.queue)?,
        },
        instance: INSTANCE_NAME.to_string(),
        max_pending: args.max_pending,
        retry_delay: Duration::from_secs(args.retry_delay),
        max_attempts: args.max_attempts,
        reconnect_timeout: (args.reconnect_timeout > 0)
            .then(|| Duration::from_secs(args.reconnect_timeout)),
        blocked_log_interval: Duration::from_secs(args.blocked_log_interval),
        durable: !args.transient,
        min_max_frame_size: args.min_max_frame_size,
        local_max_frame_size: args.max_frame_size,
        side_max_bytes: args.side_max_bytes,
        side,
        resume: Arc::new(resume),
        progress_every: args.progress_every,
    })
}

fn main() -> ExitCode {
    let args = Args::parse();
    let level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| level.into()))
        .with_target(false)
        .init();

    let cfg = match build_config(&args) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(1);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to start the tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(run(&args, cfg)) {
        Ok(total) if total.failed > 0 => {
            error!(
                "{} record(s) were never accepted (see FAILED lines)",
                total.failed
            );
            ExitCode::from(EXIT_RECORDS_FAILED)
        }
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e:#}");
            ExitCode::from(1)
        }
    }
}

/// Publishes every input file, `--parallel` at a time. The first fatal file
/// error aborts the others; the summary of what completed is printed either
/// way.
async fn run(args: &Args, cfg: PublishConfig) -> Result<FileStats> {
    let files = input::expand_inputs(&args.inputs)?;
    info!(
        "{} file(s) -> {} queue {} (parallel {}, max-pending {}, durable {})",
        files.len(),
        cfg.broker.url,
        cfg.broker.queue,
        args.parallel,
        cfg.max_pending,
        cfg.durable
    );
    let cfg = Arc::new(cfg);
    let progress = Arc::new(Progress::new(args.report_interval));
    let permits = Arc::new(Semaphore::new(args.parallel));
    let mut tasks = JoinSet::new();
    for (index, path) in files.iter().cloned().enumerate() {
        let (cfg, progress, permits) = (cfg.clone(), progress.clone(), permits.clone());
        tasks.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .expect("semaphore never closed");
            let result = FilePublisher::new(&cfg, &path, index, &progress)
                .run()
                .await;
            (path, result)
        });
    }

    let mut total = FileStats::default();
    let mut completed = 0usize;
    let mut first_error: Option<anyhow::Error> = None;
    while let Some(joined) = tasks.join_next().await {
        let (path, result) = match joined {
            Ok(r) => r,
            Err(e) if e.is_cancelled() => continue,
            Err(e) => {
                first_error.get_or_insert(anyhow!("publisher task panicked: {e}"));
                tasks.abort_all();
                continue;
            }
        };
        match result {
            Ok(stats) => {
                completed += 1;
                println!("file {}: {}", path.display(), stats.summary());
                total.merge(&stats);
            }
            Err(e) => {
                error!("{}: {e:#}", path.display());
                if first_error.is_none() {
                    first_error = Some(e.context(format!("publishing {}", path.display())));
                    tasks.abort_all();
                }
            }
        }
    }
    let secs = progress.elapsed().as_secs_f64();
    println!(
        "Publish summary: files={completed}/{} {} elapsed={secs:.1}s rate={:.0}/s",
        files.len(),
        total.summary(),
        total.settled as f64 / secs.max(1e-9)
    );
    if let Some(e) = first_error {
        return Err(e);
    }
    if completed != files.len() {
        bail!("only {completed} of {} files completed", files.len());
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Args {
        let mut argv = vec![
            "sz_activemq_publisher",
            "-u",
            "amqp://u:p@h:5673",
            "-q",
            "q",
        ];
        argv.extend_from_slice(extra);
        argv.push("in.jsonl");
        let mut a = Args::parse_from(argv);
        (a.user, a.password) = (None, None); // no inherited env
        a
    }

    #[test]
    fn defaults_are_durable_sequential_and_bounded() {
        let c = build_config(&args(&[])).expect("valid");
        assert!(c.durable, "durable by default");
        assert_eq!(c.max_pending, 500);
        assert_eq!(c.max_attempts, 10);
        assert_eq!(c.reconnect_timeout, Some(Duration::from_secs(300)));
        assert_eq!(
            c.broker.url.as_str(),
            "amqp://h:5673",
            "credentials stripped"
        );
        assert_eq!(c.broker.credentials.map(|c| c.user), Some("u".into()));
        assert_eq!(args(&[]).parallel, 1);
        assert_eq!(
            c.min_max_frame_size, 0,
            "multi-frame deliveries are fine with the vendored fix"
        );
        assert_eq!(c.side_max_bytes, 209_715_200);
        assert!(c.side.is_none() && c.resume.is_empty() && c.local_max_frame_size.is_none());
    }

    #[test]
    fn transient_and_unbounded_reconnect() {
        let c = build_config(&args(&["--transient", "--reconnect-timeout", "0"])).unwrap();
        assert!(!c.durable);
        assert_eq!(c.reconnect_timeout, None);
    }

    #[test]
    fn zero_bounds_and_bad_broker_params_are_rejected() {
        for bad in [
            &["--max-pending", "0"][..],
            &["--parallel", "0"][..],
            &["--blocked-log-interval", "0"][..],
        ] {
            assert!(build_config(&args(bad)).is_err(), "{bad:?}");
        }
        let mut a = args(&[]);
        a.url = "http://h".into();
        assert!(build_config(&a).unwrap_err().contains("amqp://"));
        a = args(&[]);
        a.url = "amqps://h".into();
        assert!(build_config(&a).unwrap_err().contains("amqp:// only"));
        a = args(&[]);
        a.queue = "a::".into();
        assert!(build_config(&a).is_err());
    }

    #[test]
    fn inputs_are_required() {
        assert!(
            Args::try_parse_from(["sz_activemq_publisher", "-u", "amqp://h", "-q", "q"]).is_err()
        );
    }
}
