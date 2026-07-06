//! Configuration: CLI arguments (clap derive) with environment-variable
//! fallbacks.
//!
//! Priority: CLI argument > environment variable > default. Environment
//! variable names are kept verbatim-compatible with the sibling drivers
//! (`sz_rabbit_consumer_rust` / `sz_simple_redoer_rust`) so existing compose
//! files need minimal changes.

use clap::Parser;

/// Default long-record threshold, in seconds (matches both sibling drivers).
pub const DEFAULT_LONG_RECORD_SECS: u64 = 300;

/// Default redo share of worker capacity, in percent (Master 2026-07-06:
/// "20% is fine" — supersedes the design doc's earlier 10; at the default 12
/// threads this yields |B| = 2 ≈ 16.7%, the FAQ's 100M-arm floor).
pub const DEFAULT_REDO_PERCENT: u8 = 20;

/// Default worker-pool size. 12 is the FAQ-proven sweet spot on this fleet
/// (below the unixODBC driver-manager convoy knee); scale by adding processes,
/// never threads. `0` retains the sibling drivers' num_cpus fallback for
/// compatibility, but is a known foot-gun at 96+ cores.
pub const DEFAULT_THREADS: usize = 12;

/// Default cadence of the diagnostic passive-declare MQ depth probe, seconds.
pub const DEFAULT_MQ_RECHECK_SECS: u64 = 30;

/// Default fetcher pause when `get_redo_record()` returns empty, seconds
/// (redoer-compatible name/default).
pub const DEFAULT_REDO_SLEEP_SECS: u64 = 60;

/// Combined RabbitMQ load + redo Senzing driver.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_rabbit_combined_consumer",
    version,
    about = "Combined Senzing driver: add_record from RabbitMQ and process_redo_record, \
             split by one redo%% knob (0 = pure loader, 100 = pure redoer)",
    long_about = None
)]
pub struct Args {
    /// RabbitMQ server URL (required when redo% < 100).
    #[arg(short = 'u', long = "url", env = "SENZING_AMQP_URL")]
    pub url: Option<String>,

    /// Source queue name (required when redo% < 100).
    #[arg(short = 'q', long = "queue", env = "SENZING_RABBITMQ_QUEUE")]
    pub queue: Option<String>,

    /// Share (%) of worker capacity preferring redo, in [0, 100].
    #[arg(
        long = "redo-percent",
        env = "SENZING_REDO_PERCENT",
        default_value_t = DEFAULT_REDO_PERCENT
    )]
    pub redo_percent: u8,

    /// Worker thread count. 0 auto-detects via available CPUs (compat).
    #[arg(
        long = "threads-per-process",
        env = "SENZING_THREADS_PER_PROCESS",
        default_value_t = DEFAULT_THREADS
    )]
    pub threads_per_process: usize,

    /// AMQP basic_qos prefetch. Defaults to threads + 2: the +2 overshoot keeps
    /// a standing load_ch buffer that masks the ack round-trip, so the workers'
    /// non-blocking dispatch never stalls per-record (design §1.2/§2.3).
    #[arg(long = "prefetch", env = "SENZING_PREFETCH")]
    pub prefetch: Option<u16>,

    /// Cadence of the diagnostic MQ depth probe (passive queue_declare) and
    /// mode-transition log hysteresis. NOT a correctness poll — the consumer
    /// subscription is push-based and detects MQ refill instantly.
    #[arg(
        long = "mq-recheck-secs",
        env = "SENZING_MQ_RECHECK_SECONDS",
        default_value_t = DEFAULT_MQ_RECHECK_SECS
    )]
    pub mq_recheck_secs: u64,

    /// Seconds the redo fetcher pauses when no redo records are available.
    #[arg(
        long = "redo-sleep-secs",
        env = "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        default_value_t = DEFAULT_REDO_SLEEP_SECS
    )]
    pub redo_sleep_secs: u64,

    /// Seconds before a record is considered long-running; stats cadence is
    /// long_record / 2 (both sibling drivers' semantics).
    #[arg(long = "long-record", env = "LONG_RECORD", default_value_t = DEFAULT_LONG_RECORD_SECS)]
    pub long_record: u64,

    /// Print the WithInfo response for each processed record. Inert at the
    /// engine level in this SDK (the WithInfo helper is always called);
    /// print-gating only, matching both sibling drivers.
    #[arg(short = 'i', long = "info", default_value_t = false)]
    pub info: bool,

    /// Output Senzing engine debug trace information (verbose logging).
    #[arg(short = 't', long = "debugTrace", default_value_t = false)]
    pub debug_trace: bool,
}

/// Fully resolved runtime configuration after applying defaults and validating
/// required values.
#[derive(Debug, Clone)]
pub struct Config {
    pub engine_config: String,
    /// `Some` iff redo% < 100 (validated).
    pub url: Option<String>,
    /// `Some` iff redo% < 100 (validated).
    pub queue: Option<String>,
    pub redo_percent: u8,
    pub threads: usize,
    pub prefetch: u16,
    pub mq_recheck_secs: u64,
    pub redo_sleep_secs: u64,
    pub long_record_secs: u64,
    pub info: bool,
    pub debug_trace: bool,
}

impl Config {
    /// Resolves the configuration from parsed [`Args`] plus the environment.
    ///
    /// Returns an error message string for any missing/invalid value so the
    /// caller can print it and exit non-zero (loud failure).
    pub fn resolve(args: Args) -> Result<Self, String> {
        let engine_config = std::env::var("SENZING_ENGINE_CONFIGURATION_JSON")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                concat!(
                    "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set ",
                    "with a proper JSON configuration.\n",
                    "Please see https://senzing.zendesk.com/hc/en-us/articles/",
                    "360038774134-G2Module-Configuration-and-the-Senzing-API"
                )
                .to_string()
            })?;

        // Validate the engine config as JSON at startup (both sibling drivers'
        // behavior; a malformed blob otherwise fails deep inside Sz_init).
        if serde_json::from_str::<serde_json::Value>(&engine_config).is_err() {
            return Err("SENZING_ENGINE_CONFIGURATION_JSON is not valid JSON".to_string());
        }

        if args.redo_percent > 100 {
            return Err(format!(
                "SENZING_REDO_PERCENT must be within [0, 100], got {}",
                args.redo_percent
            ));
        }

        let threads = if args.threads_per_process == 0 {
            num_cpus::get()
        } else {
            args.threads_per_process
        };

        let url = args.url.filter(|s| !s.is_empty());
        let queue = args.queue.filter(|s| !s.is_empty());
        validate_topology(threads, args.redo_percent, url.as_deref(), queue.as_deref())?;

        let prefetch = args
            .prefetch
            .unwrap_or_else(|| u16::try_from(threads.saturating_add(2)).unwrap_or(u16::MAX));

        Ok(Self {
            engine_config,
            url,
            queue,
            redo_percent: args.redo_percent,
            threads,
            prefetch,
            mq_recheck_secs: args.mq_recheck_secs,
            redo_sleep_secs: args.redo_sleep_secs,
            long_record_secs: args.long_record,
            info: args.info,
            debug_trace: args.debug_trace,
        })
    }

    /// Number of redo-preferring workers (|B|) for this configuration.
    pub fn redo_pref_workers(&self) -> usize {
        redo_preferring_count(self.threads, self.redo_percent)
    }
}

/// Validates the (threads, redo%, AMQP) topology (design §5).
///
/// * redo% < 100 requires a RabbitMQ URL and queue.
/// * 0 < redo% < 100 requires at least 2 workers: a single worker cannot host
///   both a load-preferring and a redo-preferring class, and the |B| clamp
///   `clamp(round(N·redo%/100), 1, N−1)` is ill-defined at N = 1.
pub fn validate_topology(
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
    if redo_percent > 0 && redo_percent < 100 && threads < 2 {
        return Err(format!(
            "0 < redo% < 100 requires at least 2 worker threads (got {threads}): \
             one worker cannot host both preference classes"
        ));
    }
    Ok(())
}

/// |B|: how many of `threads` workers are redo-preferring (design §1.2).
///
/// `|B| = clamp(round(N × redo% / 100), 1, N−1)` for interior redo%; 0 at
/// redo% = 0 (the redo channel never exists); N at redo% = 100 (the load
/// channel never exists). Interior values assume `threads >= 2` (enforced by
/// [`validate_topology`]).
pub fn redo_preferring_count(threads: usize, redo_percent: u8) -> usize {
    match redo_percent {
        0 => 0,
        100 => threads,
        pct => {
            let raw = ((threads as f64) * f64::from(pct) / 100.0).round() as usize;
            raw.clamp(1, threads.saturating_sub(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redo_pref_count_endpoints() {
        assert_eq!(redo_preferring_count(12, 0), 0);
        assert_eq!(redo_preferring_count(12, 100), 12);
        assert_eq!(redo_preferring_count(1, 0), 0);
        assert_eq!(redo_preferring_count(1, 100), 1);
    }

    #[test]
    fn redo_pref_count_rounds_and_clamps() {
        assert_eq!(redo_preferring_count(12, 20), 2); // 2.4 -> 2 (compiled default)
        assert_eq!(redo_preferring_count(12, 17), 2); // 2.04 -> 2 (100M-arm floor)
        assert_eq!(redo_preferring_count(12, 10), 1); // 1.2 -> 1
        assert_eq!(redo_preferring_count(12, 1), 1); // 0.12 -> 0 -> clamp to 1
        assert_eq!(redo_preferring_count(12, 99), 11); // 11.88 -> 12 -> clamp to N-1
        assert_eq!(redo_preferring_count(2, 50), 1);
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
}
