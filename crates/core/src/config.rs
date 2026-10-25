//! Configuration: shared CLI arguments (clap derive) with environment-variable
//! fallbacks, and the resolved runtime [`Config`]. Transport-specific flags
//! live in each backend binary, which flattens [`CommonArgs`] into its own
//! `Args`.
//!
//! Priority: CLI argument > environment variable > default. Environment
//! variable names are kept verbatim-compatible with the sibling drivers
//! (`sz_rabbit_consumer_rust` / `sz_simple_redoer_rust`) so existing compose
//! files need minimal changes.

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

/// Default cadence of the diagnostic MQ depth probe (`--mq-recheck-secs`),
/// seconds. Each queue backend exposes the flag with this default.
pub const DEFAULT_MQ_RECHECK_SECS: u64 = 30;

/// Default fetcher pause when `get_redo_record()` returns empty, seconds
/// (redoer-compatible name/default).
pub const DEFAULT_REDO_SLEEP_SECS: u64 = 60;

/// Default cadence of the redo throughput line (`--redo-stats-interval`):
/// one `Stats: N redo records processed` line every this many redo records
/// (redoer parity).
pub const DEFAULT_REDO_STATS_INTERVAL: u64 = 1000;

/// Default bound on the file-mode redo drain tail (`--redo-drain-max-secs`),
/// seconds after the input file is read to the end. `0` disables the cap, and
/// the cap is off by default (opt-in): set it together with the caller's own
/// job/task timeout.
pub const DEFAULT_REDO_DRAIN_MAX_SECS: u64 = 0;

/// Default cadence (seconds) at which the main loop flushes (seals) the INFO
/// sink's current output (`--info-sink-flush-secs`).
pub const DEFAULT_INFO_SINK_FLUSH_SECS: u64 = 30;

/// Flags and environment variables shared verbatim by every backend binary
/// (file mode, redo share, worker pool, long-record, transform, info/trace).
/// Each binary flattens this into its own clap `Args` next to its
/// transport-specific flags; [`Config::from_common`] resolves it.
#[derive(clap::Args, Debug, Clone)]
pub struct CommonArgs {
    /// Load records from a single JSONL file instead of the message queue (one
    /// JSON record per line); the queue-source flags are then ignored. redo%
    /// applies as in queue mode; at redo% > 0 the process exits once the file
    /// is loaded AND the redo queue is drained (redo% = 0: exits at end of file).
    #[arg(short = 'f', long = "file", env = "SENZING_INPUT_FILE")]
    pub input_file: Option<String>,

    /// In file mode, skip the first N physical lines before loading (resume an
    /// interrupted load). Ignored unless `--file` is set.
    #[arg(long = "skip-lines", env = "SENZING_SKIP_LINES", default_value_t = 0)]
    pub skip_lines: u64,

    /// In file mode, JSONL file that receives every rejected input line
    /// verbatim (bad data / retry timeout / unparseable) for later reprocessing.
    /// Defaults to `<input file>.rejected.jsonl`; appended to, created lazily
    /// on the first reject. Ignored unless `--file` is set.
    #[arg(long = "reject-file", env = "SENZING_REJECT_FILE")]
    pub reject_file: Option<String>,

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

    /// Seconds the redo fetcher pauses when no redo records are available.
    #[arg(
        long = "redo-sleep-secs",
        env = "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        default_value_t = DEFAULT_REDO_SLEEP_SECS
    )]
    pub redo_sleep_secs: u64,

    /// Log a redo throughput line (`Stats: N redo records processed, R/sec`)
    /// every this many processed redo records. Must be >= 1.
    #[arg(
        long = "redo-stats-interval",
        env = "SENZING_REDO_STATS_INTERVAL",
        default_value_t = DEFAULT_REDO_STATS_INTERVAL,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub redo_stats_interval: u64,

    /// File mode, redo% > 0: maximum seconds to keep draining redo after the
    /// input file has been read to the end. On expiry one final redo probe is
    /// made: drained -> normal exit 0; otherwise redo fetching stops, the
    /// remaining redo count is logged and the process exits 3. 0 (default) =
    /// no cap; opt-in, set it together with the caller's job/task timeout.
    /// Not applicable in queue mode or the pure redoer (no end of input).
    #[arg(
        long = "redo-drain-max-secs",
        env = "SENZING_REDO_DRAIN_MAX_SECS",
        default_value_t = DEFAULT_REDO_DRAIN_MAX_SECS
    )]
    pub redo_drain_max_secs: u64,

    /// Shared library implementing the INFO sink ABI (sz-info-sink). When set,
    /// add_record and process_redo_record run WITH_INFO and every INFO
    /// payload is written to the sink; any sink error is fatal.
    #[arg(long = "info-sink", env = "SENZING_INFO_SINK")]
    pub info_sink: Option<String>,

    /// Config string passed to the INFO sink plugin at create (e.g. JSON).
    #[arg(long = "info-sink-config", env = "SENZING_INFO_SINK_CONFIG")]
    pub info_sink_config: Option<String>,

    /// Seconds between INFO sink flushes (seal the current output) issued by
    /// a timer (file mode: its own thread, also during the load). Must be >= 1.
    #[arg(
        long = "info-sink-flush-secs",
        env = "SENZING_INFO_SINK_FLUSH_SECS",
        default_value_t = DEFAULT_INFO_SINK_FLUSH_SECS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub info_sink_flush_secs: u64,

    /// Seconds before a record is considered long-running; stats cadence is
    /// long_record / 2 (both sibling drivers' semantics).
    /// Must be >= 1: at 0 every in-flight record is instantly "stuck" and the
    /// monitor would dead-letter the whole queue (sibling drivers fell back to
    /// the default on 0/garbage; clap rejects it at startup here).
    #[arg(
        long = "long-record",
        env = "LONG_RECORD",
        default_value_t = DEFAULT_LONG_RECORD_SECS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub long_record: u64,

    /// Shared library implementing the record-transform ABI (sz-record-transform);
    /// applied to every load record before add_record.
    #[arg(
        long = "record-transform-plugin",
        env = "SENZING_RECORD_TRANSFORM_PLUGIN"
    )]
    pub record_transform_plugin: Option<String>,

    /// Opaque config string passed to the record-transform plugin at init.
    #[arg(
        long = "record-transform-config",
        env = "SENZING_RECORD_TRANSFORM_CONFIG"
    )]
    pub record_transform_config: Option<String>,

    /// Print the WithInfo response for each processed record. Inert at the
    /// engine level in this SDK (the WithInfo helper is always called);
    /// print-gating only, matching both sibling drivers.
    #[arg(short = 'i', long = "info", default_value_t = false)]
    pub info: bool,

    /// Output Senzing engine debug trace information (verbose logging).
    #[arg(short = 't', long = "debugTrace", default_value_t = false)]
    pub debug_trace: bool,
}

/// Suffix appended to the input path when `--reject-file` is not given.
pub const DEFAULT_REJECT_SUFFIX: &str = ".rejected.jsonl";

/// Resolves the file-mode reject path: the explicit `--reject-file` if
/// non-empty, else `<input_file>` + [`DEFAULT_REJECT_SUFFIX`]. `None` when not
/// in file mode (queue backends dead-letter to the broker instead).
pub fn resolve_reject_file(input_file: Option<&str>, explicit: Option<String>) -> Option<String> {
    let input = input_file?;
    Some(
        explicit
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("{input}{DEFAULT_REJECT_SUFFIX}")),
    )
}

/// Fully resolved runtime configuration after applying defaults and validating
/// required values.
#[derive(Debug, Clone)]
pub struct Config {
    pub engine_config: String,
    /// RabbitMQ only: `Some` iff redo% < 100 AND not file mode (validated by
    /// the RabbitMQ binary). Always `None` from [`Config::from_common`].
    pub url: Option<String>,
    /// RabbitMQ only: see `url`.
    pub queue: Option<String>,
    /// `Some` selects file-input mode (load from a JSONL file; redo% applies).
    pub input_file: Option<String>,
    /// File mode: physical lines to skip before loading (resume support).
    pub skip_lines: u64,
    /// File mode: JSONL side file receiving rejected lines verbatim. Always
    /// `Some` in file mode (explicit or derived); `None` otherwise.
    pub reject_file: Option<String>,
    pub redo_percent: u8,
    pub threads: usize,
    /// Queue mode: the TOTAL in-flight cap (received but not yet settled) on
    /// every transport, resolved by each backend via [`resolve_prefetch`];
    /// inert 0 from [`Config::from_common`].
    pub prefetch: usize,
    /// Queue mode: depth-probe cadence (`--mq-recheck-secs`, set by each queue
    /// backend); inert 0 from [`Config::from_common`].
    pub mq_recheck_secs: u64,
    pub redo_sleep_secs: u64,
    /// Redo throughput line cadence, in processed redo records (>= 1).
    pub redo_stats_interval: u64,
    pub long_record_secs: u64,
    pub info: bool,
    pub debug_trace: bool,
    /// Optional per-record transform (loaded at resolve time, fail-fast).
    pub transform: crate::transform::TransformHandle,
    /// File mode: redo drain cap after end of input; `None` = no cap.
    pub redo_drain_max: Option<std::time::Duration>,
    /// Optional INFO sink (created at resolve time, fail-fast).
    pub info_sink: crate::info_sink::InfoSinkHandle,
    /// Main-loop INFO sink flush cadence.
    pub info_sink_flush: std::time::Duration,
}

/// Reads and validates `SENZING_ENGINE_CONFIGURATION_JSON` from the environment.
/// Shared by every backend binary so the required-env + valid-JSON checks stay
/// in one place. Returns a loud, user-facing error string on failure.
pub fn engine_config_from_env() -> Result<String, String> {
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
    Ok(engine_config)
}

/// Resolves `--threads-per-process`: `0` falls back to the CPU count (sibling
/// drivers' compat behavior), anything else is used as-is. The single thread
/// fallback for every backend.
pub fn resolve_threads(threads_per_process: usize) -> usize {
    match threads_per_process {
        0 => num_cpus::get(),
        n => n,
    }
}

impl Config {
    /// Resolves the transport-independent configuration from [`CommonArgs`]
    /// and the (already validated) engine configuration JSON.
    ///
    /// Validates redo% range and the redo/load thread split, and loads the
    /// optional transform plugin (fail-fast). Transport fields (`url`, `queue`,
    /// `prefetch`, `mq_recheck_secs`) are left inert; a backend that uses them
    /// sets and validates them itself. Everything here runs before `Sz_init`.
    ///
    /// Returns an error message string for any invalid value so the caller can
    /// print it and exit non-zero (loud failure).
    pub fn from_common(common: &CommonArgs, engine_config: String) -> Result<Self, String> {
        if common.redo_percent > 100 {
            return Err(format!(
                "SENZING_REDO_PERCENT must be within [0, 100], got {}",
                common.redo_percent
            ));
        }
        let threads = resolve_threads(common.threads_per_process);
        validate_split_threads(threads, common.redo_percent)?;

        let transform = crate::transform::TransformHandle::load(
            common.record_transform_plugin.as_deref(),
            common.record_transform_config.as_deref(),
        )?;

        let info_sink = crate::info_sink::InfoSinkHandle::load(
            common.info_sink.as_deref(),
            common.info_sink_config.as_deref(),
        )?;

        let input_file = common.input_file.clone().filter(|s| !s.is_empty());
        Ok(Self {
            engine_config,
            url: None,
            queue: None,
            reject_file: resolve_reject_file(input_file.as_deref(), common.reject_file.clone()),
            input_file,
            skip_lines: common.skip_lines,
            redo_percent: common.redo_percent,
            threads,
            prefetch: 0,
            mq_recheck_secs: 0,
            redo_sleep_secs: common.redo_sleep_secs,
            redo_stats_interval: common.redo_stats_interval,
            long_record_secs: common.long_record,
            info: common.info,
            debug_trace: common.debug_trace,
            transform,
            redo_drain_max: crate::redo::drain_cap(common.redo_drain_max_secs),
            info_sink,
            info_sink_flush: std::time::Duration::from_secs(common.info_sink_flush_secs),
        })
    }

    /// Whether engine calls must return INFO: `--info` (print) or an INFO
    /// sink (which needs every payload).
    pub fn with_info(&self) -> bool {
        self.info || self.info_sink.is_enabled()
    }

    /// Number of redo-preferring workers (|B|) for this configuration.
    pub fn redo_pref_workers(&self) -> usize {
        redo_preferring_count(self.threads, self.redo_percent)
    }
}

/// 0 < redo% < 100 requires at least 2 workers: one worker cannot host both
/// preference classes and the |B| clamp is ill-defined at N = 1. Shared by the
/// RabbitMQ topology check and [`Config::from_common`] (every backend).
pub fn validate_split_threads(threads: usize, redo_percent: u8) -> Result<(), String> {
    if redo_percent > 0 && redo_percent < 100 && threads < 2 {
        return Err(format!(
            "0 < redo% < 100 requires at least 2 worker threads (got {threads}): \
             one worker cannot host both preference classes"
        ));
    }
    Ok(())
}

/// Resolves `--prefetch` / `SENZING_PREFETCH`: the TOTAL in-flight cap
/// (messages received but not yet settled) on every transport. `None` takes
/// the transport's `default`; a value below `threads` would idle workers, so
/// it is raised to `threads` with a warning.
pub fn resolve_prefetch(requested: Option<usize>, threads: usize, default: usize) -> usize {
    let prefetch = requested.unwrap_or(default);
    if prefetch >= threads {
        return prefetch;
    }
    eprintln!(
        "warning: --prefetch ({prefetch}) is below the worker count ({threads}); raising it \
         to {threads} (the in-flight cap must cover every worker)"
    );
    threads
}

/// |B|: how many of `threads` workers are redo-preferring (design §1.2).
///
/// `|B| = clamp(round(N × redo% / 100), 1, N−1)` for interior redo%; 0 at
/// redo% = 0 (the redo channel never exists); N at redo% = 100 (the load
/// channel never exists). Interior values assume `threads >= 2` (enforced by
/// [`validate_split_threads`]).
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
    use clap::Parser;

    /// Minimal binary-shaped parser so the shared flags are exercised exactly
    /// as each backend flattens them.
    #[derive(Parser, Debug)]
    struct Cli {
        #[command(flatten)]
        common: CommonArgs,
    }

    fn common(extra: &[&str]) -> CommonArgs {
        let mut argv = vec!["bin"];
        argv.extend_from_slice(extra);
        Cli::parse_from(argv).common
    }

    #[test]
    fn long_record_zero_is_rejected_at_parse() {
        let err = Cli::try_parse_from(["bin", "--long-record", "0"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
        let ok = Cli::try_parse_from(["bin", "--long-record", "1"]).expect("1 is valid");
        assert_eq!(ok.common.long_record, 1);
    }

    #[test]
    fn resolve_prefetch_is_the_total_cap_clamped_up_to_threads() {
        assert_eq!(resolve_prefetch(None, 4, 6), 6, "transport default");
        assert_eq!(resolve_prefetch(Some(9), 4, 6), 9, "explicit total");
        assert_eq!(resolve_prefetch(Some(4), 4, 6), 4, "== threads is kept");
        assert_eq!(resolve_prefetch(Some(1), 4, 6), 4, "below threads: raised");
        assert_eq!(resolve_prefetch(Some(0), 4, 6), 4, "0 is not unlimited");
    }

    #[test]
    fn from_common_applies_shared_defaults() {
        let c = Config::from_common(&common(&[]), "{}".into()).expect("defaults are valid");
        assert_eq!(c.engine_config, "{}");
        assert_eq!(c.redo_percent, DEFAULT_REDO_PERCENT);
        assert_eq!(c.threads, DEFAULT_THREADS);
        assert_eq!(c.redo_sleep_secs, DEFAULT_REDO_SLEEP_SECS);
        assert_eq!(c.long_record_secs, DEFAULT_LONG_RECORD_SECS);
        assert_eq!(c.skip_lines, 0);
        assert_eq!((c.input_file, c.reject_file), (None, None));
        assert_eq!(
            (c.url, c.queue),
            (None, None),
            "transport fields stay inert"
        );
        assert_eq!((c.prefetch, c.mq_recheck_secs), (0, 0));
        assert!(!c.info && !c.debug_trace);
    }

    #[test]
    fn redo_drain_cap_is_off_by_default_and_zero_disables_it() {
        let c = Config::from_common(&common(&[]), "{}".into()).unwrap();
        assert_eq!(c.redo_drain_max, None, "the cap is opt-in");
        assert_eq!(DEFAULT_REDO_DRAIN_MAX_SECS, 0);
        let c =
            Config::from_common(&common(&["--redo-drain-max-secs", "3600"]), "{}".into()).unwrap();
        assert_eq!(c.redo_drain_max, Some(std::time::Duration::from_secs(3600)));
        let c = Config::from_common(&common(&["--redo-drain-max-secs", "0"]), "{}".into()).unwrap();
        assert_eq!(c.redo_drain_max, None);
        let c = Config::from_common(&common(&["--redo-drain-max-secs", "5"]), "{}".into()).unwrap();
        assert_eq!(c.redo_drain_max, Some(std::time::Duration::from_secs(5)));
        assert!(Cli::try_parse_from(["bin", "--redo-drain-max-secs", "-1"]).is_err());
    }

    #[test]
    fn info_sink_defaults_off_and_with_info_follows_print_flag() {
        let c = Config::from_common(&common(&[]), "{}".into()).unwrap();
        assert!(!c.info_sink.is_enabled());
        assert!(!c.with_info());
        assert_eq!(
            c.info_sink_flush,
            std::time::Duration::from_secs(DEFAULT_INFO_SINK_FLUSH_SECS)
        );
        let c = Config::from_common(&common(&["--info"]), "{}".into()).unwrap();
        assert!(c.with_info(), "--info alone still requests INFO");
        let c = Config::from_common(&common(&["--info-sink", ""]), "{}".into()).unwrap();
        assert!(!c.info_sink.is_enabled(), "empty path = no sink");
    }

    /// The env names are part of the contract (deployments set them); check
    /// them through clap's own metadata rather than mutating the process env.
    #[test]
    fn new_options_bind_their_contract_env_names() {
        use clap::CommandFactory;
        let cmd = Cli::command();
        let env_of = |long: &str| {
            cmd.get_arguments()
                .find(|a| a.get_long() == Some(long))
                .and_then(|a| a.get_env())
                .map(|e| e.to_string_lossy().into_owned())
        };
        assert_eq!(
            env_of("redo-drain-max-secs").as_deref(),
            Some("SENZING_REDO_DRAIN_MAX_SECS")
        );
        assert_eq!(env_of("info-sink").as_deref(), Some("SENZING_INFO_SINK"));
        assert_eq!(
            env_of("info-sink-config").as_deref(),
            Some("SENZING_INFO_SINK_CONFIG")
        );
        assert_eq!(
            env_of("info-sink-flush-secs").as_deref(),
            Some("SENZING_INFO_SINK_FLUSH_SECS")
        );
        assert_eq!(
            env_of("redo-stats-interval").as_deref(),
            Some("SENZING_REDO_STATS_INTERVAL")
        );
    }

    #[test]
    fn redo_stats_interval_defaults_to_1000_and_must_be_positive() {
        let c = Config::from_common(&common(&[]), "{}".into()).unwrap();
        assert_eq!(c.redo_stats_interval, 1000, "redoer-parity default");
        assert_eq!(DEFAULT_REDO_STATS_INTERVAL, 1000);
        let c = Config::from_common(&common(&["--redo-stats-interval", "5"]), "{}".into()).unwrap();
        assert_eq!(c.redo_stats_interval, 5);
        let err = Cli::try_parse_from(["bin", "--redo-stats-interval", "0"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn info_sink_flush_secs_must_be_positive() {
        let err = Cli::try_parse_from(["bin", "--info-sink-flush-secs", "0"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
        let ok = Cli::try_parse_from(["bin", "--info-sink-flush-secs", "2"]).unwrap();
        assert_eq!(ok.common.info_sink_flush_secs, 2);
    }

    #[test]
    fn info_sink_options_parse_from_cli() {
        let c = common(&[
            "--info-sink",
            "/x/libsink.so",
            "--info-sink-config",
            r#"{"dir":"/w"}"#,
        ]);
        assert_eq!(c.info_sink.as_deref(), Some("/x/libsink.so"));
        assert_eq!(c.info_sink_config.as_deref(), Some(r#"{"dir":"/w"}"#));
    }

    #[test]
    fn info_sink_load_failure_is_a_config_error() {
        let err = Config::from_common(
            &common(&["--info-sink", "/nonexistent/libsink.so"]),
            "{}".into(),
        )
        .expect_err("missing plugin must fail before engine init");
        assert!(err.contains("dlopen"), "{err}");
    }

    #[test]
    fn from_common_file_mode_derives_reject_file() {
        let c = Config::from_common(&common(&["--file", "/data/in.jsonl"]), "{}".into()).unwrap();
        assert_eq!(c.input_file.as_deref(), Some("/data/in.jsonl"));
        assert_eq!(
            c.reject_file.as_deref(),
            Some("/data/in.jsonl.rejected.jsonl")
        );
        let c = Config::from_common(&common(&["--file", ""]), "{}".into()).unwrap();
        assert_eq!(c.input_file, None, "empty --file is not file mode");
    }

    #[test]
    fn from_common_rejects_redo_percent_over_100() {
        let err = Config::from_common(&common(&["--redo-percent", "101"]), "{}".into())
            .expect_err("redo% > 100 must fail");
        assert!(err.contains("[0, 100]"), "{err}");
    }

    #[test]
    fn from_common_rejects_single_thread_interior_split() {
        let args = common(&["--threads-per-process", "1", "--redo-percent", "50"]);
        assert!(Config::from_common(&args, "{}".into()).is_err());
        let args = common(&["--threads-per-process", "1", "--redo-percent", "100"]);
        assert!(Config::from_common(&args, "{}".into()).is_ok());
    }

    #[test]
    fn from_common_zero_threads_uses_cpu_count() {
        let c = Config::from_common(&common(&["--threads-per-process", "0"]), "{}".into())
            .expect("auto thread count is valid");
        assert_eq!(c.threads, num_cpus::get());
        assert_eq!(resolve_threads(7), 7);
    }

    #[test]
    fn reject_file_is_none_outside_file_mode() {
        assert_eq!(resolve_reject_file(None, Some("x.jsonl".into())), None);
        assert_eq!(resolve_reject_file(None, None), None);
    }

    #[test]
    fn reject_file_defaults_to_input_plus_suffix() {
        assert_eq!(
            resolve_reject_file(Some("/data/in.jsonl"), None),
            Some("/data/in.jsonl.rejected.jsonl".to_string())
        );
        assert_eq!(
            resolve_reject_file(Some("/data/in.jsonl"), Some(String::new())),
            Some("/data/in.jsonl.rejected.jsonl".to_string())
        );
    }

    #[test]
    fn reject_file_explicit_wins() {
        assert_eq!(
            resolve_reject_file(Some("/data/in.jsonl"), Some("/out/bad.jsonl".into())),
            Some("/out/bad.jsonl".to_string())
        );
    }

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
    fn split_threads_requires_two_for_interior_percent() {
        assert!(validate_split_threads(1, 50).is_err());
        assert!(validate_split_threads(2, 50).is_ok());
        // N = 1 is valid at both endpoints.
        assert!(validate_split_threads(1, 0).is_ok());
        assert!(validate_split_threads(1, 100).is_ok());
    }
}
