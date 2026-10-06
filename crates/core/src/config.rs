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
    /// RabbitMQ only (`basic_qos`); inert 0 from [`Config::from_common`].
    pub prefetch: u16,
    /// Queue mode: depth-probe cadence (`--mq-recheck-secs`, set by each queue
    /// backend); inert 0 from [`Config::from_common`].
    pub mq_recheck_secs: u64,
    pub redo_sleep_secs: u64,
    pub long_record_secs: u64,
    pub info: bool,
    pub debug_trace: bool,
    /// Optional per-record transform (loaded at resolve time, fail-fast).
    pub transform: crate::transform::TransformHandle,
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
            long_record_secs: common.long_record,
            info: common.info,
            debug_trace: common.debug_trace,
            transform,
        })
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
