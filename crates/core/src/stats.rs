//! Global counters and the `Combined stats:` status line (design §7).
//!
//! Counters are process-global atomics (this binary is one process = one
//! `Sz_init`), mirroring `sz_simple_redoer_rust`'s style. Both run paths (the
//! mixed tokio path and the pure-redoer path) share them.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use sz_rust_sdk::prelude::*;

/// Throughput reporting interval, in processed load records (consumer parity:
/// `Processed N adds, R records per second` every 10 000 adds).
pub const THROUGHPUT_INTERVAL: u64 = 10_000;

/// Emits the sibling drivers' `Processed N adds, R records per second` line
/// every [`THROUGHPUT_INTERVAL`] adds. Backend-agnostic: the caller feeds it
/// the running add count after each settled add.
pub struct ThroughputTicker {
    last_at: Instant,
}

impl Default for ThroughputTicker {
    fn default() -> Self {
        Self {
            last_at: Instant::now(),
        }
    }
}

impl ThroughputTicker {
    /// Returns the records/sec figure when `processed` just crossed an interval
    /// boundary (and resets the window), else `None`. `-1` mirrors the Python
    /// sibling when the window is zero-length.
    pub fn observe(&mut self, before: u64, processed: u64) -> Option<i64> {
        if processed > before && processed.is_multiple_of(THROUGHPUT_INTERVAL) {
            let elapsed = self.last_at.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                (THROUGHPUT_INTERVAL as f64 / elapsed) as i64
            } else {
                -1
            };
            self.last_at = Instant::now();
            Some(speed)
        } else {
            None
        }
    }

    /// Prints the throughput line if an interval boundary was crossed.
    pub fn report(&mut self, before: u64, processed: u64) {
        if let Some(speed) = self.observe(before, processed) {
            println!("Processed {processed} adds, {speed} records per second");
        }
    }
}

/// Response from the dedicated stats thread (blocking engine calls happen
/// there, never on an async task).
pub struct StatsPayload {
    pub engine_stats: Option<String>,
}

/// Dedicated thread owning one engine handle for blocking `get_stats()`.
/// Shared by every async backend (RabbitMQ, SQS); the pure redoer calls
/// `get_stats` on its own monitor thread instead.
///
/// TODO(reporting): reinstate a redo-backlog gauge WITHOUT count_redo_records().
/// count_redo_records() = `COUNT(*) FROM SYS_EVAL_QUEUE` (full table scan) and
/// dominated DB user CPU at Sayari scale. Reintroduce backlog via a cheap source
/// (e.g. engine get_stats redo counters, or a DB-side metadata rowcount like
/// sys.dm_db_partition_stats / pg_class.reltuples) so a backlog field can be
/// reported at ~zero DB cost.
pub fn stats_loop(
    env: Arc<SzEnvironmentCore>,
    req_rx: std::sync::mpsc::Receiver<()>,
    resp_tx: tokio::sync::mpsc::Sender<StatsPayload>,
) {
    let engine = match env.get_engine() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("stats thread: failed to get engine: {e}");
            return;
        }
    };
    while req_rx.recv().is_ok() {
        let engine_stats = match engine.get_stats() {
            Ok(stats) => Some(stats),
            Err(e) => {
                tracing::warn!("get_stats failed: {e}");
                None
            }
        };
        if resp_tx
            .blocking_send(StatsPayload { engine_stats })
            .is_err()
        {
            break;
        }
    }
}

/// Handles for the dedicated stats thread of an async backend.
pub struct StatsThread {
    /// One `()` per monitor tick requests a `get_stats()`; dropping it stops
    /// the thread.
    pub req_tx: std::sync::mpsc::Sender<()>,
    pub resp_rx: tokio::sync::mpsc::Receiver<StatsPayload>,
    pub handle: std::thread::JoinHandle<()>,
}

/// Spawns `sz-stats` running [`stats_loop`] (blocking `get_stats` only).
pub fn spawn_stats_thread(env: &Arc<SzEnvironmentCore>) -> anyhow::Result<StatsThread> {
    use anyhow::Context;
    let stats_env = env.clone();
    let (req_tx, req_rx) = std::sync::mpsc::channel::<()>();
    let (resp_tx, resp_rx) = tokio::sync::mpsc::channel::<StatsPayload>(1);
    let handle = std::thread::Builder::new()
        .name("sz-stats".to_string())
        .spawn(move || stats_loop(stats_env, req_rx, resp_tx))
        .context("failed to spawn stats thread")?;
    Ok(StatsThread {
        req_tx,
        resp_rx,
        handle,
    })
}

/// Rate state for the async backends' `Combined stats:` line: one line per
/// stats-thread answer, rates computed over the time since the previous one.
pub struct StatusTicker {
    redo_percent: u8,
    load_pref: usize,
    redo_pref: usize,
    last_status_at: Instant,
    prev_adds: usize,
    prev_redos: usize,
}

impl StatusTicker {
    /// Starts the rate window now.
    pub fn new(redo_percent: u8, load_pref: usize, redo_pref: usize) -> Self {
        Self {
            redo_percent,
            load_pref,
            redo_pref,
            last_status_at: Instant::now(),
            prev_adds: 0,
            prev_redos: 0,
        }
    }

    /// Prints the `Engine stats:` line (when present) then the status line.
    pub fn on_payload(&mut self, payload: &StatsPayload, mq_depth: Option<u32>) {
        if let Some(engine_stats) = &payload.engine_stats {
            // The prefix is MANDATORY: the harness scrapes on
            // "Engine stats:" (the bare {"workload":...} line broke
            // scrape_engine_stats — FAQ-documented bug).
            println!("Engine stats: {engine_stats}");
        }
        let now = Instant::now();
        let dt = now
            .duration_since(self.last_status_at)
            .as_secs_f64()
            .max(0.001);
        let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
        let redos = REDOS_PROCESSED.load(Ordering::Relaxed);
        emit_status_line(&StatusLine {
            redo_percent: self.redo_percent,
            load_pref: self.load_pref,
            redo_pref: self.redo_pref,
            adds,
            adds_rate: (adds - self.prev_adds) as f64 / dt,
            redos,
            redos_rate: (redos - self.prev_redos) as f64 / dt,
            mq_depth,
        });
        self.prev_adds = adds;
        self.prev_redos = redos;
        self.last_status_at = now;
    }
}

/// Prints the final `Processed total of ...` line the e2e tests scrape. `adds`
/// is the backend's own add total (the backends count it differently).
pub fn print_final_totals(adds: u64) {
    println!(
        "Processed total of {adds} adds, {} redo records ({} redo dropped, {} errors)",
        REDOS_PROCESSED.load(Ordering::Relaxed),
        REDOS_DROPPED.load(Ordering::Relaxed),
        ERRORS.load(Ordering::Relaxed),
    );
}

/// Global run flag: flipped to `false` on shutdown (signal or fatal error).
pub static RUNNING: AtomicBool = AtomicBool::new(true);

/// Set when a worker/fetcher hits a fatal condition so the process exits
/// non-zero after orderly teardown.
pub static WORKER_FATAL: AtomicBool = AtomicBool::new(false);

pub static ADDS_PROCESSED: AtomicUsize = AtomicUsize::new(0);
pub static ADDS_REJECTED: AtomicUsize = AtomicUsize::new(0);
pub static REDOS_PROCESSED: AtomicUsize = AtomicUsize::new(0);
pub static REDOS_DROPPED: AtomicUsize = AtomicUsize::new(0);
pub static ERRORS: AtomicUsize = AtomicUsize::new(0);

/// Number of redo records currently inside `process_redo_record`. Used by the
/// fetcher's drain-tail short re-probe (design §9-Q9): while in-flight redo can
/// still enqueue cascades, an empty `get_redo_record()` probe must not sleep
/// the full `redo_sleep_secs`.
pub static REDO_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Cumulative wall-clock nanoseconds workers spent inside load / redo engine
/// calls. Their ratio is the measured `redo_share_effective`, so the redo
/// capacity floor is verifiable from logs rather than assumed (design §7).
pub static LOAD_BUSY_NS: AtomicU64 = AtomicU64::new(0);
pub static REDO_BUSY_NS: AtomicU64 = AtomicU64::new(0);

static START_TIME: OnceLock<Instant> = OnceLock::new();

/// Process start time (first call wins; call once early in `main`).
pub fn start_time() -> Instant {
    *START_TIME.get_or_init(Instant::now)
}

/// Inputs for one `Combined stats:` status line (design §7).
pub struct StatusLine {
    pub redo_percent: u8,
    pub load_pref: usize,
    pub redo_pref: usize,
    pub adds: usize,
    pub adds_rate: f64,
    pub redos: usize,
    pub redos_rate: f64,
    /// Latest passive-declare depth; `None` when unknown (or at redo% = 100,
    /// where no AMQP connection exists and the field is omitted).
    pub mq_depth: Option<u32>,
}

/// Emits the machine-parseable `Combined stats: {...}` line.
///
/// Endpoint consistency (design §7): at redo% = 0 the redo fields are omitted
/// (no redo work happens there, design §3); at redo% = 100 the add/MQ fields are omitted (no
/// AMQP connection exists). The prefix `Combined stats:` is distinct from
/// `Engine stats:` so harness parsers can split driver-level from engine-level
/// metrics.
pub fn emit_status_line(s: &StatusLine) {
    let mut obj = serde_json::Map::new();
    if s.redo_percent < 100 {
        obj.insert("adds".into(), s.adds.into());
        obj.insert("adds_rate".into(), round1(s.adds_rate).into());
        obj.insert(
            "adds_rejected".into(),
            ADDS_REJECTED.load(Ordering::Relaxed).into(),
        );
        if let Some(depth) = s.mq_depth {
            obj.insert("mq_depth".into(), depth.into());
        }
    }
    if s.redo_percent > 0 {
        obj.insert("redos".into(), s.redos.into());
        obj.insert("redos_rate".into(), round1(s.redos_rate).into());
        obj.insert(
            "redos_dropped".into(),
            REDOS_DROPPED.load(Ordering::Relaxed).into(),
        );
    }
    obj.insert("errors".into(), ERRORS.load(Ordering::Relaxed).into());

    let load_ns = LOAD_BUSY_NS.load(Ordering::Relaxed);
    let redo_ns = REDO_BUSY_NS.load(Ordering::Relaxed);
    if load_ns + redo_ns > 0 {
        let share = redo_ns as f64 / (load_ns + redo_ns) as f64;
        obj.insert(
            "redo_share_effective".into(),
            ((share * 1000.0).round() / 1000.0).into(),
        );
    }

    obj.insert("mode".into(), mode(s).into());
    obj.insert(
        "threads".into(),
        serde_json::json!({ "load_pref": s.load_pref, "redo_pref": s.redo_pref }),
    );

    println!("Combined stats: {}", serde_json::Value::Object(obj));
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// Mode is derived, not tracked: the scheduler has no mode state machine
/// (design §2.2) — this string exists purely for log readability.
fn mode(s: &StatusLine) -> &'static str {
    match s.redo_percent {
        0 => "load_only",
        100 => "redo_only",
        _ => match s.mq_depth {
            Some(0) => "redo_drain",
            _ => "mixed",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throughput_ticker_fires_only_on_interval_boundary_crossings() {
        let mut t = ThroughputTicker::default();
        assert_eq!(t.observe(0, 1), None);
        assert_eq!(t.observe(9_999, 9_999), None, "no progress -> no report");
        assert!(t.observe(9_999, 10_000).is_some());
        assert_eq!(t.observe(10_000, 10_001), None);
        assert!(t.observe(19_999, 20_000).is_some());
    }

    #[test]
    fn status_ticker_advances_its_rate_window_per_payload() {
        let mut t = StatusTicker::new(20, 10, 2);
        let first = t.last_status_at;
        std::thread::sleep(std::time::Duration::from_millis(2));
        t.on_payload(&StatsPayload { engine_stats: None }, Some(3));
        assert!(t.last_status_at > first);
        // Counters only grow, so the snapshot never exceeds the live value.
        assert!(t.prev_adds <= ADDS_PROCESSED.load(Ordering::Relaxed));
        assert!(t.prev_redos <= REDOS_PROCESSED.load(Ordering::Relaxed));
        assert_eq!((t.redo_percent, t.load_pref, t.redo_pref), (20, 10, 2));
    }

    #[test]
    fn mode_derivation() {
        let mut s = StatusLine {
            redo_percent: 20,
            load_pref: 10,
            redo_pref: 2,
            adds: 0,
            adds_rate: 0.0,
            redos: 0,
            redos_rate: 0.0,
            mq_depth: Some(5),
        };
        assert_eq!(mode(&s), "mixed");
        s.mq_depth = Some(0);
        assert_eq!(mode(&s), "redo_drain");
        s.redo_percent = 0;
        assert_eq!(mode(&s), "load_only");
        s.redo_percent = 100;
        assert_eq!(mode(&s), "redo_only");
    }
}
