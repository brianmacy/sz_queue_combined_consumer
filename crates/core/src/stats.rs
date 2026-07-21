//! Global counters, the `Combined stats:` status line, the redo-backlog EWMA
//! slope, and the redo-floor guard (design §6/§7).
//!
//! Counters are process-global atomics (this binary is one process = one
//! `Sz_init`), mirroring `sz_simple_redoer_rust`'s style. Both run paths (the
//! mixed tokio path and the pure-redoer path) share them.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

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

/// When set (by the redo-floor guard), the redo fetcher logs each raw redo
/// record it dequeues. Per the Senzing redo guidance the record carries the
/// redo trigger-reason; a constant reason across iterations is the documented
/// signal to escalate — without the sample the floor warning is not actionable.
pub static SAMPLE_REDO_RECORDS: AtomicBool = AtomicBool::new(false);

static START_TIME: OnceLock<Instant> = OnceLock::new();

/// Process start time (first call wins; call once early in `main`).
pub fn start_time() -> Instant {
    *START_TIME.get_or_init(Instant::now)
}

/// Exponentially weighted moving average, used for the redo-backlog slope
/// (Δ `count_redo_records` per stats interval).
pub struct Ewma {
    alpha: f64,
    value: Option<f64>,
}

impl Ewma {
    pub fn new(alpha: f64) -> Self {
        Self { alpha, value: None }
    }

    /// Folds in a new sample and returns the updated average.
    pub fn update(&mut self, sample: f64) -> f64 {
        let v = match self.value {
            None => sample,
            Some(prev) => self.alpha * sample + (1.0 - self.alpha) * prev,
        };
        self.value = Some(v);
        v
    }

    pub fn value(&self) -> Option<f64> {
        self.value
    }
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
    /// Latest `count_redo_records()`; `None` when unavailable (or at
    /// redo% = 0, where the call is never made and the field is omitted).
    pub redo_backlog: Option<i64>,
    pub redo_backlog_slope: Option<f64>,
}

/// Emits the machine-parseable `Combined stats: {...}` line.
///
/// Endpoint consistency (design §7): at redo% = 0 the redo fields are omitted
/// (emitting them would require the `count_redo_records()` call that §3
/// promises never happens); at redo% = 100 the add/MQ fields are omitted (no
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
        if let Some(backlog) = s.redo_backlog {
            obj.insert("redo_backlog".into(), backlog.into());
        }
        if let Some(slope) = s.redo_backlog_slope {
            obj.insert("redo_backlog_slope".into(), round1(slope).into());
        }
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

/// Consecutive suspicious stats intervals before the redo-floor guard trips.
pub const FLOOR_GUARD_INTERVALS: u32 = 5;
/// "Small" backlog ceiling for the floor heuristic (a genuine backlog being
/// worked down is large; a `__REPAIR__` loop idles at a handful of rows).
const FLOOR_BACKLOG_SMALL: i64 = 1000;
/// Max |Δbacklog| per interval still considered "flat".
const FLOOR_BACKLOG_FLAT_DELTA: i64 = 5;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum GuardTransition {
    /// Conditions held for [`FLOOR_GUARD_INTERVALS`]: warn loudly and start
    /// sampling raw redo records ([`SAMPLE_REDO_RECORDS`]).
    Tripped,
    /// Conditions cleared after a trip: stop sampling.
    Cleared,
    Unchanged,
}

/// Redo-floor / `__REPAIR__`-loop guard (design §6, from the 4.2.4 harness
/// gap): redo throughput > 0 while `count_redo_records()` stays flat at a small
/// value and the MQ is empty, for `k` consecutive stats intervals, suggests a
/// redo loop that will never drain. The guard only WARNS (and samples records);
/// the decision to stop remains with the harness/operator.
///
/// At redo% = 100 no AMQP exists, so `mq_depth` is `None` and the "MQ is empty"
/// conjunct is vacuously TRUE — the 4.2.4 `__REPAIR__` floor was hit precisely
/// by pure redoers at the drain tail, so the check must not be unsatisfiable
/// there.
#[derive(Default)]
pub struct FloorGuard {
    consecutive: u32,
    prev_backlog: Option<i64>,
    prev_redos: usize,
    active: bool,
}

impl FloorGuard {
    /// Feed one stats interval's observations; returns the guard transition.
    pub fn observe(
        &mut self,
        redos_total: usize,
        backlog: Option<i64>,
        mq_depth: Option<u32>,
    ) -> GuardTransition {
        let redo_progress = redos_total > self.prev_redos;
        let mq_empty = mq_depth.is_none_or(|d| d == 0);
        let backlog_small_flat = matches!(
            (backlog, self.prev_backlog),
            (Some(b), Some(p))
                if b > 0 && b <= FLOOR_BACKLOG_SMALL && (b - p).abs() <= FLOOR_BACKLOG_FLAT_DELTA
        );
        let suspicious = redo_progress && mq_empty && backlog_small_flat;

        self.prev_backlog = backlog;
        self.prev_redos = redos_total;

        if suspicious {
            self.consecutive += 1;
            if self.consecutive >= FLOOR_GUARD_INTERVALS && !self.active {
                self.active = true;
                return GuardTransition::Tripped;
            }
        } else {
            self.consecutive = 0;
            if self.active {
                self.active = false;
                return GuardTransition::Cleared;
            }
        }
        GuardTransition::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_first_sample_is_identity() {
        let mut e = Ewma::new(0.3);
        assert_eq!(e.value(), None);
        assert!((e.update(10.0) - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ewma_converges_toward_samples() {
        let mut e = Ewma::new(0.5);
        e.update(0.0);
        let v = e.update(10.0);
        assert!((v - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn floor_guard_trips_after_k_flat_intervals_and_clears() {
        let mut g = FloorGuard::default();
        // First observation only primes prev_* (never suspicious).
        assert_eq!(g.observe(100, Some(50), None), GuardTransition::Unchanged);
        let mut tripped_at = None;
        for i in 1..=FLOOR_GUARD_INTERVALS {
            let t = g.observe(100 + i as usize, Some(50), None);
            if t == GuardTransition::Tripped {
                tripped_at = Some(i);
            }
        }
        assert_eq!(tripped_at, Some(FLOOR_GUARD_INTERVALS));
        // Backlog moves substantially -> condition clears.
        assert_eq!(g.observe(200, Some(500), None), GuardTransition::Cleared);
    }

    #[test]
    fn floor_guard_not_suspicious_while_mq_busy() {
        let mut g = FloorGuard::default();
        g.observe(100, Some(50), Some(10));
        for i in 1..=(FLOOR_GUARD_INTERVALS * 2) {
            // MQ has depth -> never trips no matter how flat the backlog is.
            assert_eq!(
                g.observe(100 + i as usize, Some(50), Some(10)),
                GuardTransition::Unchanged
            );
        }
    }

    #[test]
    fn floor_guard_not_suspicious_without_redo_progress() {
        let mut g = FloorGuard::default();
        g.observe(100, Some(50), None);
        for _ in 0..(FLOOR_GUARD_INTERVALS * 2) {
            assert_eq!(g.observe(100, Some(50), None), GuardTransition::Unchanged);
        }
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
            redo_backlog: None,
            redo_backlog_slope: None,
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
