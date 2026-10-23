//! The single redo-fetcher thread (design §3): one serial `get_redo_record()`
//! loop per process feeding a SMALL bounded channel.
//!
//! * ONE fetcher, never per-worker fetching: the single-producer pattern is
//!   the only documented-safe redo dequeue shape, and fetch (one DB round-trip)
//!   is far cheaper than `process_redo_record` (many round-trips), so it is not
//!   the bottleneck at 12 workers/process. Fleet scaling is by processes, and
//!   every process brings its own fetcher.
//! * The channel is deliberately tiny (|B| + 2): redo records are already
//!   durably queued in the DB — hoarding them in process memory buys nothing
//!   and loses work on crash (a fetched-but-unprocessed record was already
//!   dequeued). While the MQ is busy, the fetcher's blocking send against the
//!   full tiny channel is the redo-fetch gating valve (design §2.2): no token
//!   accounting, backpressure alone throttles fetch to actual consumption.
//! * Drain-tail short re-probe (design §9-Q9): when `get_redo_record()` comes
//!   back empty but workers still have redo in flight, cascades those calls
//!   enqueue would otherwise sit for a full `redo_sleep_secs` quantum — so
//!   re-probe after a couple of seconds and use the long sleep only when truly
//!   quiescent (steady-state poll budget unchanged).
//! * Drain exit (file mode only, [`DrainExit`]): queue mode never exits on an
//!   empty redo queue — a broker can always deliver more load, so the combined
//!   and pure-redoer paths run until SIGTERM. A file, by contrast, ENDS. Once
//!   the file is exhausted and every load outcome is in, the fetcher stops
//!   (closing `redo_ch`, which lets the workers exit) after
//!   [`DRAIN_EXIT_EMPTY_PROBES`] CONSECUTIVE `get_redo_record()` probes come
//!   back empty while this process has no redo outstanding (every record it
//!   fetched has been processed or dropped). Probes are `redo_sleep_secs`
//!   apart (the fetcher's normal quiescent pause), so the process exits
//!   ~`redo_sleep_secs` after redo first reads empty. Requiring zero
//!   outstanding work before the FIRST empty probe means nothing in this
//!   process can enqueue new redo afterwards; the second probe absorbs redo
//!   still arriving from sibling processes on the same store.
//! * Drain cap (file mode only, `--redo-drain-max-secs`, opt-in: default 0 =
//!   off): the drain tail is bounded. The clock starts when the input file has
//!   been read to the end (EOF), so a slow load never eats into it. Whichever
//!   comes first ends redo fetching: the two-empty-probe drain exit above
//!   (exit 0) or the cap. The cap can expire while the drain rule is still
//!   waiting for its second empty probe (a cap shorter than
//!   2 x `redo_sleep_secs` always does), so at expiry the fetcher first
//!   makes ONE final probe when the input is done and nothing it fetched is
//!   outstanding: an empty probe means redo is drained and the run ends
//!   normally (exit 0, no cap flag). Otherwise (work outstanding, or the
//!   probe returned a record, which is still handed over and processed) the
//!   fetcher stops fetching (a record it already fetched is still processed),
//!   sets [`REDO_DRAIN_CAP_EXPIRED`], and the run exits with
//!   `runtime::EXIT_REDO_DRAIN_CAP` after the file loader logs how many redo
//!   records remain queued. A shutdown signal or a fatal error is never
//!   reported as a cap expiry (see [`stop_reason`]).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use crate::record::RecordInfo;
use crate::stats::{
    ERRORS, REDO_DRAIN_CAP_EXPIRED, REDO_EMPTY_PROBES, REDO_FETCHED, REDO_IN_FLIGHT, REDOS_DROPPED,
    REDOS_PROCESSED, RUNNING, WORKER_FATAL,
};
use crate::worker::{Action, Outcome, RedoJob};

/// Monotonic id source for redo jobs (keys the in-flight map).
static NEXT_REDO_ID: AtomicUsize = AtomicUsize::new(0);

/// Re-probe interval while redo is still in flight (drain-tail cascades).
const DRAIN_TAIL_REPROBE: Duration = Duration::from_secs(2);

/// Consecutive empty, quiescent `get_redo_record()` probes (after the load
/// input is exhausted) that end a file-mode run. See the module docs.
pub const DRAIN_EXIT_EMPTY_PROBES: u32 = 2;

/// File-mode drain-exit wiring for [`fetcher_loop`]. `input_done` is set by
/// the file loader once the file is exhausted AND every dispatched load record
/// has reported an outcome (a still-running `add_record` can enqueue redo).
/// `input_read_at` is set once, when the reader hits end of file: the drain
/// cap's clock (`max_drain`, `None` = no cap).
#[derive(Clone)]
pub struct DrainExit {
    pub input_done: Arc<AtomicBool>,
    pub input_read_at: Arc<OnceLock<Instant>>,
    pub max_drain: Option<Duration>,
}

impl DrainExit {
    /// Fresh wiring: input not done, clock not started.
    pub fn new(max_drain: Option<Duration>) -> Self {
        Self {
            input_done: Arc::new(AtomicBool::new(false)),
            input_read_at: Arc::new(OnceLock::new()),
            max_drain,
        }
    }

    /// Starts the drain-cap clock (first call wins).
    pub fn mark_input_read(&self) {
        let _ = self.input_read_at.set(Instant::now());
    }

    /// `true` once the drain cap has elapsed since end of input.
    pub fn cap_expired(&self) -> bool {
        cap_expired(
            self.input_read_at.get().copied(),
            self.max_drain,
            Instant::now(),
        )
    }
}

/// `--redo-drain-max-secs` as a cap: `0` disables it.
pub fn drain_cap(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// Pure drain-cap check: `max` has elapsed since end of input (`eof`) at
/// `now`. No EOF yet, or no cap, never expires.
pub fn cap_expired(eof: Option<Instant>, max: Option<Duration>, now: Instant) -> bool {
    match (eof, max) {
        (Some(eof), Some(max)) => now.saturating_duration_since(eof) >= max,
        _ => false,
    }
}

/// Why the fetcher stops before its next probe (other exits — drained,
/// fatal fetch error, channel gone — happen inside the loop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetcherStop {
    /// Shutdown signal or fatal error elsewhere (`RUNNING` = false).
    Shutdown,
    /// The file-mode drain cap elapsed.
    DrainCap,
}

/// The fetcher's stop decision. A shutdown always wins over the cap so a
/// SIGTERM or fatal run keeps its own exit code.
pub fn stop_reason(running: bool, cap_expired: bool) -> Option<FetcherStop> {
    match (running, cap_expired) {
        (false, _) => Some(FetcherStop::Shutdown),
        (true, true) => Some(FetcherStop::DrainCap),
        (true, false) => None,
    }
}

/// At cap expiry: is redo drained? Only when the input is done, nothing
/// this fetcher handed out is outstanding, and the final probe was empty.
pub fn drained_at_cap(input_done: bool, quiescent: bool, probe_empty: bool) -> bool {
    input_done && quiescent && probe_empty
}

/// `true` when a cap is set below what the two-empty-probe drain rule needs
/// (2 x `redo_sleep_secs`), so it routinely expires before the rule can
/// confirm an empty queue (startup warning; the final probe still decides).
pub fn cap_shorter_than_drain_rule(cap: Option<Duration>, redo_sleep_secs: u64) -> bool {
    cap.is_some_and(|c| {
        c < Duration::from_secs(redo_sleep_secs.saturating_mul(u64::from(DRAIN_EXIT_EMPTY_PROBES)))
    })
}

/// Pure decision state for the drain exit (unit-tested without an engine).
#[derive(Debug, Default)]
pub struct DrainGate {
    consecutive_empty: u32,
}

impl DrainGate {
    /// Feed one probe result. `probe_empty`: `get_redo_record()` returned
    /// nothing; `input_done`: see [`DrainExit`]; `quiescent`: every redo record
    /// this fetcher handed out had completed BEFORE the probe was issued.
    /// Returns `true` when the fetcher should stop.
    pub fn observe(&mut self, probe_empty: bool, input_done: bool, quiescent: bool) -> bool {
        if probe_empty && input_done && quiescent {
            self.consecutive_empty = self.consecutive_empty.saturating_add(1);
        } else {
            self.consecutive_empty = 0;
        }
        self.consecutive_empty >= DRAIN_EXIT_EMPTY_PROBES
    }
}

/// Classifies one `get_redo_record()` result and counts it: an empty probe
/// bumps [`REDO_EMPTY_PROBES`], a record bumps [`REDO_FETCHED`] (at dequeue,
/// before the hand-off, so a worker can never finish it before it is counted).
/// Returns `true` when the probe was empty.
fn count_probe(record: &str) -> bool {
    let empty = record.trim().is_empty();
    let counter = if empty {
        &REDO_EMPTY_PROBES
    } else {
        &REDO_FETCHED
    };
    counter.fetch_add(1, Ordering::Relaxed);
    empty
}

/// Marks the process fatal from the fetcher and (mixed mode) hands the async
/// side its durable fatal signal plus a wakeup.
fn fetcher_fatal(
    result_tx: Option<&mpsc::Sender<Outcome>>,
    shutdown_notify: Option<&Arc<Notify>>,
    msg: String,
) {
    ERRORS.fetch_add(1, Ordering::Relaxed);
    WORKER_FATAL.store(true, Ordering::Relaxed);
    RUNNING.store(false, Ordering::Relaxed);
    if let Some(result_tx) = result_tx {
        let _ = result_tx.blocking_send(Outcome {
            delivery_tag: 0,
            info: RecordInfo::empty(),
            action: Action::Fatal(msg),
        });
    }
    if let Some(notify) = shutdown_notify {
        notify.notify_waiters();
    }
}

/// Runs the fetcher until shutdown, a fatal engine error, or the worker
/// channel disconnecting. Dropping `tx` on exit closes `redo_ch`, which is how
/// redo-capable workers learn no more redo is coming.
///
/// The engine handle is derived INSIDE this thread: `Box<dyn SzEngine>` is not
/// `Send` in this SDK, so handles never cross thread boundaries (the sibling
/// drivers' pattern).
///
/// `result_tx`/`shutdown_notify` exist only in mixed mode (redo% < 100), where
/// the async loop needs a DURABLE fatal signal; at redo% = 100 the monitor
/// loop polls the `RUNNING`/`WORKER_FATAL` atomics instead.
///
/// `drain_exit` is `Some` only in file mode: it adds the drain-exit rule (see
/// module docs). `None` keeps the queue-mode behaviour (never exit on idle).
pub fn fetcher_loop(
    env: Arc<SzEnvironmentCore>,
    tx: SyncSender<RedoJob>,
    redo_sleep_secs: u64,
    result_tx: Option<mpsc::Sender<Outcome>>,
    shutdown_notify: Option<Arc<Notify>>,
    drain_exit: Option<DrainExit>,
) {
    let engine = match env.get_engine() {
        Ok(engine) => engine,
        Err(e) => {
            // Fatal misconfiguration: signal durably and exit (dropping `tx`
            // closes redo_ch so redo-capable workers stop polling it).
            error!("redo fetcher: failed to get engine: {e}");
            fetcher_fatal(
                result_tx.as_ref(),
                shutdown_notify.as_ref(),
                format!("redo fetcher could not initialize engine: {e}"),
            );
            return;
        }
    };
    let sleep_dur = Duration::from_secs(redo_sleep_secs);
    let mut gate = DrainGate::default();
    // Records this fetcher has handed to the workers (drain-exit quiescence).
    let mut sent: usize = 0;

    let cap_hit = || drain_exit.as_ref().is_some_and(DrainExit::cap_expired);
    loop {
        match stop_reason(RUNNING.load(Ordering::Relaxed), cap_hit()) {
            None => {}
            Some(FetcherStop::Shutdown) => break,
            Some(FetcherStop::DrainCap) => {
                let drain = drain_exit
                    .as_ref()
                    .expect("the cap exists only with DrainExit");
                let input_done = drain.input_done.load(Ordering::Acquire);
                let quiescent = REDOS_PROCESSED.load(Ordering::Relaxed)
                    + REDOS_DROPPED.load(Ordering::Relaxed)
                    >= sent;
                // The final probe (only when it can prove a drain).
                let probe = if input_done && quiescent {
                    match engine.get_redo_record() {
                        Ok(record) => Some(record),
                        Err(e) => {
                            error!("Error retrieving redo record: {e}");
                            fetcher_fatal(
                                result_tx.as_ref(),
                                shutdown_notify.as_ref(),
                                format!("redo fetcher: {e}"),
                            );
                            break;
                        }
                    }
                } else {
                    None
                };
                let probe_empty = probe.as_deref().is_some_and(count_probe);
                if drained_at_cap(input_done, quiescent, probe_empty) {
                    info!(
                        "redo drain cap reached, but the final probe found the redo queue empty \
                         with nothing outstanding; redo drained, stopping"
                    );
                    break;
                }
                if let Some(record) = probe.filter(|r| !r.trim().is_empty()) {
                    // Already dequeued: hand it over so it is still processed.
                    let id = NEXT_REDO_ID.fetch_add(1, Ordering::Relaxed);
                    let _ = send_interruptible(&tx, (id, record));
                }
                REDO_DRAIN_CAP_EXPIRED.store(true, Ordering::Relaxed);
                warn!(
                    "redo drain cap reached ({}s after the input was read); no longer fetching \
                     redo (records already fetched are still processed)",
                    drain.max_drain.map_or(0, |d| d.as_secs())
                );
                break;
            }
        }
        // Periodic live-config-reload check (throttled process-globally; this is
        // the redo reader thread — see config_reload).
        crate::config_reload::poll(&env);
        // Snapshot BEFORE the probe: if everything sent had already finished,
        // no redo from this process can land after the probe.
        let quiescent =
            REDOS_PROCESSED.load(Ordering::Relaxed) + REDOS_DROPPED.load(Ordering::Relaxed) >= sent;
        let record = match engine.get_redo_record() {
            Ok(record) => record,
            Err(e) => {
                // Fatal engine/DB failure: tear the process down loudly.
                error!("Error retrieving redo record: {e}");
                fetcher_fatal(
                    result_tx.as_ref(),
                    shutdown_notify.as_ref(),
                    format!("redo fetcher: {e}"),
                );
                break;
            }
        };

        if count_probe(&record) {
            // Emptiness is ALWAYS detected here, by get_redo_record() coming
            // back empty — never by count_redo_records() (a table scan; it is
            // monitoring-only, design §2.4).
            if let Some(drain) = &drain_exit
                && gate.observe(true, drain.input_done.load(Ordering::Acquire), quiescent)
            {
                info!(
                    "Input exhausted and redo queue empty on {DRAIN_EXIT_EMPTY_PROBES} \
                     consecutive probes {redo_sleep_secs}s apart; redo drained, stopping"
                );
                break;
            }
            // The cap also cuts a pause short (the drain tail must not
            // overrun it by up to a full --redo-sleep-secs).
            let keep_sleeping = || !cap_hit();
            if REDO_IN_FLIGHT.load(Ordering::Relaxed) > 0 {
                debug!("redo queue empty but redo still in flight; re-probing for cascades");
                interruptible_sleep_while(DRAIN_TAIL_REPROBE.min(sleep_dur), keep_sleeping);
            } else {
                info!("No redo records available. Pausing for {redo_sleep_secs} seconds.");
                interruptible_sleep_while(sleep_dur, keep_sleeping);
            }
            continue;
        }

        gate.observe(false, false, false); // non-empty probe resets the streak

        let id = NEXT_REDO_ID.fetch_add(1, Ordering::Relaxed);
        if send_interruptible(&tx, (id, record)).is_err() {
            warn!("Redo fetcher stopping: shutdown requested or worker channel disconnected");
            break;
        }
        sent += 1;
    }
    // tx dropped here -> redo_ch closes.
}

/// Send a job, re-checking `RUNNING` so a shutdown signal unblocks a fetcher
/// wedged on a full channel (redoer FIX-2b, verbatim). Returns `Err` if
/// shutdown was requested before the send completed, or if the channel
/// disconnected.
fn send_interruptible(tx: &SyncSender<RedoJob>, mut job: RedoJob) -> Result<(), ()> {
    let step = Duration::from_millis(100);
    loop {
        if !RUNNING.load(Ordering::Relaxed) {
            return Err(());
        }
        match tx.try_send(job) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(returned)) => {
                // Channel saturated: this is the deliberate redo-fetch gating
                // valve while class B is busy. Wait briefly and retry,
                // re-checking RUNNING on the next iteration.
                job = returned;
                thread::sleep(step);
            }
            Err(TrySendError::Disconnected(_)) => return Err(()),
        }
    }
}

/// Sleep up to `dur`, waking early (in 250 ms steps) if shutdown is requested
/// (redoer parity).
pub fn interruptible_sleep(dur: Duration) {
    interruptible_sleep_while(dur, || true);
}

/// [`interruptible_sleep`] that also wakes early once `keep` returns `false`.
pub fn interruptible_sleep_while(dur: Duration, keep: impl Fn() -> bool) {
    let step = Duration::from_millis(250);
    let mut remaining = dur;
    while remaining > Duration::ZERO && RUNNING.load(Ordering::Relaxed) && keep() {
        let nap = remaining.min(step);
        thread::sleep(nap);
        remaining = remaining.saturating_sub(nap);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;

    /// Serializes the probe-counter tests: both read the two process-global
    /// counters, so one test's bump must not land inside the other's window.
    static PROBE_COUNTERS: Mutex<()> = Mutex::new(());

    fn probe_counters() -> (u64, u64) {
        (
            REDO_FETCHED.load(Ordering::Relaxed),
            REDO_EMPTY_PROBES.load(Ordering::Relaxed),
        )
    }

    #[test]
    fn a_redo_record_counts_as_fetched_only() {
        let _guard = PROBE_COUNTERS.lock().unwrap_or_else(|e| e.into_inner());
        let (fetched, empty) = probe_counters();
        assert!(!count_probe(r#"{"DATA_SOURCE":"X","RECORD_ID":"1"}"#));
        assert_eq!(probe_counters(), (fetched + 1, empty));
    }

    #[test]
    fn an_empty_probe_counts_as_empty_only() {
        let _guard = PROBE_COUNTERS.lock().unwrap_or_else(|e| e.into_inner());
        let (fetched, empty) = probe_counters();
        assert!(count_probe(""));
        assert!(count_probe(" \n"), "whitespace-only is empty, as before");
        assert_eq!(probe_counters(), (fetched, empty + 2));
    }

    #[test]
    fn drain_gate_exits_only_after_consecutive_quiescent_empty_probes_post_input() {
        let mut g = DrainGate::default();
        // While the file is still loading, empty redo never ends the run.
        for _ in 0..10 {
            assert!(!g.observe(true, false, true));
        }
        // Input done: first empty+quiescent probe is not enough...
        assert!(!g.observe(true, true, true));
        // ...the second consecutive one is.
        assert!(g.observe(true, true, true));
    }

    #[test]
    fn drain_cap_clock_starts_only_once_input_is_read() {
        let max = Some(Duration::from_secs(5));
        let t0 = Instant::now();
        // No EOF yet: the cap never fires, however long the load takes.
        assert!(!cap_expired(None, max, t0 + Duration::from_secs(3600)));
        // EOF at t0: not yet / exactly at / past the cap.
        assert!(!cap_expired(Some(t0), max, t0 + Duration::from_secs(4)));
        assert!(cap_expired(Some(t0), max, t0 + Duration::from_secs(5)));
        assert!(cap_expired(Some(t0), max, t0 + Duration::from_secs(6)));
        // A clock reading before EOF (cannot happen, but must not underflow).
        assert!(!cap_expired(Some(t0 + Duration::from_secs(1)), max, t0));
    }

    #[test]
    fn cap_expiry_is_a_drain_only_with_an_empty_final_probe_and_nothing_outstanding() {
        assert!(drained_at_cap(true, true, true), "drained: exit 0");
        assert!(!drained_at_cap(true, true, false), "probe returned redo");
        assert!(!drained_at_cap(true, false, true), "redo still outstanding");
        assert!(!drained_at_cap(false, true, true), "adds still running");
    }

    #[test]
    fn a_cap_below_two_redo_sleeps_is_flagged() {
        let s = |n| Some(Duration::from_secs(n));
        assert!(cap_shorter_than_drain_rule(s(3), 10), "reviewer case");
        assert!(cap_shorter_than_drain_rule(s(19), 10));
        assert!(!cap_shorter_than_drain_rule(s(20), 10));
        assert!(!cap_shorter_than_drain_rule(None, 10), "no cap");
        assert!(!cap_shorter_than_drain_rule(s(3600), 60));
    }

    #[test]
    fn drain_cap_zero_or_unset_disables_it() {
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(1_000_000);
        assert!(!cap_expired(Some(t0), None, later));
        assert_eq!(drain_cap(0), None, "0 disables the cap");
        assert_eq!(drain_cap(3600), Some(Duration::from_secs(3600)));
    }

    #[test]
    fn fetcher_stop_reason_prefers_shutdown_over_cap() {
        assert_eq!(stop_reason(true, false), None, "keep fetching");
        assert_eq!(stop_reason(true, true), Some(FetcherStop::DrainCap));
        // SIGTERM / fatal (RUNNING = false) is never reported as a cap expiry,
        // even when the cap has also passed: the exit code stays the
        // shutdown's (0 for a signal, 255 for a fatal).
        assert_eq!(stop_reason(false, false), Some(FetcherStop::Shutdown));
        assert_eq!(stop_reason(false, true), Some(FetcherStop::Shutdown));
    }

    #[test]
    fn drain_exit_reports_cap_from_its_own_clock() {
        let d = DrainExit::new(Some(Duration::from_secs(60)));
        assert!(!d.cap_expired(), "no EOF yet");
        d.mark_input_read();
        assert!(!d.cap_expired(), "EOF just now, 60 s cap");
        let first = *d.input_read_at.get().expect("set");
        d.mark_input_read();
        assert_eq!(
            *d.input_read_at.get().expect("set"),
            first,
            "the clock starts once; later marks do not restart it"
        );
        let unlimited = DrainExit::new(None);
        unlimited.mark_input_read();
        assert!(!unlimited.cap_expired());
    }

    #[test]
    fn interruptible_sleep_while_returns_early_when_told_to_stop() {
        let start = Instant::now();
        interruptible_sleep_while(Duration::from_secs(30), || false);
        assert!(start.elapsed() < Duration::from_secs(1));
        let start = Instant::now();
        interruptible_sleep_while(Duration::from_millis(300), || true);
        assert!(start.elapsed() >= Duration::from_millis(300));
    }

    #[test]
    fn drain_gate_streak_resets_on_redo_or_outstanding_work() {
        let mut g = DrainGate::default();
        assert!(!g.observe(true, true, true));
        // A redo record arrived (cascade / sibling process) -> reset.
        assert!(!g.observe(false, true, true));
        assert!(!g.observe(true, true, true));
        // Empty, but this process still has redo outstanding -> reset.
        assert!(!g.observe(true, true, false));
        assert!(!g.observe(true, true, true));
        assert!(g.observe(true, true, true));
    }
}
