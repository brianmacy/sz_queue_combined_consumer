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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::thread;
use std::time::Duration;

use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use crate::record::RecordInfo;
use crate::stats::{
    ERRORS, REDO_IN_FLIGHT, REDOS_DROPPED, REDOS_PROCESSED, RUNNING, SAMPLE_REDO_RECORDS,
    WORKER_FATAL,
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
#[derive(Clone)]
pub struct DrainExit {
    pub input_done: Arc<AtomicBool>,
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
            ERRORS.fetch_add(1, Ordering::Relaxed);
            WORKER_FATAL.store(true, Ordering::Relaxed);
            RUNNING.store(false, Ordering::Relaxed);
            if let Some(result_tx) = &result_tx {
                let _ = result_tx.blocking_send(Outcome {
                    delivery_tag: 0,
                    info: RecordInfo::empty(),
                    action: Action::Fatal(format!("redo fetcher could not initialize engine: {e}")),
                });
            }
            if let Some(notify) = &shutdown_notify {
                notify.notify_waiters();
            }
            return;
        }
    };
    let sleep_dur = Duration::from_secs(redo_sleep_secs);
    let mut gate = DrainGate::default();
    // Records this fetcher has handed to the workers (drain-exit quiescence).
    let mut sent: usize = 0;

    while RUNNING.load(Ordering::Relaxed) {
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
                ERRORS.fetch_add(1, Ordering::Relaxed);
                WORKER_FATAL.store(true, Ordering::Relaxed);
                RUNNING.store(false, Ordering::Relaxed);
                if let Some(result_tx) = &result_tx {
                    let _ = result_tx.blocking_send(Outcome {
                        delivery_tag: 0,
                        info: RecordInfo::empty(),
                        action: Action::Fatal(format!("redo fetcher: {e}")),
                    });
                }
                if let Some(notify) = &shutdown_notify {
                    notify.notify_waiters();
                }
                break;
            }
        };

        if record.trim().is_empty() {
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
            if REDO_IN_FLIGHT.load(Ordering::Relaxed) > 0 {
                debug!("redo queue empty but redo still in flight; re-probing for cascades");
                interruptible_sleep(DRAIN_TAIL_REPROBE.min(sleep_dur));
            } else {
                info!("No redo records available. Pausing for {redo_sleep_secs} seconds.");
                interruptible_sleep(sleep_dur);
            }
            continue;
        }

        gate.observe(false, false, false); // non-empty probe resets the streak

        if SAMPLE_REDO_RECORDS.load(Ordering::Relaxed) {
            // Redo-floor guard tripped: sample raw records so the trigger
            // reason is visible in the log (a constant reason across
            // iterations is the documented escalate-to-Senzing signal).
            warn!("redo-floor sample: {record}");
        }

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
    let step = Duration::from_millis(250);
    let mut remaining = dur;
    while remaining > Duration::ZERO && RUNNING.load(Ordering::Relaxed) {
        let nap = remaining.min(step);
        thread::sleep(nap);
        remaining = remaining.saturating_sub(nap);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
