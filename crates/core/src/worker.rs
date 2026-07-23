//! Engine worker pool: homogeneous workers processing load AND redo items,
//! split into load-preferring / redo-preferring classes with cross-over
//! fallback (design §1.2/§2).
//!
//! # Dispatch discipline (the caught bug — do not "simplify" this)
//!
//! In mixed mode BOTH dequeues are NON-BLOCKING: a blocking `recv` on the
//! preferred channel would park the worker there and the cross-over fallback
//! would never fire (a load-preferring worker would sit on an empty `load_ch`
//! through the entire redo tail). A worker polls its preferred channel, then
//! the other, then sleeps a SHORT poll interval (1 ms) so preferred work and
//! cross-over are both picked up promptly; only after ~25 consecutive empty
//! passes (a genuine drought on BOTH channels) does it fall back to the long
//! 50 ms backoff. A bare 50 ms sleep between passes would be paid per-record
//! whenever prefetch credits are exhausted — a tens-of-% regression (design
//! §1.2). The +2 prefetch overshoot (config) masks the ack round-trip so the
//! short path is rarely taken at all.
//!
//! At the endpoints only one channel exists and there is no cross-over to
//! preserve, so the sibling drivers' exact dispatch is used verbatim:
//! blocking `recv` at redo% = 0 (consumer parity), `try_recv` + 50 ms backoff
//! at redo% = 100 (redoer parity).
//!
//! Locks are held only for the (non-blocking) `try_recv` itself, never across
//! an engine call — the redoer's proven discipline.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use crate::record::{ErrorClass, RecordInfo, classify_error, logging_id};
use crate::stats::{
    ERRORS, LOAD_BUSY_NS, REDO_BUSY_NS, REDO_IN_FLIGHT, REDOS_DROPPED, REDOS_PROCESSED, RUNNING,
    WORKER_FATAL, start_time,
};

/// Bit 62, the Senzing `SZ_WITH_INFO` flag (equals `SzFlags::WITH_INFO`). As of
/// sz-rust-sdk v4.3.1 this flag is HONORED: with it set, `add_record` /
/// `process_redo_record` call the with-info FFI entry point and return the info
/// payload; without it they return `SZ_NO_INFO` (empty string). `--info` gates
/// whether the flag is passed (and thus whether the payload is produced/PRINTED).
pub const SZ_WITH_INFO_BITS: u64 = 1 << 62;

/// Flags for `add_record` (consumer parity; observably a no-op at the engine).
pub fn add_record_flags(info: bool) -> Option<SzFlags> {
    if info {
        Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
    } else {
        Some(SzFlags::ADD_RECORD_DEFAULT_FLAGS)
    }
}

/// Flags for `process_redo_record` (redoer parity; same no-op caveat).
pub fn redo_flags(info: bool) -> Option<SzFlags> {
    if info {
        Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
    } else {
        None
    }
}

/// Throughput line cadence for redo records (redoer parity).
const REDO_STATS_INTERVAL: usize = 1000;

/// Short poll interval between non-blocking dispatch passes (mixed mode).
const SHORT_POLL: Duration = Duration::from_millis(1);
/// Consecutive empty passes before escalating to the long idle backoff.
const SHORT_POLL_PASSES: u32 = 25;
/// Long idle backoff once BOTH channels are in a genuine drought (redoer's
/// proven value).
const IDLE_BACKOFF: Duration = Duration::from_millis(50);

/// Total shutdown grace window shared by the in-flight drain and the bounded
/// worker join (both sibling drivers' hardened value). Keeps shutdown under
/// `docker stop`'s SIGTERM grace.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// A load unit of work handed from the async consumer to a worker thread.
pub struct LoadItem {
    pub delivery_tag: u64,
    pub body: Vec<u8>,
    pub info: RecordInfo,
}

/// A redo unit of work: a monotonic id (for the in-flight map) plus the raw
/// redo record JSON from `get_redo_record()`.
pub type RedoJob = (usize, String);

/// In-flight redo records: id -> (pickup time, raw JSON). Scanned by the
/// long-record monitor (redoer parity).
pub type RedoInFlight = HashMap<usize, (Instant, String)>;

/// What the async task should do with a load delivery once a worker finishes.
#[derive(Debug)]
pub enum Action {
    /// Engine accepted the record (optionally carrying the WithInfo response).
    Ack(Option<String>),
    /// Bad data / timeout / SENZ0082 -> dead-letter (reject, no requeue).
    RejectNoRequeue,
    /// A non-recoverable engine error -> trigger graceful shutdown.
    Fatal(String),
}

/// Result message sent from a worker back to the async task (load outcomes
/// only; redo outcomes are terminal in the worker — there is no broker
/// delivery to ack).
pub struct Outcome {
    pub delivery_tag: u64,
    pub info: RecordInfo,
    pub action: Action,
}

/// Static worker preference class (design §1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    LoadPreferring,
    RedoPreferring,
}

/// The load-side wiring a worker needs (present iff redo% < 100).
#[derive(Clone)]
pub struct LoadSide {
    pub work_rx: Arc<Mutex<mpsc::Receiver<LoadItem>>>,
    pub result_tx: mpsc::Sender<Outcome>,
    /// Delivery tags currently being processed by SOME worker (picked up from
    /// the channel). At shutdown this distinguishes design §6's two paths:
    /// (a) in-flight-in-worker -> reject(requeue=false) to the DLQ (the engine
    /// call may still complete — requeue would risk double-processing);
    /// (b) queued-but-unstarted -> left unacked (broker requeues on close).
    pub started: Arc<Mutex<HashSet<u64>>>,
    /// Wakeup optimization for the async loop on fatal errors. The DURABLE
    /// fatal signal is always the `Fatal` Outcome on `result_tx` (`Notify`
    /// stores no permit — consumer FIX-3).
    pub shutdown_notify: Arc<Notify>,
}

/// The redo-side wiring a worker needs (present iff redo% > 0).
#[derive(Clone)]
pub struct RedoSide {
    pub redo_rx: Arc<Mutex<std::sync::mpsc::Receiver<RedoJob>>>,
    pub in_flight: Arc<Mutex<RedoInFlight>>,
}

/// Everything one worker thread needs (bundled to keep the spawn site tidy).
pub struct WorkerCtx {
    pub worker_id: usize,
    pub class: Class,
    pub env: Arc<SzEnvironmentCore>,
    pub load: Option<LoadSide>,
    pub redo: Option<RedoSide>,
    pub add_flags: Option<SzFlags>,
    pub redo_flags: Option<SzFlags>,
    pub want_info: bool,
}

/// Worker thread entry point: derive an engine handle, then run the dispatch
/// loop appropriate to which work sources exist.
pub fn worker_loop(ctx: WorkerCtx) {
    let engine = match ctx.env.get_engine() {
        Ok(e) => e,
        Err(e) => {
            fatal_no_engine(&ctx, &e);
            return;
        }
    };

    match (&ctx.load, &ctx.redo) {
        (Some(_), None) => pure_load_loop(&ctx, engine.as_ref()),
        (None, Some(_)) => pure_redo_loop(&ctx, engine.as_ref()),
        (Some(_), Some(_)) => mixed_loop(&ctx, engine.as_ref()),
        (None, None) => unreachable!("worker requires at least one work source"),
    }
    debug!("worker {} finished", ctx.worker_id);
}

/// A worker that cannot obtain an engine handle is a fatal misconfiguration.
/// If it just returned, the bounded work channel would fill and the async loop
/// would wedge on `send`. Signal durably (result channel) plus a wakeup
/// (Notify), and stop the redo fetcher via the atomics.
fn fatal_no_engine(ctx: &WorkerCtx, e: &SzError) {
    error!("worker {}: failed to get engine: {e}", ctx.worker_id);
    ERRORS.fetch_add(1, Ordering::Relaxed);
    WORKER_FATAL.store(true, Ordering::Relaxed);
    RUNNING.store(false, Ordering::Relaxed);
    if let Some(load) = &ctx.load {
        // `delivery_tag = 0` is a sentinel (lapin tags start at 1).
        let _ = load.result_tx.blocking_send(Outcome {
            delivery_tag: 0,
            info: RecordInfo::empty(),
            action: Action::Fatal(format!(
                "worker {} could not initialize engine: {e}",
                ctx.worker_id
            )),
        });
        load.shutdown_notify.notify_waiters();
    }
}

/// redo% = 0 endpoint: single channel, plain blocking recv — dispatch parity
/// with `sz_rabbit_consumer_rust` (lock held only for the recv).
fn pure_load_loop(ctx: &WorkerCtx, engine: &dyn SzEngine) {
    let load = ctx
        .load
        .as_ref()
        .expect("pure_load_loop requires load side");
    loop {
        let item = {
            let mut rx = load.work_rx.lock().unwrap_or_else(PoisonError::into_inner);
            rx.blocking_recv()
        };
        let Some(item) = item else {
            // Channel closed -> graceful exit.
            break;
        };
        if !process_load(ctx, engine, load, item) {
            break;
        }
    }
}

/// redo% = 100 endpoint: single channel, `try_recv` + 50 ms backoff — dispatch
/// parity with `sz_simple_redoer_rust` (its fetcher runs ahead, so the backoff
/// fires only in a genuine drought).
fn pure_redo_loop(ctx: &WorkerCtx, engine: &dyn SzEngine) {
    let redo = ctx
        .redo
        .as_ref()
        .expect("pure_redo_loop requires redo side");
    loop {
        let try_result = {
            let rx = redo.redo_rx.lock().unwrap_or_else(PoisonError::into_inner);
            rx.try_recv()
        };
        let job = match try_result {
            Ok(job) => job,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if RUNNING.load(Ordering::Relaxed) {
                    thread::sleep(IDLE_BACKOFF);
                    continue;
                }
                // Shutdown requested: drain any record queued between the
                // try_recv above and the RUNNING check, then exit.
                let rx = redo.redo_rx.lock().unwrap_or_else(PoisonError::into_inner);
                match rx.try_recv() {
                    Ok(job) => job,
                    Err(_) => break,
                }
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        };
        if !process_redo(ctx, engine, redo, job) {
            break;
        }
    }
}

enum Next {
    Load(LoadItem),
    Redo(RedoJob),
}

enum TryOutcome<T> {
    Item(T),
    Empty,
    Closed,
}

fn try_recv_load(load: &LoadSide) -> TryOutcome<LoadItem> {
    let mut rx = load.work_rx.lock().unwrap_or_else(PoisonError::into_inner);
    match rx.try_recv() {
        Ok(item) => TryOutcome::Item(item),
        Err(mpsc::error::TryRecvError::Empty) => TryOutcome::Empty,
        Err(mpsc::error::TryRecvError::Disconnected) => TryOutcome::Closed,
    }
}

fn try_recv_redo(redo: &RedoSide) -> TryOutcome<RedoJob> {
    let rx = redo.redo_rx.lock().unwrap_or_else(PoisonError::into_inner);
    match rx.try_recv() {
        Ok(job) => TryOutcome::Item(job),
        Err(std::sync::mpsc::TryRecvError::Empty) => TryOutcome::Empty,
        Err(std::sync::mpsc::TryRecvError::Disconnected) => TryOutcome::Closed,
    }
}

/// Mixed mode (0 < redo% < 100): non-blocking preferred-channel dispatch with
/// cross-over fallback (see module docs). A worker exits when BOTH channels
/// are closed and drained.
fn mixed_loop(ctx: &WorkerCtx, engine: &dyn SzEngine) {
    let load = ctx.load.as_ref().expect("mixed_loop requires load side");
    let redo = ctx.redo.as_ref().expect("mixed_loop requires redo side");
    let mut load_open = true;
    let mut redo_open = true;

    'outer: loop {
        let mut idle_passes: u32 = 0;
        let next = loop {
            let prefer_redo = ctx.class == Class::RedoPreferring;
            let mut got: Option<Next> = None;
            // One non-blocking pass over both channels, preferred first.
            for pick_redo in [prefer_redo, !prefer_redo] {
                if pick_redo && redo_open {
                    match try_recv_redo(redo) {
                        TryOutcome::Item(job) => {
                            got = Some(Next::Redo(job));
                            break;
                        }
                        TryOutcome::Empty => {}
                        TryOutcome::Closed => redo_open = false,
                    }
                } else if !pick_redo && load_open {
                    match try_recv_load(load) {
                        TryOutcome::Item(item) => {
                            got = Some(Next::Load(item));
                            break;
                        }
                        TryOutcome::Empty => {}
                        TryOutcome::Closed => load_open = false,
                    }
                }
            }
            if let Some(next) = got {
                break next;
            }
            if !load_open && !redo_open {
                // Both sources closed and drained -> graceful exit.
                break 'outer;
            }
            idle_passes += 1;
            thread::sleep(if idle_passes <= SHORT_POLL_PASSES {
                SHORT_POLL
            } else {
                IDLE_BACKOFF
            });
        };
        let keep_going = match next {
            Next::Load(item) => process_load(ctx, engine, load, item),
            Next::Redo(job) => process_redo(ctx, engine, redo, job),
        };
        if !keep_going {
            break;
        }
    }
}

/// Processes one load item (`add_record`) and reports the outcome to the async
/// side. Returns `false` when the worker should exit (async side gone).
fn process_load(ctx: &WorkerCtx, engine: &dyn SzEngine, load: &LoadSide, item: LoadItem) -> bool {
    let LoadItem {
        delivery_tag,
        body,
        info,
    } = item;

    // Live-config-reload check (throttled process-globally; see config_reload).
    crate::config_reload::poll(&ctx.env);

    {
        let mut started = load.started.lock().unwrap_or_else(PoisonError::into_inner);
        started.insert(delivery_tag);
    }

    let t0 = Instant::now();
    let action = match std::str::from_utf8(&body) {
        Err(_) => {
            // Non-UTF-8 body is bad input -> dead-letter.
            warn!("worker {}: non-UTF-8 message body", ctx.worker_id);
            Action::RejectNoRequeue
        }
        Ok(body_str) => {
            let mut result =
                engine.add_record(&info.data_source, &info.record_id, body_str, ctx.add_flags);
            if result.is_err() && crate::config_reload::reinit_if_stale(&ctx.env) {
                // The registered default config drifted; the engine has been
                // reinitialized (handles stay valid) — retry the record once.
                result =
                    engine.add_record(&info.data_source, &info.record_id, body_str, ctx.add_flags);
            }
            match result {
                Ok(resp) => Action::Ack(if ctx.want_info { Some(resp) } else { None }),
                Err(e) => match classify_error(&e) {
                    ErrorClass::BadInputOrTimeout => Action::RejectNoRequeue,
                    ErrorClass::Fatal => Action::Fatal(e.to_string()),
                },
            }
        }
    };
    LOAD_BUSY_NS.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

    {
        let mut started = load.started.lock().unwrap_or_else(PoisonError::into_inner);
        started.remove(&delivery_tag);
    }

    if matches!(action, Action::Fatal(_)) {
        // Stop the redo fetcher and mark the process fatal; the DURABLE fatal
        // signal for the async side is the Outcome below (consumer FIX-3).
        ERRORS.fetch_add(1, Ordering::Relaxed);
        WORKER_FATAL.store(true, Ordering::Relaxed);
        RUNNING.store(false, Ordering::Relaxed);
    }

    load.result_tx
        .blocking_send(Outcome {
            delivery_tag,
            info,
            action,
        })
        .is_ok()
}

/// Processes one redo record. Redo outcomes are terminal here (no broker
/// delivery to ack): success/drop are counted; any non-BadInput error is FATAL
/// (redoer FIX-1 — a DB drop mid-run must not be silently absorbed). Returns
/// `false` when the worker should exit.
fn process_redo(ctx: &WorkerCtx, engine: &dyn SzEngine, redo: &RedoSide, job: RedoJob) -> bool {
    let (id, record) = job;

    // Live-config-reload check (throttled process-globally; see config_reload).
    crate::config_reload::poll(&ctx.env);

    REDO_IN_FLIGHT.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut map) = redo.in_flight.lock() {
        map.insert(id, (Instant::now(), record.clone()));
    }

    let t0 = Instant::now();
    let mut keep_going = true;
    let mut redo_result = engine.process_redo_record(&record, ctx.redo_flags);
    if redo_result.is_err() && crate::config_reload::reinit_if_stale(&ctx.env) {
        // Registered default config drifted; engine reinitialized — retry once.
        redo_result = engine.process_redo_record(&record, ctx.redo_flags);
    }
    match redo_result {
        Ok(result) => {
            let count = REDOS_PROCESSED.fetch_add(1, Ordering::Relaxed) + 1;
            if ctx.want_info && !result.is_empty() {
                println!("{result}");
            }
            if count.is_multiple_of(REDO_STATS_INTERVAL) {
                let elapsed = start_time().elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    count as f64 / elapsed
                } else {
                    0.0
                };
                info!("Stats: {count} redo records processed, {rate:.1}/sec");
            }
        }
        Err(e) => match classify_error(&e) {
            ErrorClass::BadInputOrTimeout => {
                // Bad data, SENZ0082, or a retryable/timeout error (incl.
                // SENZ0010). Redo records are engine-internal; there is no queue
                // to reject to, so log loudly and drop (redoer parity).
                warn!(
                    "REDO FAILED due to bad data or timeout [worker {}]: {}",
                    ctx.worker_id,
                    logging_id(&record)
                );
                REDOS_DROPPED.fetch_add(1, Ordering::Relaxed);
            }
            ErrorClass::Fatal => {
                error!(
                    "FATAL error processing redo record [worker {}]: {e} [{}]",
                    ctx.worker_id,
                    logging_id(&record)
                );
                ERRORS.fetch_add(1, Ordering::Relaxed);
                WORKER_FATAL.store(true, Ordering::Relaxed);
                RUNNING.store(false, Ordering::Relaxed);
                if let Some(load) = &ctx.load {
                    // Durable fatal for the async side (mixed mode).
                    let _ = load.result_tx.blocking_send(Outcome {
                        delivery_tag: 0,
                        info: RecordInfo::empty(),
                        action: Action::Fatal(e.to_string()),
                    });
                    load.shutdown_notify.notify_waiters();
                }
                keep_going = false;
            }
        },
    }
    REDO_BUSY_NS.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

    if let Ok(mut map) = redo.in_flight.lock() {
        map.remove(&id);
    }
    REDO_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
    keep_going
}

/// Scans the redo in-flight map for long runners (redoer parity), warning
/// per-class when every redo-capable worker is stuck.
pub fn monitor_redo_in_flight(
    in_flight: &Arc<Mutex<RedoInFlight>>,
    long_record_secs: u64,
    redo_capable_workers: usize,
) {
    let long_threshold = Duration::from_secs(long_record_secs);
    let stuck_threshold = Duration::from_secs(long_record_secs.saturating_mul(2));
    let mut num_stuck = 0usize;
    if let Ok(map) = in_flight.lock() {
        for (started, record) in map.values() {
            let duration = started.elapsed();
            if duration > long_threshold {
                info!(
                    "Long redo record ({:.1} min): {}",
                    duration.as_secs_f64() / 60.0,
                    logging_id(record)
                );
            }
            if duration > stuck_threshold {
                num_stuck += 1;
            }
        }
    }
    if redo_capable_workers > 0 && num_stuck >= redo_capable_workers {
        warn!("All {redo_capable_workers} redo-preferring threads are stuck on long redo records");
    }
}

/// Join all worker handles within [`SHUTDOWN_GRACE`] (redoer FIX-3, verbatim).
///
/// Returns `true` iff ALL workers finished within the window. A `false` return
/// means at least one worker is still inside an uninterruptible engine call;
/// the caller must NOT tear down the native environment in that case
/// (leak-on-exit over use-after-free).
pub fn join_workers_bounded(handles: Vec<thread::JoinHandle<()>>) -> bool {
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    let mut pending = handles;

    loop {
        let mut still_running = Vec::new();
        for handle in pending {
            if handle.is_finished() {
                if handle.join().is_err() {
                    error!("A worker thread panicked");
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                still_running.push(handle);
            }
        }
        pending = still_running;

        if pending.is_empty() {
            return true;
        }
        if Instant::now() >= deadline {
            warn!(
                "{} worker(s) still running after {:?} grace; detaching and \
                 skipping native teardown to avoid use-after-free",
                pending.len(),
                SHUTDOWN_GRACE
            );
            drop(pending);
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_flags_set_bit62_when_info_set() {
        assert_eq!(
            add_record_flags(true),
            Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
        );
        assert_eq!(
            add_record_flags(false),
            Some(SzFlags::ADD_RECORD_DEFAULT_FLAGS)
        );
    }

    #[test]
    fn redo_flags_match_redoer() {
        assert_eq!(
            redo_flags(true),
            Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
        );
        assert_eq!(redo_flags(false), None);
    }

    #[test]
    fn with_info_bit_is_bit_62() {
        assert_eq!(SZ_WITH_INFO_BITS, 1u64 << 62);
    }
}
