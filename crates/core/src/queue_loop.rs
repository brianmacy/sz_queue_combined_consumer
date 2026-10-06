//! The broker-agnostic queue-mode (redo% < 100) event loop shared by every
//! async backend. A backend supplies only a [`Transport`] (receive, ack,
//! dead-letter, depth, close) and a [`Policy`]; this module owns everything
//! else: the in-flight table, the engine pool, the stats thread, the
//! long-record monitor, the diagnostic depth probe and the shutdown sequence.
//!
//! Shutdown (consumer FIX-2): on a signal (SIGINT/SIGTERM/SIGHUP), a fatal
//! outcome or the end of the delivery stream, intake stops, the work channel
//! is closed, results are drained until ONE deadline (`SHUTDOWN_GRACE` after
//! the trigger), every engine-owning thread (workers, redo fetcher, stats) is
//! joined against that same deadline, and whatever is still unsettled is
//! handled per [`Policy`] — design §6: (a) in-flight-in-worker vs (b)
//! queued-but-unstarted.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sz_rust_sdk::prelude::*;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Notify, mpsc};

use crate::config::Config;
use crate::pool::{EnginePool, spawn_engine_pool};
use crate::queue_run::{RunOutcome, monitor_interval};
use crate::record::{ParseError, RecordInfo, parse_record};
use crate::stats::{
    self, ADDS_PROCESSED, ADDS_REJECTED, RUNNING, StatsThread, StatusTicker, ThroughputTicker,
};
use crate::worker::{Action, LoadItem, Outcome, SHUTDOWN_GRACE, monitor_redo_in_flight};

/// Message logged (and reported as the fatal reason) when a worker or the
/// redo fetcher signals a fatal shutdown via the wakeup `Notify`.
const WORKER_FATAL_MSG: &str = "worker or fetcher thread failed fatally";

/// One message received from the broker. `tag` is the backend's opaque,
/// process-unique delivery identifier used for every later settle call.
pub struct Delivery {
    pub tag: u64,
    pub body: Vec<u8>,
}

/// Why a delivery is being dead-lettered. Its [`Display`](std::fmt::Display)
/// text is the reason on the `REJECTING:` stdout marker and the dead-letter
/// metadata a backend can attach (SQS: the `SzReason` message attribute).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadLetterReason {
    /// Unparseable body (poison message), never handed to a worker; carries
    /// the parse error.
    Malformed(String),
    /// A worker reported bad data / timeout; carries the engine (or
    /// transform) error text.
    Rejected(String),
    /// Still processing past `2 * LONG_RECORD`.
    LongRecord,
    /// Still inside a worker when the shutdown grace elapsed.
    Shutdown,
}

impl std::fmt::Display for DeadLetterReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(err) => write!(f, "malformed record: {err}"),
            Self::Rejected(err) => f.write_str(err),
            Self::LongRecord => f.write_str("still processing past 2x LONG_RECORD"),
            Self::Shutdown => f.write_str(
                "in-flight-in-worker on shutdown (engine call may still complete in background)",
            ),
        }
    }
}

/// A message broker as seen by [`run`]. Settle methods log their own failures
/// (a failed ack/reject never stops the loop).
pub trait Transport {
    /// Next delivery; `None` when the stream has ended. MUST be cancel-safe:
    /// it is polled inside `tokio::select!`.
    fn recv(&mut self) -> impl Future<Output = Option<Result<Delivery>>> + Send;
    /// Settles a successfully loaded delivery.
    fn ack(&mut self, tag: u64) -> impl Future<Output = ()> + Send;
    /// Removes a delivery for good, to the dead-letter destination (reject
    /// without requeue).
    fn dead_letter(
        &mut self,
        tag: u64,
        reason: DeadLetterReason,
    ) -> impl Future<Output = ()> + Send;
    /// Leaves a delivery for redelivery at shutdown. Default: no-op (the
    /// broker redelivers whatever is unsettled when the connection closes).
    fn release(&mut self, _tag: u64) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Called each monitor tick for a delivery running past `LONG_RECORD`
    /// (e.g. to extend a visibility lease). Default: no-op.
    fn extend_lease(&mut self, _tag: u64, _elapsed: Duration) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Stops taking new deliveries (called once, when the running loop ends
    /// and before the drain), so a backend that prefetches on its own (e.g. a
    /// polling task) does not keep pulling messages nobody will process.
    /// Default: no-op (push consumers stop with the connection close).
    fn stop_intake(&mut self) {}
    /// Current queue depth for the diagnostic probe; `None` if unknown.
    fn depth(&mut self) -> impl Future<Output = Option<u32>> + Send;
    /// Closes the broker connection (last call of a run).
    fn close(self) -> impl Future<Output = ()> + Send;
}

/// Per-backend settle policy for the two cases a broker answers differently.
pub struct Policy {
    /// Dead-letter a load delivery still running at `2 * LONG_RECORD` (the
    /// worker keeps going; its late result is ignored). `false`: only
    /// [`Transport::extend_lease`] is called.
    pub dead_letter_long_records: bool,
    /// At shutdown, dead-letter a delivery still inside a worker (the engine
    /// call may still complete, so a requeue would double-process). `false`:
    /// [`Transport::release`] it like an unstarted one.
    pub dead_letter_in_worker_at_shutdown: bool,
    /// What the all-workers-stuck warning calls the records
    /// (`All N threads are stuck on long running <label>`); kept per backend
    /// so existing log searches keep matching.
    pub stuck_records_label: &'static str,
}

/// In-flight bookkeeping for one load delivery the loop is tracking.
struct InFlight {
    info: RecordInfo,
    started: Instant,
    /// Set once dead-lettered (long record) so the worker's late result is
    /// not settled a second time.
    rejected: bool,
}

/// The SIGINT / SIGTERM / SIGHUP streams; any one starts a graceful shutdown.
pub struct ShutdownSignals {
    sigint: Signal,
    sigterm: Signal,
    sighup: Signal,
}

impl ShutdownSignals {
    /// Installs the handlers. Must be called inside the tokio runtime.
    pub fn install() -> Result<Self> {
        let install = |kind: SignalKind, name: &str| {
            signal(kind).with_context(|| format!("failed to install {name} handler"))
        };
        Ok(Self {
            sigint: install(SignalKind::interrupt(), "SIGINT")?,
            sigterm: install(SignalKind::terminate(), "SIGTERM")?,
            sighup: install(SignalKind::hangup(), "SIGHUP")?,
        })
    }

    /// Waits for the next signal and returns its name. Cancel-safe.
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            biased;
            _ = self.sigint.recv() => "SIGINT",
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sighup.recv() => "SIGHUP",
        }
    }
}

/// Last known queue depth + the mode-transition log hysteresis.
#[derive(Default)]
struct DepthProbe {
    depth: Option<u32>,
    was_empty: Option<bool>,
}

impl DepthProbe {
    /// Records a probe result; logs only on an empty <-> non-empty change.
    /// `None` (probe failed) keeps the previous depth.
    fn observe(&mut self, depth: Option<u32>) {
        let Some(depth) = depth else {
            return;
        };
        let empty = depth == 0;
        if self.was_empty != Some(empty) {
            if empty {
                tracing::info!(
                    "MQ drained (depth 0): load-preferring workers fall \
                     into redo (full-capacity drain)"
                );
            } else {
                tracing::info!("MQ active (depth {depth}): load-preferred scheduling");
            }
            self.was_empty = Some(empty);
        }
        self.depth = Some(depth);
    }
}

/// Runs the queue-mode driver until a shutdown signal, a fatal engine error or
/// the end of the delivery stream.
///
/// `connect` is awaited only AFTER the engine pool is spawned (a connect
/// failure stops the already-spawned workers/fetcher via `RUNNING`).
pub async fn run<T, C>(
    config: &Config,
    env: Arc<SzEnvironmentCore>,
    connect: C,
    policy: Policy,
) -> Result<RunOutcome>
where
    T: Transport,
    C: Future<Output = Result<T>>,
{
    let result = run_inner(config, env, connect, policy).await;
    if result.is_err() {
        // A startup error (e.g. connect failure) must also stop the
        // already-spawned fetcher/workers; process exit reclaims the rest.
        RUNNING.store(false, Ordering::Relaxed);
    }
    result
}

async fn run_inner<T, C>(
    config: &Config,
    env: Arc<SzEnvironmentCore>,
    connect: C,
    policy: Policy,
) -> Result<RunOutcome>
where
    T: Transport,
    C: Future<Output = Result<T>>,
{
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    let EnginePool {
        work_tx,
        mut result_rx,
        started,
        shutdown_notify,
        redo_in_flight,
        threads: engine_threads,
    } = spawn_engine_pool(config, &env)?;

    let transport = connect.await?;
    let mut signals = ShutdownSignals::install()?;
    // Stats thread (blocking get_stats only). The redo backlog is NOT polled:
    // count_redo_records() is a full SYS_EVAL_QUEUE scan (see stats.rs).
    let StatsThread {
        req_tx: stats_req_tx,
        resp_rx: mut stats_resp_rx,
        handle: stats_handle,
    } = stats::spawn_stats_thread(&env)?;

    let long_record = Duration::from_secs(config.long_record_secs);
    let mut monitor = monitor_interval(config.long_record_secs);
    // Diagnostic depth probe (NOT a correctness poll — push consumers detect
    // refill instantly, §2.2).
    let mut depth_tick = tokio::time::interval(Duration::from_secs(config.mq_recheck_secs.max(1)));
    depth_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut probe = DepthProbe::default();
    let mut status = StatusTicker::new(config.redo_percent, threads - redo_pref, redo_pref);
    let mut throughput = ThroughputTicker::default();
    let mut s = Session {
        transport,
        policy,
        in_flight: HashMap::new(),
        processed: 0,
        fatal: None,
    };

    // --- Running -------------------------------------------------------------
    loop {
        let stop = tokio::select! {
            biased;

            name = signals.recv() => {
                tracing::info!("{name} received, shutting down gracefully");
                true
            }
            // Wakeup optimization for fatal errors; the durable signal is the
            // Fatal Outcome on the result channel (consumer FIX-3).
            _ = shutdown_notify.notified() => {
                tracing::error!("worker/fetcher signalled fatal shutdown");
                s.fail(WORKER_FATAL_MSG.to_string());
                true
            }
            maybe_outcome = result_rx.recv() => match maybe_outcome {
                Some(outcome) => {
                    let before = s.processed;
                    let fatal = s.settle(outcome).await;
                    throughput.report(before, s.processed);
                    fatal
                }
                // All workers (and the fetcher) exited.
                None => true,
            },
            _ = monitor.tick() => {
                let _ = stats_req_tx.send(());
                s.monitor_long_records(long_record, threads).await;
                if config.redo_percent > 0 {
                    monitor_redo_in_flight(&redo_in_flight, config.long_record_secs, redo_pref);
                }
                false
            }
            Some(payload) = stats_resp_rx.recv() => {
                status.on_payload(&payload, probe.depth);
                false
            }
            _ = depth_tick.tick() => {
                probe.observe(s.transport.depth().await);
                false
            }
            delivery = s.transport.recv() => {
                s.on_delivery(delivery, &work_tx, &shutdown_notify).await
            }
        };
        if stop {
            break;
        }
    }

    // --- Stopping: one deadline bounds the drain AND the join ------------------
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    s.transport.stop_intake();
    // Stop the redo fetcher (it drops the redo sender on exit) and let idle
    // workers observe end-of-stream on both channels.
    RUNNING.store(false, Ordering::Relaxed);
    drop(work_tx);
    while !s.in_flight.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, result_rx.recv()).await {
            Ok(Some(outcome)) => {
                s.settle(outcome).await;
            }
            Ok(None) | Err(_) => break,
        }
    }
    tracing::info!("drain window elapsed; finalizing shutdown");

    // Bounded join over workers, the redo fetcher AND the stats thread (all
    // hold engine handles; destroying the environment under any of them is a
    // use-after-free). Dropping both stats channel ends unblocks the thread
    // whether it waits on a request or on handing back a response.
    drop(stats_req_tx);
    drop(stats_resp_rx);
    let all_workers_joined = engine_threads
        .join_bounded(deadline, Some(stats_handle))
        .await;

    let started_snapshot: HashSet<u64> = started
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    s.settle_remainder(&started_snapshot).await;

    let Session {
        transport,
        processed,
        fatal,
        ..
    } = s;
    transport.close().await;
    stats::print_final_totals(processed);

    Ok(RunOutcome {
        all_workers_joined,
        fatal,
    })
}

/// Mutable state of one run: the transport plus everything settled through it.
struct Session<T> {
    transport: T,
    policy: Policy,
    in_flight: HashMap<u64, InFlight>,
    /// Successful adds (acks) for the final total and the throughput line;
    /// dead-lettered deliveries are counted in `ADDS_REJECTED` instead.
    processed: u64,
    fatal: Option<String>,
}

impl<T: Transport> Session<T> {
    /// Records the first fatal reason (later ones are logged by their callers).
    fn fail(&mut self, msg: String) {
        if self.fatal.is_none() {
            self.fatal = Some(msg);
        }
    }

    /// Handles one `recv` result; returns `true` when the loop must stop.
    async fn on_delivery(
        &mut self,
        delivery: Option<Result<Delivery>>,
        work_tx: &mpsc::Sender<LoadItem>,
        shutdown_notify: &Notify,
    ) -> bool {
        let delivery = match delivery {
            Some(Ok(d)) => d,
            Some(Err(e)) => {
                self.fail(format!("{e}"));
                return true;
            }
            None => {
                tracing::info!("consumer stream ended");
                return true;
            }
        };
        let info = match parse_record(&delivery.body) {
            Ok(info) => info,
            Err(e) => {
                self.dead_letter_poison(&delivery, &e).await;
                return false;
            }
        };
        let tag = delivery.tag;
        self.in_flight.insert(
            tag,
            InFlight {
                info: info.clone(),
                started: Instant::now(),
                rejected: false,
            },
        );
        let item = LoadItem {
            delivery_tag: tag,
            body: delivery.body,
            info,
        };
        // Backpressured send, cancellable on a fatal wakeup so a dead worker
        // pool can never wedge the loop (consumer FIX-1).
        tokio::select! {
            biased;
            _ = shutdown_notify.notified() => {
                self.fail(WORKER_FATAL_MSG.to_string());
                true
            }
            send_res = work_tx.send(item) => {
                if send_res.is_err() {
                    self.fail("worker pool closed unexpectedly".to_string());
                }
                send_res.is_err()
            }
        }
    }

    /// Applies a worker outcome (consumer parity, plus the global add counters
    /// for the status line). Returns `true` if it was fatal.
    async fn settle(&mut self, outcome: Outcome) -> bool {
        let Outcome {
            delivery_tag,
            info,
            action,
        } = outcome;
        // A delivery the long-record monitor already dead-lettered: its late
        // result is not settled again.
        let already_rejected = self
            .in_flight
            .remove(&delivery_tag)
            .is_some_and(|f| f.rejected);
        match action {
            Action::Ack(_) if already_rejected => false,
            Action::Ack(maybe_info) => {
                if let Some(resp) = maybe_info {
                    println!("{resp}");
                }
                self.transport.ack(delivery_tag).await;
                self.processed += 1;
                ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
                false
            }
            Action::RejectNoRequeue(_) if already_rejected => false,
            Action::RejectNoRequeue(reason) => {
                self.reject(delivery_tag, &info, DeadLetterReason::Rejected(reason))
                    .await;
                false
            }
            Action::Fatal(msg) => {
                tracing::error!(
                    "fatal engine error on {} : {} -> {msg}",
                    info.data_source,
                    info.record_id
                );
                // Left unsettled so the broker redelivers it after exit.
                self.fail(msg);
                true
            }
        }
    }

    /// Long-record monitoring (consumer parity): deliveries past `LONG_RECORD`
    /// are logged (and offered a lease extension); with
    /// [`Policy::dead_letter_long_records`], those past `2 * LONG_RECORD` are
    /// dead-lettered while the uninterruptible engine call keeps running.
    async fn monitor_long_records(&mut self, long_record: Duration, max_workers: usize) {
        let now = Instant::now();
        let mut long: Vec<(u64, Duration)> = Vec::new();
        for (tag, f) in &self.in_flight {
            let duration = now.duration_since(f.started);
            if duration > long_record {
                long.push((*tag, duration));
                tracing::info!(
                    "Still processing ({:.3} min, rejected: {}): {} : {}",
                    duration.as_secs_f64() / 60.0,
                    f.rejected,
                    f.info.data_source,
                    f.info.record_id
                );
            }
        }
        // Every delivery past LONG_RECORD counts, not only those touched this
        // tick.
        if long.len() >= max_workers {
            println!(
                "All {max_workers} threads are stuck on long running {}",
                self.policy.stuck_records_label
            );
        }
        for (tag, duration) in long {
            self.transport.extend_lease(tag, duration).await;
            if !self.policy.dead_letter_long_records || duration <= long_record * 2 {
                continue;
            }
            if let Some(f) = self.in_flight.get_mut(&tag).filter(|f| !f.rejected) {
                f.rejected = true;
                let info = f.info.clone();
                self.reject(tag, &info, DeadLetterReason::LongRecord).await;
            }
        }
    }

    /// Settles every delivery still unsettled after the join — design §6:
    /// (a) in-flight-in-worker: the engine call may still complete in the
    ///     background (the batch-deadline "bookmark" pattern), so per
    ///     [`Policy::dead_letter_in_worker_at_shutdown`] it is dead-lettered
    ///     rather than requeued (a requeue risks double-processing);
    /// (b) queued-but-unstarted (never dispatched): released, nothing lost.
    ///
    /// A worker could in principle pick an item up between the `started`
    /// snapshot and the connection close (post-grace window); the window is a
    /// few microseconds and the failure mode is a redelivery, not data loss.
    async fn settle_remainder(&mut self, started: &HashSet<u64>) {
        let remaining: Vec<(u64, InFlight)> = self.in_flight.drain().collect();
        for (tag, record) in remaining {
            if record.rejected {
                continue;
            }
            let RecordInfo {
                data_source,
                record_id,
            } = &record.info;
            if !started.contains(&tag) {
                tracing::info!(
                    "leaving queued-but-unstarted delivery unacked (broker requeues on \
                     close): {data_source} : {record_id}"
                );
                self.transport.release(tag).await;
            } else if self.policy.dead_letter_in_worker_at_shutdown {
                self.reject(tag, &record.info, DeadLetterReason::Shutdown)
                    .await;
            } else {
                tracing::warn!(
                    "releasing in-flight-in-worker delivery on shutdown (engine call may \
                     still complete in background): {data_source} : {record_id}"
                );
                self.transport.release(tag).await;
            }
        }
    }

    /// The single dead-letter path for every transport and cause: prints the
    /// one `REJECTING: DS : ID -> reason` stdout marker, counts the record
    /// as rejected and hands it to [`Transport::dead_letter`].
    async fn reject(&mut self, tag: u64, info: &RecordInfo, reason: DeadLetterReason) {
        println!("{}", reject_marker(info, &reason));
        ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
        self.transport.dead_letter(tag, reason).await;
    }

    /// The single poison-message path: an unparseable body is never handed to
    /// a worker; it is logged once (body truncated to [`POISON_EXCERPT_CHARS`])
    /// and dead-lettered like any reject, and the loop keeps going (consumer
    /// parity; see record.rs for the rationale).
    async fn dead_letter_poison(&mut self, delivery: &Delivery, err: &ParseError) {
        tracing::warn!(
            "DEAD-LETTERING malformed record: {err} [{}]",
            poison_excerpt(&delivery.body)
        );
        let reason = DeadLetterReason::Malformed(err.to_string());
        self.reject(delivery.tag, &RecordInfo::empty(), reason)
            .await;
    }
}

/// The unified reject marker (identical on every transport).
fn reject_marker(info: &RecordInfo, reason: &DeadLetterReason) -> String {
    format!(
        "REJECTING: {} : {} -> {reason}",
        info.data_source, info.record_id
    )
}

/// Characters of a poison body quoted in its warning.
const POISON_EXCERPT_CHARS: usize = 2048;

/// First [`POISON_EXCERPT_CHARS`] chars of a (lossily decoded) body, suffixed
/// `…truncated` when cut.
fn poison_excerpt(body: &[u8]) -> String {
    let raw = String::from_utf8_lossy(body);
    let excerpt: String = raw.chars().take(POISON_EXCERPT_CHARS).collect();
    if raw.len() > excerpt.len() {
        format!("{excerpt}…truncated")
    } else {
        excerpt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_probe_tracks_depth_and_keeps_it_on_probe_failure() {
        let mut p = DepthProbe::default();
        assert_eq!(p.depth, None);
        p.observe(Some(5));
        assert_eq!((p.depth, p.was_empty), (Some(5), Some(false)));
        p.observe(None);
        assert_eq!(p.depth, Some(5), "a failed probe keeps the last depth");
        p.observe(Some(0));
        assert_eq!((p.depth, p.was_empty), (Some(0), Some(true)));
        p.observe(Some(7));
        assert_eq!((p.depth, p.was_empty), (Some(7), Some(false)));
    }

    #[test]
    fn reject_marker_names_record_and_reason() {
        let info = RecordInfo {
            data_source: "DS".to_string(),
            record_id: "ID".to_string(),
        };
        let engine = DeadLetterReason::Rejected("SENZ2207|bad data source".to_string());
        assert_eq!(
            reject_marker(&info, &engine),
            "REJECTING: DS : ID -> SENZ2207|bad data source"
        );
        let poison = DeadLetterReason::Malformed("missing DATA_SOURCE".to_string());
        assert_eq!(
            reject_marker(&RecordInfo::empty(), &poison),
            "REJECTING:  :  -> malformed record: missing DATA_SOURCE"
        );
        assert!(
            DeadLetterReason::Shutdown
                .to_string()
                .starts_with("in-flight-in-worker")
        );
        assert!(
            DeadLetterReason::LongRecord
                .to_string()
                .contains("LONG_RECORD")
        );
    }

    #[test]
    fn poison_excerpt_truncates_at_2048_chars() {
        assert_eq!(poison_excerpt(b"{bad"), "{bad");
        let long = "x".repeat(3000);
        let e = poison_excerpt(long.as_bytes());
        assert!(e.ends_with("…truncated"));
        assert_eq!(e.len(), POISON_EXCERPT_CHARS + "…truncated".len());
        // Non-UTF-8 is decoded lossily, never panics.
        assert_eq!(poison_excerpt(&[0xff]), "\u{fffd}");
    }

    #[test]
    fn shutdown_signals_install_inside_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        assert!(rt.block_on(async { ShutdownSignals::install() }).is_ok());
    }
}
