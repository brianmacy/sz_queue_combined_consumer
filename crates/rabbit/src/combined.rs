//! The mixed-mode run path (redo% < 100): tokio/lapin AMQP layer + engine
//! worker pool + (at redo% > 0) the single redo-fetcher thread.
//!
//! Inherited verbatim from `sz_rabbit_consumer_rust` (see its module docs for
//! the full rationale): one lapin `Connection` + `Channel`, single async
//! consumer, `basic_qos` prefetch, acks/rejects ONLY on the async task, bounded
//! tokio channels bridging to `std::thread` engine workers, the durable-fatal
//! result-channel discipline (FIX-3), and the bounded-grace shutdown that skips
//! the native environment destroy when a worker is still inside an
//! uninterruptible engine call (FIX-2).
//!
//! Added for the combined design: worker preference classes with cross-over
//! fallback (`worker.rs`), the redo fetcher + tiny bounded redo channel
//! (`redo.rs`), the `Combined stats:` status line (`stats.rs`), the diagnostic MQ depth probe, and the
//! §6(a)/(b) shutdown split between in-flight-in-worker (DLQ) and
//! queued-but-unstarted (left unacked for broker requeue) deliveries.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::pool::{EnginePool, spawn_engine_pool};
use sz_combined_consumer_core::queue_run::{RunOutcome, install_signals, monitor_interval};
use sz_combined_consumer_core::record::{RecordInfo, parse_record};
use sz_combined_consumer_core::stats::{
    self, ADDS_PROCESSED, ADDS_REJECTED, RUNNING, StatsThread, StatusTicker, ThroughputTicker,
};
use sz_combined_consumer_core::worker::{
    Action, LoadItem, Outcome, SHUTDOWN_GRACE, monitor_redo_in_flight,
};

/// In-flight bookkeeping for one load delivery the async task is tracking.
struct InFlight {
    info: RecordInfo,
    started: Instant,
    /// Set once we have rejected this delivery to the dead-letter queue so we
    /// do not also ack it when the (now ignored) worker result arrives.
    rejected: bool,
}

/// Runs the combined driver until SIGINT/SIGTERM or a fatal engine error.
pub async fn run(config: Config, env: Arc<SzEnvironmentCore>) -> Result<RunOutcome> {
    let result = run_inner(config, env).await;
    if result.is_err() {
        // A startup error (e.g. AMQP connect failure) must also stop the
        // already-spawned fetcher/workers; process exit reclaims the rest.
        RUNNING.store(false, Ordering::Relaxed);
    }
    result
}

async fn run_inner(config: Config, env: Arc<SzEnvironmentCore>) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    let load_pref = threads - redo_pref;
    tracing::info!(
        "Threads: {threads} (load-preferring: {load_pref}, redo-preferring: {redo_pref}, \
         redo%: {}, prefetch: {})",
        config.redo_percent,
        config.prefetch
    );
    // DIAGNOSTIC: license as seen right after engine init (before any config
    // reload / reinitialize). Compare against "LICENSE AFTER REINIT" to prove
    // whether reinitialize() drops the init-JSON license -> demo recordLimit.
    match env.get_product().and_then(|p| p.get_license()) {
        Ok(lic) => tracing::info!("LICENSE AFTER INIT: {lic}"),
        Err(e) => tracing::warn!("get_license after init failed: {e}"),
    }
    // Log the engine's active-config-id vs the registered default at startup, so we
    // can see whether the first reconcile reinit is real (active != default) — keyed
    // on get_active_config_id(), the engine's true state.
    sz_combined_consumer_core::config_reload::log_startup_config(&env);
    let url = config
        .url
        .clone()
        .context("AMQP URL required for redo% < 100 (validated at startup)")?;
    let queue = config
        .queue
        .clone()
        .context("queue required for redo% < 100 (validated at startup)")?;

    // --- Engine pool: bridge channels, redo fetcher (redo% > 0), workers ----
    // Spawned BEFORE connecting (a connect failure stops them via run()).
    let EnginePool {
        work_tx,
        mut result_rx,
        started,
        shutdown_notify,
        redo_in_flight,
        threads: engine_threads,
    } = spawn_engine_pool(&config, &env)?;

    // --- Connect to RabbitMQ -------------------------------------------------
    tracing::info!("Connecting to RabbitMQ");
    let connection = Connection::connect(&url, ConnectionProperties::default())
        .await
        .context("failed to connect to RabbitMQ")?;
    let channel = connection
        .create_channel()
        .await
        .context("failed to create channel")?;

    // Passive declare: assert the queue exists; do not create it.
    channel
        .queue_declare(
            queue.as_str().into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("queue '{queue}' does not exist (passive declare)"))?;

    // prefetch = threads + 2 by default: the +2 overshoot keeps a standing
    // load_ch buffer that masks the ack round-trip so the non-blocking worker
    // dispatch never stalls per-record (design §1.2/§2.3). Not materially
    // larger: prefetched messages are invisible to sibling processes.
    channel
        .basic_qos(config.prefetch, BasicQosOptions::default())
        .await
        .context("failed to set basic_qos")?;

    let mut consumer = channel
        .basic_consume(
            queue.as_str().into(),
            crate::INSTANCE_NAME.into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("failed to start consuming")?;

    // --- Signal handling -----------------------------------------------------
    let (mut sigint, mut sigterm) = install_signals()?;

    // --- Stats thread (blocking get_stats only) ------------------------------
    // NOTE: redo backlog is NO LONGER polled here. count_redo_records() issues a
    // `COUNT(*) FROM SYS_EVAL_QUEUE` FULL TABLE SCAN; at Sayari scale (30M+ queue
    // rows) each scan cost ~300+ CPU-sec on the DB, and with one call per stats
    // interval across the whole consumer fleet it dominated DB user CPU (~25% of
    // total worker_time on the live MSSQL run). Emptiness/drain is already
    // detected by the fetcher's get_redo_record() returning empty, so the count
    // was pure monitoring cost. Removed.
    let StatsThread {
        req_tx: stats_req_tx,
        resp_rx: mut stats_resp_rx,
        handle: stats_handle,
    } = stats::spawn_stats_thread(&env)?;

    // --- Main event loop -----------------------------------------------------
    let long_record = Duration::from_secs(config.long_record_secs);
    let mut monitor = monitor_interval(config.long_record_secs);
    // Diagnostic MQ depth probe: one passive declare per interval (NOT a
    // correctness poll — the push consumer detects refill instantly, §2.2).
    let mut mq_probe = tokio::time::interval(Duration::from_secs(config.mq_recheck_secs.max(1)));
    mq_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
    let mut processed: u64 = 0;
    let mut throughput = ThroughputTicker::default();
    let mut shutting_down = false;
    let mut fatal: Option<String> = None;
    let mut shutdown_deadline: Option<Instant> = None;

    // Status-line state.
    let mut mq_depth: Option<u32> = None;
    let mut mq_was_empty: Option<bool> = None;
    let mut status = StatusTicker::new(config.redo_percent, load_pref, redo_pref);

    loop {
        tokio::select! {
            biased;

            // Signals: begin graceful shutdown.
            _ = sigint.recv(), if !shutting_down => {
                tracing::info!("SIGINT received, shutting down gracefully");
                shutting_down = true;
            }
            _ = sigterm.recv(), if !shutting_down => {
                tracing::info!("SIGTERM received, shutting down gracefully");
                shutting_down = true;
            }

            // Wakeup optimization for fatal errors; the durable signal is the
            // Fatal Outcome on the result channel (consumer FIX-3).
            _ = shutdown_notify.notified(), if !shutting_down => {
                tracing::error!("worker/fetcher signalled fatal shutdown");
                if fatal.is_none() {
                    fatal = Some("worker or fetcher thread failed fatally".to_string());
                }
                shutting_down = true;
            }

            // A worker finished a load record (or reported a fatal error).
            maybe_outcome = result_rx.recv() => {
                match maybe_outcome {
                    Some(outcome) => {
                        let before = processed;
                        if let Some(msg) = handle_outcome(&channel, &mut in_flight, outcome, &mut processed).await {
                            fatal = Some(msg);
                            shutting_down = true;
                        }
                        throughput.report(before, processed);
                    }
                    None => {
                        // All workers (and the fetcher) exited.
                        break;
                    }
                }
            }

            // Periodic monitoring: kick the stats thread, scan long records.
            _ = monitor.tick() => {
                let _ = stats_req_tx.send(());
                monitor_long_records(&channel, &mut in_flight, long_record, threads).await;
                if config.redo_percent > 0 {
                    monitor_redo_in_flight(
                        &redo_in_flight,
                        config.long_record_secs,
                        redo_pref,
                    );
                }
            }

            // Stats answer arrived: engine stats + status line.
            Some(payload) = stats_resp_rx.recv() => {
                status.on_payload(&payload, mq_depth);
            }

            // Diagnostic MQ depth probe + mode-transition logging.
            _ = mq_probe.tick(), if !shutting_down => {
                match channel
                    .queue_declare(
                        queue.as_str().into(),
                        QueueDeclareOptions { passive: true, ..QueueDeclareOptions::default() },
                        FieldTable::default(),
                    )
                    .await
                {
                    Ok(q) => {
                        let depth = q.message_count();
                        let empty = depth == 0;
                        if mq_was_empty != Some(empty) {
                            if empty {
                                tracing::info!(
                                    "MQ drained (depth 0): load-preferring workers fall \
                                     into redo (full-capacity drain)"
                                );
                            } else {
                                tracing::info!(
                                    "MQ active (depth {depth}): load-preferred scheduling"
                                );
                            }
                            mq_was_empty = Some(empty);
                        }
                        mq_depth = Some(depth);
                    }
                    Err(e) => tracing::warn!("MQ depth probe failed: {e:#}"),
                }
            }

            // Next delivery from RabbitMQ.
            delivery = consumer.next(), if !shutting_down => {
                match delivery {
                    Some(Ok(delivery)) => {
                        // Malformed record = poison message -> DLQ, keep going
                        // (consumer parity; see record.rs for the rationale).
                        let info = match parse_record(&delivery.data) {
                            Ok(info) => info,
                            Err(e) => {
                                let raw = String::from_utf8_lossy(&delivery.data);
                                let body: String = raw.chars().take(2048).collect();
                                let truncated = if raw.len() > body.len() {
                                    " (truncated)"
                                } else {
                                    ""
                                };
                                tracing::warn!(
                                    "DEAD-LETTERING malformed record: {e} [{body}{truncated}]"
                                );
                                if let Err(re) = channel
                                    .basic_reject(
                                        delivery.delivery_tag,
                                        BasicRejectOptions { requeue: false },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "basic_reject failed for malformed record {}: {re:#}",
                                        delivery.delivery_tag
                                    );
                                }
                                continue;
                            }
                        };
                        let item = LoadItem {
                            delivery_tag: delivery.delivery_tag,
                            body: delivery.data.clone(),
                            info: info.clone(),
                        };
                        in_flight.insert(delivery.delivery_tag, InFlight {
                            info,
                            started: Instant::now(),
                            rejected: false,
                        });
                        // Backpressured send, cancellable on shutdown so a dead
                        // worker pool can never wedge the loop (consumer FIX-1).
                        tokio::select! {
                            biased;
                            _ = shutdown_notify.notified() => {
                                if fatal.is_none() {
                                    fatal = Some(
                                        "worker or fetcher thread failed fatally".to_string(),
                                    );
                                }
                                shutting_down = true;
                            }
                            send_res = work_tx.send(item) => {
                                if send_res.is_err() {
                                    fatal = Some("worker pool closed unexpectedly".to_string());
                                    shutting_down = true;
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        fatal = Some(format!("AMQP consume error: {e}"));
                        shutting_down = true;
                    }
                    None => {
                        tracing::info!("consumer stream ended");
                        shutting_down = true;
                    }
                }
            }
        }

        // Once shutting down, stop accepting new work and drain the in-flight
        // set within the single shared grace window (consumer FIX-2).
        if shutting_down {
            let deadline =
                *shutdown_deadline.get_or_insert_with(|| Instant::now() + SHUTDOWN_GRACE);
            // Stop the redo fetcher (it drops the redo sender on exit) and let
            // idle workers observe end-of-stream on both channels.
            RUNNING.store(false, Ordering::Relaxed);
            drop(work_tx);
            if !in_flight.is_empty() {
                while !in_flight.is_empty() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, result_rx.recv()).await {
                        Ok(Some(outcome)) => {
                            if let Some(msg) =
                                handle_outcome(&channel, &mut in_flight, outcome, &mut processed)
                                    .await
                                && fatal.is_none()
                            {
                                fatal = Some(msg);
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            break;
        }
    }

    // --- Shutdown ------------------------------------------------------------
    tracing::info!("drain window elapsed; finalizing shutdown");
    drop(stats_req_tx);

    // Bounded join over workers AND the redo fetcher (both hold engine
    // handles; destroying the environment under either is a use-after-free).
    let join_deadline = shutdown_deadline.unwrap_or_else(|| Instant::now() + SHUTDOWN_GRACE);
    let all_workers_joined = engine_threads.join_bounded(join_deadline, None).await;
    // The stats thread only blocks on a channel recv or a short engine call;
    // detaching it is safe and it is reaped at process exit (consumer parity).
    drop(stats_handle);

    // Remaining unacked deliveries — design §6's two distinct paths:
    // (a) in-flight-in-worker after the grace -> reject(requeue=false) to the
    //     DLQ: the engine call may still complete in the background (the
    //     documented batch-deadline "bookmark" pattern), so a requeue would
    //     risk double-processing.
    // (b) queued-but-unstarted (still in load_ch, never dispatched) -> left
    //     unacked, NO reject: the broker requeues them on connection close and
    //     nothing is lost.
    // A worker could in principle pick an item up between this snapshot and
    // the connection close (post-grace window, so both drivers accept an
    // equivalent race); the window is a few microseconds and the failure mode
    // is a broker redelivery, not data loss.
    let started_snapshot: HashSet<u64> = {
        let guard = started.lock().unwrap_or_else(PoisonError::into_inner);
        guard.clone()
    };
    for (tag, mut record) in in_flight.drain() {
        if record.rejected {
            continue;
        }
        if started_snapshot.contains(&tag) {
            record.rejected = true;
            tracing::warn!(
                "REJECTING in-flight-in-worker on shutdown (engine call may still \
                 complete in background): {} : {}",
                record.info.data_source,
                record.info.record_id
            );
            let _ = channel
                .basic_reject(tag, BasicRejectOptions { requeue: false })
                .await;
        } else {
            tracing::info!(
                "leaving queued-but-unstarted delivery unacked (broker requeues on \
                 close): {} : {}",
                record.info.data_source,
                record.info.record_id
            );
        }
    }

    if let Err(e) = connection.close(0, "shutting down".into()).await {
        tracing::warn!("error closing connection: {e:#}");
    }

    // `processed` counts acks AND rejects (unlike SQS's ack-only total).
    stats::print_final_totals(processed);

    Ok(RunOutcome {
        all_workers_joined,
        fatal,
    })
}

/// Applies a worker (or monitor) outcome to the AMQP channel (consumer parity,
/// plus the global add counters for the status line).
///
/// Returns `Some(message)` if the outcome was fatal and the caller should
/// begin graceful shutdown.
async fn handle_outcome(
    channel: &lapin::Channel,
    in_flight: &mut HashMap<u64, InFlight>,
    outcome: Outcome,
    processed: &mut u64,
) -> Option<String> {
    let Outcome {
        delivery_tag,
        info,
        action,
    } = outcome;

    // If the monitor already rejected this delivery, ignore the late result.
    let already_rejected = in_flight
        .get(&delivery_tag)
        .map(|f| f.rejected)
        .unwrap_or(false);

    match action {
        Action::Ack(maybe_info) => {
            if already_rejected {
                in_flight.remove(&delivery_tag);
                return None;
            }
            if let Some(resp) = maybe_info {
                println!("{resp}");
            }
            if let Err(e) = channel
                .basic_ack(delivery_tag, BasicAckOptions::default())
                .await
            {
                tracing::error!("basic_ack failed for {delivery_tag}: {e:#}");
            }
            in_flight.remove(&delivery_tag);
            *processed += 1;
            ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
            None
        }
        Action::RejectNoRequeue => {
            if !already_rejected {
                println!(
                    "REJECTING due to bad data or timeout: {} : {}",
                    info.data_source, info.record_id
                );
                if let Err(e) = channel
                    .basic_reject(delivery_tag, BasicRejectOptions { requeue: false })
                    .await
                {
                    tracing::error!("basic_reject failed for {delivery_tag}: {e:#}");
                }
            }
            in_flight.remove(&delivery_tag);
            *processed += 1;
            ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
            None
        }
        Action::Fatal(msg) => {
            tracing::error!(
                "fatal engine error on {} : {} -> {msg}",
                info.data_source,
                info.record_id
            );
            // Leave the delivery unacked so the broker redelivers after exit.
            in_flight.remove(&delivery_tag);
            Some(msg)
        }
    }
}

/// Long-record monitoring for load deliveries (consumer parity): records past
/// `2 * LONG_RECORD` are dead-lettered; records past `LONG_RECORD` are logged.
/// Engine calls are uninterruptible, so the broker-side delivery is rejected
/// while the worker keeps running; the `rejected` flag prevents a double ack.
async fn monitor_long_records(
    channel: &lapin::Channel,
    in_flight: &mut HashMap<u64, InFlight>,
    long_record: Duration,
    max_workers: usize,
) {
    let now = Instant::now();
    let mut to_reject: Vec<u64> = Vec::new();
    let mut num_stuck: usize = 0;

    for (tag, f) in in_flight.iter() {
        let duration = now.duration_since(f.started);
        if !f.rejected && duration > long_record * 2 {
            to_reject.push(*tag);
        }
        if duration > long_record {
            num_stuck += 1;
            tracing::info!(
                "Still processing ({:.3} min, rejected: {}): {} : {}",
                duration.as_secs_f64() / 60.0,
                f.rejected,
                f.info.data_source,
                f.info.record_id
            );
        }
    }

    if num_stuck >= max_workers {
        println!("All {max_workers} threads are stuck on long running load records");
    }

    for tag in to_reject {
        if let Some(f) = in_flight.get_mut(&tag) {
            f.rejected = true;
            println!("REJECTING: {} : {}", f.info.data_source, f.info.record_id);
            if let Err(e) = channel
                .basic_reject(tag, BasicRejectOptions { requeue: false })
                .await
            {
                tracing::error!("basic_reject (long record) failed for {tag}: {e:#}");
            }
        }
    }
}
