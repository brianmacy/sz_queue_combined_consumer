//! Amazon SQS ingestion loop (standard queues).
//!
//! The SQS analogue of the RabbitMQ `combined` loop. It reuses the SAME core
//! worker pool + redo fetcher + stats; only the ingestion and settle differ:
//!
//! | Concern      | RabbitMQ (combined.rs)        | SQS (here)                          |
//! |--------------|-------------------------------|-------------------------------------|
//! | ingest       | lapin push consumer stream    | ReceiveMessage long-poll            |
//! | backpressure | basic_qos prefetch            | bounded in-flight count (`cap`)      |
//! | ack success  | basic_ack(delivery_tag)       | DeleteMessage(receipt_handle)        |
//! | dead-letter  | basic_reject(requeue=false)   | DeleteMessage (drop; redrive = DLQ)  |
//! | fatal/leave  | leave unacked -> redeliver    | don't delete -> visibility expiry    |
//! | identity     | u64 delivery tag              | synthetic u64 -> receipt-handle map  |
//!
//! Worker code is keyed by an opaque `u64` (see `worker.rs`); SQS receipt-handle
//! strings are opaque and variable-length, so each received message is assigned a
//! monotonic `u64` id mapped to its handle in `in_flight`. On settle we look the
//! handle back up. This is the same indirection the redo in-flight map uses.
//!
//! NOTE (v1): the SQS visibility timeout MUST exceed the worst-case record
//! processing time or SQS will redeliver an in-progress record (duplicate add).
//! The extended status-line observability (EWMA slope, floor guard, MQ-depth
//! probe) present in the RabbitMQ loop is not yet ported here.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use aws_sdk_sqs::Client;
use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::record::parse_record;
use sz_combined_consumer_core::redo::fetcher_loop;
use sz_combined_consumer_core::stats::{ADDS_PROCESSED, ADDS_REJECTED, RUNNING};
use sz_combined_consumer_core::worker::{
    Action, Class, LoadItem, LoadSide, Outcome, RedoInFlight, RedoJob, RedoSide, SHUTDOWN_GRACE,
    WorkerCtx, add_record_flags, redo_flags, worker_loop,
};

use crate::SqsParams;

/// Outcome of a run, mirroring the RabbitMQ loop, so `main` can apply the shared
/// use-after-free exit discipline (destroy on clean join, else leak-on-exit).
pub struct RunOutcome {
    pub all_workers_joined: bool,
    pub fatal: Option<String>,
}

/// Runs the SQS combined driver until SIGINT/SIGTERM or a fatal engine error.
pub async fn run(
    config: &Config,
    params: &SqsParams,
    env: Arc<SzEnvironmentCore>,
) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    let load_pref = threads - redo_pref;
    info!(
        "SQS threads: {threads} (load-preferring: {load_pref}, redo-preferring: {redo_pref}, \
         redo%: {}, queue: {})",
        config.redo_percent, params.queue_url
    );

    // --- Bridge channels (identical to the AMQP path) ------------------------
    let (work_tx, work_rx) = mpsc::channel::<LoadItem>(threads);
    let (result_tx, mut result_rx) = mpsc::channel::<Outcome>(threads * 2);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let started: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let shutdown_notify = Arc::new(Notify::new());
    let add_flags = add_record_flags(config.info);
    let rflags = redo_flags(config.info);
    let want_info = config.info;

    // --- Redo side (only when redo% > 0) — reuses the core fetcher -----------
    let redo_in_flight: Arc<Mutex<RedoInFlight>> = Arc::new(Mutex::new(HashMap::new()));
    let (redo_side, fetcher_handle) = if config.redo_percent > 0 {
        let (redo_tx, redo_rx) = std::sync::mpsc::sync_channel::<RedoJob>(redo_pref + 2);
        let redo_rx = Arc::new(Mutex::new(redo_rx));
        let fetcher_env = env.clone();
        let sleep_secs = config.redo_sleep_secs;
        let fetcher_result_tx = result_tx.clone();
        let fetcher_notify = shutdown_notify.clone();
        let handle = std::thread::Builder::new()
            .name("sz-redo-fetcher".to_string())
            .spawn(move || {
                fetcher_loop(
                    fetcher_env,
                    redo_tx,
                    sleep_secs,
                    Some(fetcher_result_tx),
                    Some(fetcher_notify),
                )
            })
            .context("failed to spawn redo fetcher thread")?;
        (
            Some(RedoSide {
                redo_rx,
                in_flight: redo_in_flight.clone(),
            }),
            Some(handle),
        )
    } else {
        (None, None)
    };

    // --- Spawn engine worker threads -----------------------------------------
    let mut workers = Vec::with_capacity(threads + 1);
    for worker_id in 0..threads {
        let class = if worker_id < redo_pref {
            Class::RedoPreferring
        } else {
            Class::LoadPreferring
        };
        let ctx = WorkerCtx {
            worker_id,
            class,
            env: env.clone(),
            load: Some(LoadSide {
                work_rx: work_rx.clone(),
                result_tx: result_tx.clone(),
                started: started.clone(),
                shutdown_notify: shutdown_notify.clone(),
            }),
            redo: redo_side.clone(),
            add_flags,
            redo_flags: rflags,
            want_info,
        };
        let handle = std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || worker_loop(ctx))
            .context("failed to spawn worker thread")?;
        workers.push(handle);
    }
    drop(result_tx);
    drop(redo_side);

    // --- SQS client (region/credentials from the standard AWS chain) ---------
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Arc::new(Client::new(&aws_cfg));

    // Synthetic id -> receipt handle for received-but-not-settled messages.
    let in_flight: Arc<Mutex<HashMap<u64, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // --- Poller task: ReceiveMessage -> parse -> work channel ----------------
    let poller = {
        let client = client.clone();
        let in_flight = in_flight.clone();
        let notify = shutdown_notify.clone();
        let queue_url = params.queue_url.clone();
        let vis = params.visibility_timeout;
        let wait = params.wait_time;
        let maxn = params.max_messages;
        let cap = threads; // max outstanding received-but-unsettled messages
        tokio::spawn(async move {
            poll_loop(
                client, queue_url, vis, wait, maxn, cap, work_tx, in_flight, notify,
            )
            .await;
        })
    };

    // --- Main loop: signals + worker outcomes (delete on settle) -------------
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("failed to install SIGINT handler")?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;
    let mut fatal: Option<String> = None;
    let mut shutting_down = false;

    loop {
        tokio::select! {
            _ = sigint.recv(), if !shutting_down => {
                info!("SIGINT received, shutting down gracefully");
                shutting_down = true;
                RUNNING.store(false, Ordering::Relaxed);
                shutdown_notify.notify_waiters();
            }
            _ = sigterm.recv(), if !shutting_down => {
                info!("SIGTERM received, shutting down gracefully");
                shutting_down = true;
                RUNNING.store(false, Ordering::Relaxed);
                shutdown_notify.notify_waiters();
            }
            maybe = result_rx.recv() => {
                match maybe {
                    Some(outcome) => {
                        if let Some(msg) =
                            handle_outcome(&client, &params.queue_url, &in_flight, outcome).await
                        {
                            if fatal.is_none() {
                                fatal = Some(msg);
                            }
                            shutting_down = true;
                            RUNNING.store(false, Ordering::Relaxed);
                            shutdown_notify.notify_waiters();
                        }
                    }
                    // All worker + fetcher result senders dropped -> everyone
                    // finished. Only happens after RUNNING=false stops the poller.
                    None => break,
                }
            }
        }
    }

    // --- Shutdown: poller already stopping; bounded worker join --------------
    let _ = poller.await;
    if let Some(handle) = fetcher_handle {
        workers.push(handle);
    }
    let join_deadline = Instant::now() + SHUTDOWN_GRACE;
    while Instant::now() < join_deadline && workers.iter().any(|h| !h.is_finished()) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let all_workers_joined = workers.iter().all(|h| h.is_finished());
    if all_workers_joined {
        for handle in workers {
            let _ = handle.join();
        }
        info!("all engine workers finished; safe to destroy environment");
    } else {
        warn!(
            "shutdown grace elapsed with workers still in engine calls; \
             skipping environment destroy to avoid use-after-free (leak-on-exit)"
        );
    }

    // Any messages received but never settled are simply left un-deleted; SQS
    // redelivers them after the visibility timeout expires (at-least-once).
    println!(
        "Processed total of {} adds, {} redo records ({} redo dropped, {} errors)",
        ADDS_PROCESSED.load(Ordering::Relaxed),
        sz_combined_consumer_core::stats::REDOS_PROCESSED.load(Ordering::Relaxed),
        sz_combined_consumer_core::stats::REDOS_DROPPED.load(Ordering::Relaxed),
        sz_combined_consumer_core::stats::ERRORS.load(Ordering::Relaxed),
    );

    Ok(RunOutcome {
        all_workers_joined,
        fatal,
    })
}

/// Applies a worker outcome to SQS. Returns `Some(msg)` on a fatal engine error
/// (caller begins shutdown). Ack/Reject both remove the record from the queue
/// (DeleteMessage); Fatal leaves it un-deleted so SQS redelivers it.
async fn handle_outcome(
    client: &Client,
    queue_url: &str,
    in_flight: &Arc<Mutex<HashMap<u64, String>>>,
    outcome: Outcome,
) -> Option<String> {
    let Outcome {
        delivery_tag,
        info: _,
        action,
    } = outcome;
    match action {
        Action::Ack(maybe_info) => {
            if let Some(resp) = maybe_info {
                println!("{resp}");
            }
            ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
            delete_message(client, queue_url, in_flight, delivery_tag).await;
            None
        }
        Action::RejectNoRequeue => {
            ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
            delete_message(client, queue_url, in_flight, delivery_tag).await;
            None
        }
        Action::Fatal(msg) => {
            error!("fatal engine error on SQS message {delivery_tag}: {msg}");
            // Drop the map entry WITHOUT deleting from SQS so the visibility
            // timeout redelivers the record after this process exits.
            in_flight
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&delivery_tag);
            Some(msg)
        }
    }
}

/// Removes id -> receipt-handle from the map and issues DeleteMessage. The lock
/// is released before the await (never held across it).
async fn delete_message(
    client: &Client,
    queue_url: &str,
    in_flight: &Arc<Mutex<HashMap<u64, String>>>,
    id: u64,
) {
    let handle = in_flight
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&id);
    if let Some(handle) = handle
        && let Err(e) = client
            .delete_message()
            .queue_url(queue_url)
            .receipt_handle(handle)
            .send()
            .await
    {
        warn!("SQS DeleteMessage failed for id {id}: {e}");
    }
}

/// Long-polls SQS and feeds parsed records to the worker pool, bounded by `cap`
/// outstanding messages. Aborts promptly on `notify` (shutdown) so `work_tx` is
/// dropped and idle workers observe end-of-stream. Bad records are deleted
/// immediately (dead-letter/drop). Exits when `RUNNING` is cleared.
#[allow(clippy::too_many_arguments)]
async fn poll_loop(
    client: Arc<Client>,
    queue_url: String,
    visibility_timeout: i32,
    wait_time: i32,
    max_messages: i32,
    cap: usize,
    work_tx: mpsc::Sender<LoadItem>,
    in_flight: Arc<Mutex<HashMap<u64, String>>>,
    notify: Arc<Notify>,
) {
    let mut next_id: u64 = 1;
    while RUNNING.load(Ordering::Relaxed) {
        // Backpressure: cap outstanding received-but-unsettled messages so we do
        // not let their visibility timers run down while queued.
        let outstanding = in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if outstanding >= cap {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = notify.notified() => break,
            }
            continue;
        }

        let recv = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_time)
            .visibility_timeout(visibility_timeout)
            .send();
        let resp = tokio::select! {
            r = recv => r,
            _ = notify.notified() => break,
        };
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                warn!("SQS ReceiveMessage error: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        for m in resp.messages() {
            let (Some(body), Some(handle)) = (m.body(), m.receipt_handle()) else {
                continue;
            };
            let id = next_id;
            next_id += 1;
            match parse_record(body.as_bytes()) {
                Ok(info) => {
                    in_flight
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(id, handle.to_string());
                    let item = LoadItem {
                        delivery_tag: id,
                        body: body.as_bytes().to_vec(),
                        info,
                    };
                    if work_tx.send(item).await.is_err() {
                        // Worker pool gone (fatal) -> stop; work_tx drops on return.
                        return;
                    }
                }
                Err(e) => {
                    warn!("dead-lettering unparseable SQS message: {e}");
                    ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
                    let _ = client
                        .delete_message()
                        .queue_url(&queue_url)
                        .receipt_handle(handle)
                        .send()
                        .await;
                }
            }
        }
    }
}
