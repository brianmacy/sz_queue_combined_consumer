//! Amazon SQS ingestion loop.
//!
//! The SQS analogue of the RabbitMQ `combined` loop. It reuses the SAME core
//! worker pool + redo fetcher + stats; only the ingestion and settle differ:
//!
//! | Concern      | RabbitMQ (combined.rs)        | SQS (here)                                   |
//! |--------------|-------------------------------|----------------------------------------------|
//! | ingest       | lapin push consumer stream    | ReceiveMessage long-poll                     |
//! | backpressure | basic_qos prefetch            | bounded in-flight count (threads + prefetch) |
//! | ack success  | basic_ack(delivery_tag)       | DeleteMessageBatch(receipt_handle)           |
//! | dead-letter  | basic_reject(requeue=false)   | SendMessage to the DLQ, then delete          |
//! | long record  | reject at 2x LONG_RECORD      | ChangeMessageVisibility heartbeat            |
//! | fatal/leave  | leave unacked -> redeliver    | don't delete -> visibility expiry            |
//! | identity     | u64 delivery tag              | synthetic u64 -> in-flight entry map         |
//!
//! ## Dead-letter queue
//! SQS has no reject verb. An explicit `DeleteMessage` removes the message for
//! good and NEVER routes it through the queue's redrive policy (redrive only
//! moves messages that were received `maxReceiveCount` times without being
//! deleted). So, exactly like `sz_sqs_consumer-v4`, a rejected record is
//! `SendMessage`d to the dead-letter queue and only then deleted from the
//! source. The DLQ is taken from `--dead-letter-queue-url`, else discovered
//! from the source queue's `RedrivePolicy` (`deadLetterTargetArn` ->
//! `GetQueueUrl`). Without either the driver refuses to start unless
//! `--allow-no-dlq` is given (bad records would be silently destroyed).
//!
//! ## Long records
//! A record still processing past `LONG_RECORD * (n + 1)` seconds has its
//! visibility extended to `(n + 2) * LONG_RECORD` so SQS does not redeliver it
//! mid-`add_record` (duplicate add). Cadence `LONG_RECORD / 2`, redoer/consumer
//! parity. SQS caps visibility at 12 h; extensions are clamped.
//!
//! Worker code is keyed by an opaque `u64` (see `worker.rs`); each received
//! message is assigned a monotonic `u64` id mapped to its [`SqsInFlight`] entry
//! (receipt handle, body, ids, start time, extension count).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    DeleteMessageBatchRequestEntry, MessageSystemAttributeName, QueueAttributeName,
};
use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::record::{RecordInfo, parse_record};
use sz_combined_consumer_core::redo::fetcher_loop;
use sz_combined_consumer_core::stats::{
    self, ADDS_PROCESSED, ADDS_REJECTED, RUNNING, StatsPayload, ThroughputTicker, stats_loop,
};
use sz_combined_consumer_core::worker::{
    Action, Class, LoadItem, LoadSide, Outcome, RedoInFlight, RedoJob, RedoSide, SHUTDOWN_GRACE,
    WorkerCtx, add_record_flags, monitor_redo_in_flight, redo_flags, worker_loop,
};

use crate::SqsParams;

/// SQS hard maximum for a message's visibility timeout (12 hours).
pub const MAX_VISIBILITY_SECS: u64 = 43_200;
/// SQS hard maximum entries per `DeleteMessageBatch`.
const DELETE_BATCH_MAX: usize = 10;
/// How often pending deletes are flushed even when the batch is not full.
const DELETE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Backoff after a `ReceiveMessage` API error.
const RECEIVE_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// A received-but-unsettled message.
pub struct SqsInFlight {
    pub receipt_handle: String,
    /// Original body, forwarded verbatim to the DLQ on reject.
    pub body: Vec<u8>,
    pub info: RecordInfo,
    pub started: Instant,
    /// Number of visibility extensions granted so far.
    pub extended: u32,
    /// FIFO source queues: the received message's group id, reused on the DLQ.
    pub message_group_id: Option<String>,
    /// SQS message id; used as the FIFO dedup id on the DLQ.
    pub message_id: Option<String>,
}

pub type InFlightMap = Arc<Mutex<HashMap<u64, SqsInFlight>>>;

/// Resolved dead-letter destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    pub url: String,
    /// `.fifo` queues require `MessageGroupId` (+ dedup id) on `SendMessage`.
    pub fifo: bool,
}

impl DeadLetter {
    pub fn from_url(url: String) -> Self {
        let fifo = url.ends_with(".fifo");
        Self { url, fifo }
    }
}

/// Outcome of a run, mirroring the RabbitMQ loop, so `main` can apply the shared
/// use-after-free exit discipline (destroy on clean join, else leak-on-exit).
pub struct RunOutcome {
    pub all_workers_joined: bool,
    pub fatal: Option<String>,
}

/// Splits an SQS queue ARN (`arn:<partition>:sqs:<region>:<account>:<name>`)
/// into `(account_id, queue_name)`. Structured parse, no endpoint guessing.
pub fn parse_queue_arn(arn: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() != 6 || parts[0] != "arn" || parts[2] != "sqs" {
        return None;
    }
    let (account, name) = (parts[4], parts[5]);
    if account.is_empty() || name.is_empty() {
        return None;
    }
    Some((account.to_string(), name.to_string()))
}

/// Extracts `deadLetterTargetArn` from a `RedrivePolicy` attribute value.
pub fn dead_letter_arn_from_redrive_policy(policy_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(policy_json).ok()?;
    v.get("deadLetterTargetArn")
        .and_then(|a| a.as_str())
        .filter(|a| !a.is_empty())
        .map(str::to_string)
}

/// Visibility heartbeat rule (sz_sqs_consumer-v4 parity): a record running
/// longer than `LONG_RECORD * (extended + 1)` is extended to
/// `(extended + 2) * LONG_RECORD`, clamped to the SQS 12 h maximum. Returns the
/// new visibility (seconds) when an extension is due.
pub fn visibility_extension(
    elapsed: Duration,
    long_record_secs: u64,
    extended: u32,
) -> Option<u64> {
    let threshold = long_record_secs.saturating_mul(u64::from(extended) + 1);
    if elapsed.as_secs() <= threshold {
        return None;
    }
    let new_vis = long_record_secs.saturating_mul(u64::from(extended) + 2);
    Some(new_vis.min(MAX_VISIBILITY_SECS))
}

/// Resolves the DLQ: explicit override, else the source queue's RedrivePolicy.
/// `Ok(None)` means neither yielded a destination.
pub async fn resolve_dead_letter(
    client: &Client,
    params: &SqsParams,
) -> Result<Option<DeadLetter>> {
    if let Some(url) = params
        .dead_letter_queue_url
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(DeadLetter::from_url(url.to_string())));
    }
    let attrs = client
        .get_queue_attributes()
        .queue_url(&params.queue_url)
        .attribute_names(QueueAttributeName::RedrivePolicy)
        .send()
        .await
        .with_context(|| format!("GetQueueAttributes(RedrivePolicy) on {}", params.queue_url))?;
    let Some(policy) = attrs
        .attributes()
        .and_then(|m| m.get(&QueueAttributeName::RedrivePolicy))
    else {
        return Ok(None);
    };
    let arn = dead_letter_arn_from_redrive_policy(policy)
        .ok_or_else(|| anyhow!("RedrivePolicy has no deadLetterTargetArn: {policy}"))?;
    let (account, name) =
        parse_queue_arn(&arn).ok_or_else(|| anyhow!("unparseable deadLetterTargetArn: {arn}"))?;
    let url = client
        .get_queue_url()
        .queue_name(&name)
        .queue_owner_aws_account_id(&account)
        .send()
        .await
        .with_context(|| format!("GetQueueUrl for dead-letter queue {arn}"))?
        .queue_url()
        .ok_or_else(|| anyhow!("GetQueueUrl returned no URL for {arn}"))?
        .to_string();
    Ok(Some(DeadLetter::from_url(url)))
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
         redo%: {}, queue: {}, prefetch: {})",
        config.redo_percent, params.queue_url, params.prefetch
    );

    // --- SQS client + dead-letter resolution (fail fast, before any engine work)
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Arc::new(Client::new(&aws_cfg));
    let dead_letter = match resolve_dead_letter(&client, params).await? {
        Some(dl) => {
            println!("DeadLetter: {}", dl.url);
            Some(Arc::new(dl))
        }
        None if params.allow_no_dlq => {
            warn!(
                "NO dead-letter queue: {} has no RedrivePolicy and --dead-letter-queue-url is \
                 unset. --allow-no-dlq given: rejected records (bad data / retry timeout) will \
                 be DELETED and lost. Only the application log will name them.",
                params.queue_url
            );
            None
        }
        None => {
            return Err(anyhow!(
                "no dead-letter queue: {} has no RedrivePolicy and --dead-letter-queue-url \
                 (SENZING_SQS_DEAD_LETTER_QUEUE_URL) is unset. Rejected records would be \
                 silently destroyed. Attach a redrive policy, pass --dead-letter-queue-url, \
                 or pass --allow-no-dlq to accept the loss.",
                params.queue_url
            ));
        }
    };

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

    // --- Stats thread (blocking get_stats only; shared core impl) ------------
    let (stats_req_tx, stats_req_rx) = std::sync::mpsc::channel::<()>();
    let (stats_resp_tx, mut stats_resp_rx) = mpsc::channel::<StatsPayload>(1);
    let stats_env = env.clone();
    let stats_handle = std::thread::Builder::new()
        .name("sz-stats".to_string())
        .spawn(move || stats_loop(stats_env, stats_req_rx, stats_resp_tx))
        .context("failed to spawn stats thread")?;

    // Synthetic id -> in-flight entry for received-but-not-settled messages.
    let in_flight: InFlightMap = Arc::new(Mutex::new(HashMap::new()));

    // --- Poller task: ReceiveMessage -> parse -> work channel ----------------
    let poller = {
        let client = client.clone();
        let in_flight = in_flight.clone();
        let notify = shutdown_notify.clone();
        let dead_letter = dead_letter.clone();
        let p = PollParams {
            queue_url: params.queue_url.clone(),
            visibility_timeout: params.visibility_timeout,
            wait_time: params.wait_time,
            max_messages: params.max_messages,
            // Overshoot so workers never idle waiting on a receive (v4 parity).
            cap: threads + params.prefetch,
        };
        tokio::spawn(async move {
            poll_loop(client, p, dead_letter, work_tx, in_flight, notify).await;
        })
    };

    // --- Main loop: signals + outcomes + monitor + stats + delete flush ------
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("failed to install SIGINT handler")?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;

    let monitor_interval = Duration::from_secs(config.long_record_secs.max(2) / 2);
    let mut monitor = tokio::time::interval(monitor_interval);
    monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut flush_tick = tokio::time::interval(DELETE_FLUSH_INTERVAL);
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut deletes = DeleteBatcher::default();
    let mut throughput = ThroughputTicker::default();
    let mut processed: u64 = 0;
    let mut fatal: Option<String> = None;
    let mut shutting_down = false;
    let mut queue_depth: Option<u32> = None;
    let mut last_status_at = Instant::now();
    let mut prev_adds: usize = 0;
    let mut prev_redos: usize = 0;

    loop {
        tokio::select! {
            biased;

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
                        let before = processed;
                        let settled = handle_outcome(
                            &client, dead_letter.as_deref(), &in_flight,
                            &mut deletes, outcome,
                        ).await;
                        match settled {
                            Settled::Added => processed += 1,
                            Settled::Rejected => {}
                            Settled::Fatal(msg) => {
                                if fatal.is_none() {
                                    fatal = Some(msg);
                                }
                                shutting_down = true;
                                RUNNING.store(false, Ordering::Relaxed);
                                shutdown_notify.notify_waiters();
                            }
                        }
                        throughput.report(before, processed);
                        if deletes.len() >= DELETE_BATCH_MAX {
                            deletes.flush(&client, &params.queue_url).await;
                        }
                    }
                    // All worker + fetcher result senders dropped -> everyone
                    // finished. Only happens after RUNNING=false stops the poller.
                    None => break,
                }
            }
            _ = flush_tick.tick() => {
                deletes.flush(&client, &params.queue_url).await;
            }
            _ = monitor.tick() => {
                let _ = stats_req_tx.send(());
                extend_long_records(
                    &client, &params.queue_url, &in_flight, config.long_record_secs, threads,
                ).await;
                if config.redo_percent > 0 {
                    monitor_redo_in_flight(&redo_in_flight, config.long_record_secs, redo_pref);
                }
                queue_depth = approximate_depth(&client, &params.queue_url).await;
            }
            Some(payload) = stats_resp_rx.recv() => {
                if let Some(engine_stats) = &payload.engine_stats {
                    // The prefix is MANDATORY: the harness scrapes on "Engine stats:".
                    println!("Engine stats: {engine_stats}");
                }
                let now = Instant::now();
                let dt = now.duration_since(last_status_at).as_secs_f64().max(0.001);
                let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
                let redos = stats::REDOS_PROCESSED.load(Ordering::Relaxed);
                stats::emit_status_line(&stats::StatusLine {
                    redo_percent: config.redo_percent,
                    load_pref,
                    redo_pref,
                    adds,
                    adds_rate: (adds - prev_adds) as f64 / dt,
                    redos,
                    redos_rate: (redos - prev_redos) as f64 / dt,
                    mq_depth: queue_depth,
                    redo_backlog: payload.redo_backlog,
                    redo_backlog_slope: None,
                });
                prev_adds = adds;
                prev_redos = redos;
                last_status_at = now;
            }
        }
    }

    // --- Shutdown: poller already stopping; bounded worker join --------------
    let _ = poller.await;
    drop(stats_req_tx);
    workers.push(stats_handle);
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

    // Settled adds whose delete is still pending must not be redelivered.
    deletes.flush(&client, &params.queue_url).await;

    // Messages received but never settled are left un-deleted; SQS redelivers
    // them after the visibility timeout expires (at-least-once). Name them
    // (v4 parity) so an operator can correlate a later duplicate.
    {
        let now = Instant::now();
        let map = in_flight.lock().unwrap_or_else(PoisonError::into_inner);
        for f in map.values() {
            println!(
                "Still processing ({:.1} min): {} : {}",
                now.duration_since(f.started).as_secs_f64() / 60.0,
                f.info.data_source,
                f.info.record_id
            );
        }
    }

    println!(
        "Processed total of {} adds, {} redo records ({} redo dropped, {} errors)",
        ADDS_PROCESSED.load(Ordering::Relaxed),
        stats::REDOS_PROCESSED.load(Ordering::Relaxed),
        stats::REDOS_DROPPED.load(Ordering::Relaxed),
        stats::ERRORS.load(Ordering::Relaxed),
    );

    Ok(RunOutcome {
        all_workers_joined,
        fatal,
    })
}

/// What `handle_outcome` did with a delivery.
enum Settled {
    Added,
    Rejected,
    Fatal(String),
}

/// Applies a worker outcome to SQS. Ack -> queue a delete; Reject -> forward to
/// the DLQ then queue a delete; Fatal -> leave un-deleted so SQS redelivers it.
async fn handle_outcome(
    client: &Client,
    dead_letter: Option<&DeadLetter>,
    in_flight: &InFlightMap,
    deletes: &mut DeleteBatcher,
    outcome: Outcome,
) -> Settled {
    let Outcome {
        delivery_tag,
        info,
        action,
    } = outcome;
    let entry = in_flight
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&delivery_tag);
    match action {
        Action::Ack(maybe_info) => {
            if let Some(resp) = maybe_info {
                println!("{resp}");
            }
            ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
            if let Some(f) = entry {
                deletes.push(delivery_tag, f.receipt_handle);
            }
            Settled::Added
        }
        Action::RejectNoRequeue => {
            ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
            match entry {
                Some(f) => {
                    // A failed DLQ send leaves the source message for
                    // redelivery; either way this delivery is settled here.
                    dead_letter_and_delete(client, dead_letter, &f, deletes, delivery_tag).await;
                    Settled::Rejected
                }
                None => {
                    warn!(
                        "no in-flight entry for rejected SQS message {delivery_tag} ({} : {}); \
                         cannot dead-letter",
                        info.data_source, info.record_id
                    );
                    Settled::Rejected
                }
            }
        }
        Action::Fatal(msg) => {
            error!("fatal engine error on SQS message {delivery_tag}: {msg}");
            // Entry already removed WITHOUT a delete: the visibility timeout
            // redelivers the record after this process exits.
            Settled::Fatal(msg)
        }
    }
}

/// Forwards a rejected message to the DLQ (v4 parity: `Sending to deadletter`)
/// and, only if that succeeded (or no DLQ is configured), queues the source
/// delete. On a failed `SendMessage` the source message is left alone so the
/// visibility timeout redelivers it — never delete what was not preserved.
async fn dead_letter_and_delete(
    client: &Client,
    dead_letter: Option<&DeadLetter>,
    f: &SqsInFlight,
    deletes: &mut DeleteBatcher,
    id: u64,
) {
    match dead_letter {
        Some(dl) => {
            println!(
                "Sending to deadletter: {} : {}",
                f.info.data_source, f.info.record_id
            );
            match send_to_dead_letter(client, dl, f).await {
                Ok(()) => deletes.push(id, f.receipt_handle.clone()),
                Err(e) => error!(
                    "SendMessage to DLQ {} failed for {} : {}: {e:#}; leaving the source \
                     message for redelivery",
                    dl.url, f.info.data_source, f.info.record_id
                ),
            }
        }
        None => {
            warn!(
                "REJECTING (no DLQ, --allow-no-dlq): deleting {} : {}; body follows: {}",
                f.info.data_source,
                f.info.record_id,
                String::from_utf8_lossy(&f.body)
            );
            deletes.push(id, f.receipt_handle.clone());
        }
    }
}

async fn send_to_dead_letter(client: &Client, dl: &DeadLetter, f: &SqsInFlight) -> Result<()> {
    let body = std::str::from_utf8(&f.body).context("non-UTF-8 body cannot be forwarded to SQS")?;
    let mut req = client.send_message().queue_url(&dl.url).message_body(body);
    if dl.fifo {
        let group = f
            .message_group_id
            .clone()
            .unwrap_or_else(|| f.info.data_source.clone());
        let dedup = f
            .message_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", f.info.data_source, f.info.record_id));
        req = req.message_group_id(group).message_deduplication_id(dedup);
    }
    req.send().await.map(|_| ()).map_err(anyhow::Error::from)
}

/// Accumulates receipt handles and deletes them in batches of up to 10.
#[derive(Default)]
struct DeleteBatcher {
    pending: Vec<(u64, String)>,
}

impl DeleteBatcher {
    fn push(&mut self, id: u64, receipt_handle: String) {
        self.pending.push((id, receipt_handle));
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    /// Issues `DeleteMessageBatch` for everything pending. A failed entry is
    /// logged (the message redelivers after its visibility timeout, and
    /// `add_record` is idempotent), never retried here.
    async fn flush(&mut self, client: &Client, queue_url: &str) {
        while !self.pending.is_empty() {
            let take = self.pending.len().min(DELETE_BATCH_MAX);
            let chunk: Vec<(u64, String)> = self.pending.drain(..take).collect();
            let entries: Vec<DeleteMessageBatchRequestEntry> = chunk
                .iter()
                .filter_map(|(id, handle)| {
                    DeleteMessageBatchRequestEntry::builder()
                        .id(id.to_string())
                        .receipt_handle(handle)
                        .build()
                        .ok()
                })
                .collect();
            match client
                .delete_message_batch()
                .queue_url(queue_url)
                .set_entries(Some(entries))
                .send()
                .await
            {
                Ok(resp) => {
                    for failed in resp.failed() {
                        warn!(
                            "SQS DeleteMessageBatch entry {} failed: {} ({}); message will \
                             redeliver after its visibility timeout",
                            failed.id(),
                            failed.code(),
                            failed.message().unwrap_or("")
                        );
                    }
                }
                Err(e) => warn!(
                    "SQS DeleteMessageBatch of {} message(s) failed: {e}; they will redeliver \
                     after their visibility timeout",
                    chunk.len()
                ),
            }
        }
    }
}

/// Visibility heartbeat + long-record report (v4 parity). Extends every record
/// past its current threshold and prints the all-stuck warning.
async fn extend_long_records(
    client: &Client,
    queue_url: &str,
    in_flight: &InFlightMap,
    long_record_secs: u64,
    max_workers: usize,
) {
    let now = Instant::now();
    // Decide under the lock, call the API outside it.
    let mut to_extend: Vec<(u64, String, u64, u32, RecordInfo, Duration)> = Vec::new();
    let mut num_stuck = 0usize;
    {
        let mut map = in_flight.lock().unwrap_or_else(PoisonError::into_inner);
        for (id, f) in map.iter_mut() {
            let elapsed = now.duration_since(f.started);
            if let Some(new_vis) = visibility_extension(elapsed, long_record_secs, f.extended) {
                num_stuck += 1;
                f.extended += 1;
                to_extend.push((
                    *id,
                    f.receipt_handle.clone(),
                    new_vis,
                    f.extended,
                    f.info.clone(),
                    elapsed,
                ));
            }
        }
    }
    for (id, handle, new_vis, times, info, elapsed) in to_extend {
        match client
            .change_message_visibility()
            .queue_url(queue_url)
            .receipt_handle(&handle)
            .visibility_timeout(new_vis as i32)
            .send()
            .await
        {
            Ok(_) => println!(
                "Extended visibility ({:.1} min, extended {times} times): {} : {}",
                elapsed.as_secs_f64() / 60.0,
                info.data_source,
                info.record_id
            ),
            Err(e) => warn!(
                "ChangeMessageVisibility failed for {id} ({} : {}): {e}; SQS may redeliver \
                 this in-progress record",
                info.data_source, info.record_id
            ),
        }
    }
    if num_stuck >= max_workers {
        println!("All {max_workers} threads are stuck on long running records");
    }
}

/// Diagnostic `ApproximateNumberOfMessages` for the status line (one cheap
/// attribute call per monitor tick; not a correctness poll).
async fn approximate_depth(client: &Client, queue_url: &str) -> Option<u32> {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .ok()?;
    resp.attributes()?
        .get(&QueueAttributeName::ApproximateNumberOfMessages)?
        .parse()
        .ok()
}

struct PollParams {
    queue_url: String,
    visibility_timeout: i32,
    wait_time: i32,
    max_messages: i32,
    cap: usize,
}

/// Long-polls SQS and feeds parsed records to the worker pool, bounded by
/// `cap` outstanding messages. Aborts promptly on `notify` (shutdown) so
/// `work_tx` is dropped and idle workers observe end-of-stream. Unparseable
/// messages are dead-lettered (DLQ send, then delete) immediately.
async fn poll_loop(
    client: Arc<Client>,
    p: PollParams,
    dead_letter: Option<Arc<DeadLetter>>,
    work_tx: mpsc::Sender<LoadItem>,
    in_flight: InFlightMap,
    notify: Arc<Notify>,
) {
    let mut next_id: u64 = 1;
    let mut poison_deletes = DeleteBatcher::default();
    while RUNNING.load(Ordering::Relaxed) {
        // Backpressure: cap outstanding received-but-unsettled messages so we do
        // not let their visibility timers run down while queued.
        let outstanding = in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if outstanding >= p.cap {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = notify.notified() => break,
            }
            continue;
        }
        // Never receive more than we have room for (v4 parity).
        let room = (p.cap - outstanding).min(p.max_messages as usize).max(1) as i32;

        let recv = client
            .receive_message()
            .queue_url(&p.queue_url)
            .max_number_of_messages(room)
            .wait_time_seconds(p.wait_time)
            .visibility_timeout(p.visibility_timeout)
            .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
            .send();
        let resp = tokio::select! {
            r = recv => r,
            _ = notify.notified() => break,
        };
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                warn!("SQS ReceiveMessage error: {e}; retrying");
                tokio::time::sleep(RECEIVE_ERROR_BACKOFF).await;
                continue;
            }
        };

        for m in resp.messages() {
            let (Some(body), Some(handle)) = (m.body(), m.receipt_handle()) else {
                continue;
            };
            let id = next_id;
            next_id += 1;
            let message_group_id = m
                .attributes()
                .and_then(|a| a.get(&MessageSystemAttributeName::MessageGroupId))
                .cloned();
            match parse_record(body.as_bytes()) {
                Ok(info) => {
                    in_flight
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(
                            id,
                            SqsInFlight {
                                receipt_handle: handle.to_string(),
                                body: body.as_bytes().to_vec(),
                                info: info.clone(),
                                started: Instant::now(),
                                extended: 0,
                                message_group_id,
                                message_id: m.message_id().map(str::to_string),
                            },
                        );
                    let item = LoadItem {
                        delivery_tag: id,
                        body: body.as_bytes().to_vec(),
                        info,
                    };
                    if work_tx.send(item).await.is_err() {
                        // Worker pool gone (fatal) -> stop; work_tx drops on return.
                        in_flight
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&id);
                        poison_deletes.flush(&client, &p.queue_url).await;
                        return;
                    }
                }
                Err(e) => {
                    // Poison message: cannot even name DATA_SOURCE/RECORD_ID.
                    warn!("REJECTING unparseable SQS message {id}: {e}");
                    ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
                    let f = SqsInFlight {
                        receipt_handle: handle.to_string(),
                        body: body.as_bytes().to_vec(),
                        info: RecordInfo::empty(),
                        started: Instant::now(),
                        extended: 0,
                        message_group_id,
                        message_id: m.message_id().map(str::to_string),
                    };
                    dead_letter_and_delete(
                        &client,
                        dead_letter.as_deref(),
                        &f,
                        &mut poison_deletes,
                        id,
                    )
                    .await;
                }
            }
        }
        poison_deletes.flush(&client, &p.queue_url).await;
    }
    poison_deletes.flush(&client, &p.queue_url).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_and_partitioned_queue_arns() {
        assert_eq!(
            parse_queue_arn("arn:aws:sqs:us-east-1:123456789012:my-dlq"),
            Some(("123456789012".to_string(), "my-dlq".to_string()))
        );
        assert_eq!(
            parse_queue_arn("arn:aws-cn:sqs:cn-north-1:000000000000:q.fifo"),
            Some(("000000000000".to_string(), "q.fifo".to_string()))
        );
        assert_eq!(parse_queue_arn("arn:aws:sns:us-east-1:1:topic"), None);
        assert_eq!(parse_queue_arn("arn:aws:sqs:us-east-1::noaccount"), None);
        assert_eq!(parse_queue_arn("garbage"), None);
    }

    #[test]
    fn extracts_dead_letter_arn_from_redrive_policy() {
        let policy =
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":"5"}"#;
        assert_eq!(
            dead_letter_arn_from_redrive_policy(policy).as_deref(),
            Some("arn:aws:sqs:us-east-1:1:dlq")
        );
        assert_eq!(
            dead_letter_arn_from_redrive_policy(r#"{"maxReceiveCount":"5"}"#),
            None
        );
        assert_eq!(dead_letter_arn_from_redrive_policy("not json"), None);
    }

    #[test]
    fn dead_letter_detects_fifo_by_suffix() {
        assert!(DeadLetter::from_url("https://sqs.x/1/q.fifo".into()).fifo);
        assert!(!DeadLetter::from_url("https://sqs.x/1/q".into()).fifo);
    }

    #[test]
    fn visibility_extension_matches_v4_thresholds_and_clamps() {
        let lr = 300;
        // Not yet past LONG_RECORD: nothing.
        assert_eq!(visibility_extension(Duration::from_secs(300), lr, 0), None);
        // Past 1x: extend to 2x.
        assert_eq!(
            visibility_extension(Duration::from_secs(301), lr, 0),
            Some(600)
        );
        // Already extended once: threshold is 2x, extend to 3x.
        assert_eq!(visibility_extension(Duration::from_secs(500), lr, 1), None);
        assert_eq!(
            visibility_extension(Duration::from_secs(601), lr, 1),
            Some(900)
        );
        // Clamped to the SQS 12 h ceiling.
        assert_eq!(
            visibility_extension(Duration::from_secs(u64::MAX / 4), 40_000, 5),
            Some(MAX_VISIBILITY_SECS)
        );
    }
}
