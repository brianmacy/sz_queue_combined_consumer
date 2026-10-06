//! Amazon SQS [`Transport`] for the shared core queue loop
//! ([`queue_loop::run`]), which owns the engine worker pool, the redo fetcher
//! (redo% > 0), stats, the long-record monitor, the depth probe and the
//! bounded-grace shutdown. Only ingestion and settle are SQS-specific:
//!
//! | Concern      | RabbitMQ (combined.rs)        | SQS (here)                                   |
//! |--------------|-------------------------------|----------------------------------------------|
//! | ingest       | lapin push consumer stream    | ReceiveMessage long-poll task -> channel     |
//! | backpressure | basic_qos prefetch            | bounded in-flight count (prefetch)           |
//! | ack success  | basic_ack(delivery_tag)       | DeleteMessageBatch(receipt_handle)           |
//! | dead-letter  | basic_reject(requeue=false)   | SendMessage to the DLQ, then delete          |
//! | long record  | reject at 2x LONG_RECORD      | ChangeMessageVisibility heartbeat            |
//! | fatal/release| leave unacked -> redeliver    | don't delete -> visibility expiry            |
//! | identity     | u64 delivery tag              | synthetic u64 -> receipt-handle map          |
//!
//! ## Receiving
//! A poller task long-polls `ReceiveMessage` and feeds a bounded channel that
//! [`Transport::recv`] reads (cancel-safe): cancelling a raw `ReceiveMessage`
//! inside the loop's `select!` could orphan messages SQS already handed out.
//! The poller holds one semaphore permit per received-but-unsettled message
//! (cap = `prefetch`, the total in-flight cap; default 2 x threads, v4
//! parity) so queued messages never sit with their visibility timer running
//! down. A permit is released only once the message has left the queue
//! (`DeleteMessageBatch` returned) or been released at shutdown, so the cap
//! bounds the messages this consumer holds invisible; the waiting poller is
//! woken the moment one is released. [`RECEIVE_ERROR_MAX`] consecutive
//! `ReceiveMessage` failures are fatal (orderly shutdown, exit 255).
//!
//! ## Dead-letter queue
//! SQS has no reject verb. An explicit `DeleteMessage` removes the message for
//! good and NEVER routes it through the queue's redrive policy (redrive only
//! moves messages that were received `maxReceiveCount` times without being
//! deleted). So, exactly like `sz_sqs_consumer-v4`, a rejected record is
//! `SendMessage`d to the dead-letter queue verbatim and only then deleted from
//! the source, with the reject reason in the `SzReason` message attribute
//! (see [`SZ_REASON_ATTRIBUTE`]). The DLQ is taken from `--dead-letter-queue-url`, else
//! discovered from the source queue's `RedrivePolicy` (`deadLetterTargetArn`
//! -> `GetQueueUrl`), BEFORE anything is spawned. Without either the driver
//! refuses to start unless `--allow-no-dlq` is given (bad records would be
//! silently destroyed).
//!
//! ## Long records
//! A record still processing past `LONG_RECORD * (n + 1)` seconds has its
//! visibility extended to `(n + 2) * LONG_RECORD` so SQS does not redeliver it
//! mid-`add_record` (duplicate add); it is never dead-lettered for running
//! long (Senzing v4 SQS consumer parity). SQS caps visibility at 12 h;
//! extensions are clamped. At shutdown every unsettled message (in a worker or
//! queued for one) is left un-deleted for redelivery after its visibility
//! timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::retry::RetryConfig;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput;
use aws_sdk_sqs::types::{
    DeleteMessageBatchRequestEntry, Message, MessageAttributeValue, MessageSystemAttributeName,
    QueueAttributeName,
};
use sz_rust_sdk::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::queue_loop::{self, DeadLetterReason, Delivery, Policy, Transport};
use sz_combined_consumer_core::queue_run::RunOutcome;
use sz_combined_consumer_core::record::{RecordInfo, parse_record};

use crate::SqsParams;

/// SQS settle policy: extend visibility on long records (never dead-letter
/// them).
const SQS_POLICY: Policy = Policy {
    dead_letter_long_records: false,
    stuck_records_label: "records",
};

/// String message attribute on every DLQ copy carrying why it was rejected
/// (the engine / parse error text), so the DLQ is triageable without the log.
pub const SZ_REASON_ATTRIBUTE: &str = "SzReason";
/// Cap on the `SzReason` value, in bytes (engine errors are short; this
/// bounds a pathological one).
const SZ_REASON_MAX_BYTES: usize = 1024;
/// SQS message size limit the body AND attributes (name + type + value) must
/// fit together (the classic 256 KiB, the smallest a DLQ may be set to allow).
const SQS_MAX_MESSAGE_BYTES: usize = 262_144;

/// SQS hard maximum for a message's visibility timeout (12 hours).
pub const MAX_VISIBILITY_SECS: u64 = 43_200;
/// SQS hard maximum entries per `DeleteMessageBatch`.
const DELETE_BATCH_MAX: usize = 10;
/// Backoff after a `ReceiveMessage` API error.
const RECEIVE_ERROR_BACKOFF: Duration = Duration::from_secs(1);
/// Consecutive `ReceiveMessage` failures that make the run fatal (exit 255
/// after the orderly shutdown). With [`RECEIVE_ERROR_BACKOFF`] spacing and
/// SDK-internal retries disabled for this call, that is ~30 s of an
/// unreachable/denying SQS endpoint. Any success resets the count.
const RECEIVE_ERROR_MAX: u32 = 30;
/// Bound on one diagnostic depth probe, so a hung endpoint cannot stall the
/// loop that also handles signals.
const DEPTH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the close-time delete flush: an SQS endpoint that stops answering
/// must not hold the process past shutdown (an unflushed delete only means
/// that message redelivers after its visibility timeout).
const CLOSE_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Resolves the DLQ (or the `--allow-no-dlq` opt-out) and announces it.
async fn dead_letter_or_refuse(client: &Client, params: &SqsParams) -> Result<Option<DeadLetter>> {
    match resolve_dead_letter(client, params).await? {
        Some(dl) => {
            println!("DeadLetter: {}", dl.url);
            Ok(Some(dl))
        }
        None if params.allow_no_dlq => {
            warn!(
                "NO dead-letter queue: {} has no RedrivePolicy and --dead-letter-queue-url is \
                 unset. --allow-no-dlq given: rejected records (bad data / retry timeout) will \
                 be DELETED and lost. Only the application log will name them.",
                params.queue_url
            );
            Ok(None)
        }
        None => Err(anyhow!(
            "no dead-letter queue: {} has no RedrivePolicy and --dead-letter-queue-url \
             (SENZING_SQS_DEAD_LETTER_QUEUE_URL) is unset. Rejected records would be \
             silently destroyed. Attach a redrive policy, pass --dead-letter-queue-url, \
             or pass --allow-no-dlq to accept the loss.",
            params.queue_url
        )),
    }
}

/// Runs the SQS driver until SIGINT/SIGTERM/SIGHUP, a fatal engine error or
/// [`RECEIVE_ERROR_MAX`] consecutive receive failures.
pub async fn run(
    config: &Config,
    params: &SqsParams,
    env: Arc<SzEnvironmentCore>,
) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    info!(
        "SQS threads: {threads} (load-preferring: {}, redo-preferring: {redo_pref}, \
         redo%: {}, queue: {}, prefetch: {})",
        threads - redo_pref,
        config.redo_percent,
        params.queue_url,
        config.prefetch
    );

    // Client + DLQ resolution fail fast, before any engine thread is spawned.
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Client::new(&aws_cfg);
    let dead_letter = dead_letter_or_refuse(&client, params).await?;

    let poll = PollParams {
        queue_url: params.queue_url.clone(),
        visibility_timeout: params.visibility_timeout,
        wait_time: params.wait_time,
        max_messages: params.max_messages,
        cap: config.prefetch,
    };
    // The core loop spawns the engine pool BEFORE awaiting this, so the
    // poller only starts once the workers exist.
    let connect = async move {
        Ok(SqsTransport::start(
            client,
            poll,
            dead_letter,
            config.long_record_secs,
        ))
    };
    queue_loop::run(config, env, connect, SQS_POLICY).await
}

/// A received-but-unsettled message.
struct SqsMessage {
    receipt_handle: String,
    /// Original body, forwarded verbatim to the DLQ on reject.
    body: Vec<u8>,
    /// FIFO source queues: the received message's group id, reused on the DLQ.
    message_group_id: Option<String>,
    /// SQS message id; used as the FIFO dedup id on the DLQ.
    message_id: Option<String>,
    /// Receive time: the visibility clock started here.
    received: Instant,
    /// Number of visibility extensions granted so far.
    extended: u32,
    /// In-flight cap slot, released once the message has left the queue
    /// (deleted) or been released at shutdown.
    slot: OwnedSemaphorePermit,
}

impl SqsMessage {
    /// `DATA_SOURCE` / `RECORD_ID` for log lines (empty for a poison body).
    fn info(&self) -> RecordInfo {
        parse_record(&self.body).unwrap_or_else(|_| RecordInfo::empty())
    }
}

/// What the poller hands to [`Transport::recv`].
type Received = Result<(u64, SqsMessage)>;

/// A settled message awaiting `DeleteMessageBatch`. Its in-flight slot is
/// held until the batch call returns (the message is invisible until then).
struct PendingDelete {
    tag: u64,
    receipt_handle: String,
    _slot: OwnedSemaphorePermit,
}

/// The SQS transport: a poller task feeding a bounded channel, the receipt
/// handles of everything unsettled, and a background delete batcher.
pub struct SqsTransport {
    client: Client,
    queue_url: String,
    dead_letter: Option<DeadLetter>,
    long_record_secs: u64,
    rx: mpsc::Receiver<Received>,
    unsettled: HashMap<u64, SqsMessage>,
    delete_tx: mpsc::UnboundedSender<PendingDelete>,
    stop_tx: watch::Sender<bool>,
    poller: JoinHandle<()>,
    deleter: JoinHandle<()>,
}

impl SqsTransport {
    /// Spawns the poller and the delete batcher.
    fn start(
        client: Client,
        poll: PollParams,
        dead_letter: Option<DeadLetter>,
        long_record_secs: u64,
    ) -> Self {
        let queue_url = poll.queue_url.clone();
        let (tx, rx) = mpsc::channel(poll.max_messages.max(1) as usize);
        let (stop_tx, stop_rx) = watch::channel(false);
        let (delete_tx, delete_rx) = mpsc::unbounded_channel();
        let poller = Poller {
            client: client.clone(),
            slots: Arc::new(Semaphore::new(poll.cap.max(1))),
            p: poll,
            tx,
            stop: stop_rx,
            next_tag: 1,
        };
        let deleter = tokio::spawn(delete_loop(client.clone(), queue_url.clone(), delete_rx));
        Self {
            client,
            queue_url,
            dead_letter,
            long_record_secs,
            rx,
            unsettled: HashMap::new(),
            delete_tx,
            stop_tx,
            poller: tokio::spawn(poller.run()),
            deleter,
        }
    }

    fn delete(&self, tag: u64, m: SqsMessage) {
        // The batcher only exits after `close` drops the sender.
        let _ = self.delete_tx.send(PendingDelete {
            tag,
            receipt_handle: m.receipt_handle,
            _slot: m.slot,
        });
    }

    /// Forwards `m` to the DLQ (tagged with `reason`) and, only if that
    /// succeeded (or no DLQ is configured), deletes the source. On a failed
    /// `SendMessage` the source is left alone so the visibility timeout
    /// redelivers it — never delete what was not preserved.
    async fn dead_letter_and_delete(&self, tag: u64, m: SqsMessage, reason: &DeadLetterReason) {
        let info = m.info();
        let Some(dl) = &self.dead_letter else {
            warn!(
                "REJECTING (no DLQ, --allow-no-dlq): deleting {} : {}; body follows: {}",
                info.data_source,
                info.record_id,
                String::from_utf8_lossy(&m.body)
            );
            self.delete(tag, m);
            return;
        };
        match send_to_dead_letter(&self.client, dl, &m, &info, reason).await {
            Ok(()) => self.delete(tag, m),
            Err(e) => error!(
                "SendMessage to DLQ {} failed for {} : {}: {e:#}; leaving the source \
                 message for redelivery",
                dl.url, info.data_source, info.record_id
            ),
        }
    }
}

impl Transport for SqsTransport {
    async fn recv(&mut self) -> Option<Result<Delivery>> {
        let (tag, m) = match self.rx.recv().await? {
            Ok(received) => received,
            Err(e) => return Some(Err(e)),
        };
        let body = m.body.clone();
        self.unsettled.insert(tag, m);
        Some(Ok(Delivery { tag, body }))
    }

    async fn ack(&mut self, tag: u64) {
        if let Some(m) = self.unsettled.remove(&tag) {
            self.delete(tag, m);
        }
    }

    async fn dead_letter(&mut self, tag: u64, reason: DeadLetterReason) {
        let Some(m) = self.unsettled.remove(&tag) else {
            warn!("no unsettled SQS message {tag} ({reason}); cannot dead-letter");
            return;
        };
        self.dead_letter_and_delete(tag, m, &reason).await;
    }

    /// Leaves the message un-deleted: SQS redelivers it after its visibility
    /// timeout (at-least-once; the core loop names in-worker ones).
    async fn release(&mut self, tag: u64) {
        self.unsettled.remove(&tag);
    }

    /// Visibility heartbeat, measured from the receive time (the visibility
    /// clock), not the loop's dispatch time.
    async fn extend_lease(&mut self, tag: u64, _elapsed: Duration) {
        let Some(m) = self.unsettled.get_mut(&tag) else {
            return;
        };
        let elapsed = m.received.elapsed();
        let Some(new_vis) = visibility_extension(elapsed, self.long_record_secs, m.extended) else {
            return;
        };
        m.extended += 1;
        let (handle, times, info) = (m.receipt_handle.clone(), m.extended, m.info());
        match self
            .client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(handle)
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
                "ChangeMessageVisibility failed for {tag} ({} : {}): {e}; SQS may redeliver \
                 this in-progress record",
                info.data_source, info.record_id
            ),
        }
    }

    fn stop_intake(&mut self) {
        let _ = self.stop_tx.send(true);
    }

    /// `ApproximateNumberOfMessages` (visible messages only).
    async fn depth(&mut self) -> Option<u32> {
        let probe = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send();
        let resp = match tokio::time::timeout(DEPTH_PROBE_TIMEOUT, probe).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                warn!("MQ depth probe failed: {e}");
                return None;
            }
            Err(_) => {
                warn!("MQ depth probe timed out after {DEPTH_PROBE_TIMEOUT:?}");
                return None;
            }
        };
        resp.attributes()?
            .get(&QueueAttributeName::ApproximateNumberOfMessages)?
            .parse()
            .ok()
    }

    /// Stops the poller, flushes every pending delete (settled adds must not
    /// be redelivered; bounded by [`CLOSE_FLUSH_TIMEOUT`]) and names messages
    /// received but never dispatched (left for redelivery).
    async fn close(mut self) {
        self.stop_intake();
        let _ = self.poller.await;
        let mut undispatched = 0usize;
        while let Ok(Ok(_)) = self.rx.try_recv() {
            undispatched += 1;
        }
        if undispatched > 0 {
            info!("leaving {undispatched} received-but-undispatched SQS message(s) for redelivery");
        }
        drop(self.delete_tx);
        let abort = self.deleter.abort_handle();
        if tokio::time::timeout(CLOSE_FLUSH_TIMEOUT, self.deleter)
            .await
            .is_err()
        {
            abort.abort();
            warn!(
                "SQS delete flush timed out after {CLOSE_FLUSH_TIMEOUT:?}; unflushed message(s) \
                 redeliver after their visibility timeout"
            );
        }
    }
}

async fn send_to_dead_letter(
    client: &Client,
    dl: &DeadLetter,
    m: &SqsMessage,
    info: &RecordInfo,
    reason: &DeadLetterReason,
) -> Result<()> {
    let body = std::str::from_utf8(&m.body).context("non-UTF-8 body cannot be forwarded to SQS")?;
    let mut req = client.send_message().queue_url(&dl.url).message_body(body);
    if let Some(value) = sz_reason_value(&reason.to_string(), body.len()) {
        let attr = MessageAttributeValue::builder()
            .data_type("String")
            .string_value(value)
            .build()
            .context("build SzReason message attribute")?;
        req = req.message_attributes(SZ_REASON_ATTRIBUTE, attr);
    }
    if dl.fifo {
        let group = m
            .message_group_id
            .clone()
            .unwrap_or_else(|| info.data_source.clone());
        let dedup = m
            .message_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", info.data_source, info.record_id));
        req = req.message_group_id(group).message_deduplication_id(dedup);
    }
    req.send().await.map(|_| ()).map_err(anyhow::Error::from)
}

/// The `SzReason` attribute value for `reason`: characters SQS rejects in
/// attribute values replaced by a space, then cut (on a char boundary) to
/// [`SZ_REASON_MAX_BYTES`] and to whatever room a `body_len`-byte body leaves
/// under [`SQS_MAX_MESSAGE_BYTES`]. `None` when nothing fits.
fn sz_reason_value(reason: &str, body_len: usize) -> Option<String> {
    let overhead = body_len + SZ_REASON_ATTRIBUTE.len() + "String".len();
    let limit = SZ_REASON_MAX_BYTES.min(SQS_MAX_MESSAGE_BYTES.saturating_sub(overhead));
    let mut value = String::new();
    for c in reason.chars() {
        let c = if sqs_attribute_char(c) { c } else { ' ' };
        if value.len() + c.len_utf8() > limit {
            break;
        }
        value.push(c);
    }
    (!value.is_empty()).then_some(value)
}

/// Whether SQS accepts `c` in a message attribute value (#x9 | #xA | #xD |
/// #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF; Rust `char` already
/// excludes the surrogates).
fn sqs_attribute_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r') || (c >= ' ' && c != '\u{fffe}' && c != '\u{ffff}')
}

/// Background delete batcher: waits for at least one settled message, then
/// deletes everything pending (up to [`DELETE_BATCH_MAX`] per call) at once.
/// Batches form naturally under load (acks pile up during each call) and a
/// lone delete is never held back, so freed in-flight slots return promptly.
/// Exits once `close` drops the sender and the rest is flushed.
async fn delete_loop(
    client: Client,
    queue_url: String,
    mut rx: mpsc::UnboundedReceiver<PendingDelete>,
) {
    let mut batch = Vec::with_capacity(DELETE_BATCH_MAX);
    while rx.recv_many(&mut batch, DELETE_BATCH_MAX).await > 0 {
        flush_deletes(&client, &queue_url, &batch).await;
        // Dropping the entries releases their in-flight slots.
        batch.clear();
    }
}

/// Issues one `DeleteMessageBatch` (at most [`DELETE_BATCH_MAX`] entries). A
/// failed entry is logged (the message redelivers after its visibility
/// timeout, and `add_record` is idempotent), never retried here.
async fn flush_deletes(client: &Client, queue_url: &str, chunk: &[PendingDelete]) {
    let entries: Vec<DeleteMessageBatchRequestEntry> = chunk
        .iter()
        .filter_map(|d| {
            DeleteMessageBatchRequestEntry::builder()
                .id(d.tag.to_string())
                .receipt_handle(&d.receipt_handle)
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

struct PollParams {
    queue_url: String,
    visibility_timeout: i32,
    wait_time: i32,
    max_messages: i32,
    cap: usize,
}

/// Consecutive `ReceiveMessage` failure counter ([`RECEIVE_ERROR_MAX`]).
#[derive(Default)]
struct ReceiveFailures(u32);

impl ReceiveFailures {
    /// Records one failure; `true` once the run must become fatal.
    fn failed(&mut self) -> bool {
        self.0 += 1;
        self.0 >= RECEIVE_ERROR_MAX
    }

    fn succeeded(&mut self) {
        self.0 = 0;
    }
}

/// Waits until `stop` is set (or its sender is gone). Cancel-safe.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}

/// The `ReceiveMessage` long-poll task. Every await point races the stop
/// signal, so intake ends promptly at shutdown.
struct Poller {
    client: Client,
    p: PollParams,
    slots: Arc<Semaphore>,
    tx: mpsc::Sender<Received>,
    stop: watch::Receiver<bool>,
    next_tag: u64,
}

impl Poller {
    async fn run(mut self) {
        let mut failures = ReceiveFailures::default();
        loop {
            let Some(slots) = self.reserve().await else {
                return;
            };
            let resp = tokio::select! {
                biased;
                () = stopped(&mut self.stop) => return,
                r = receive_batch(&self.client, &self.p, slots.len()) => r,
            };
            let more = match resp {
                Ok(resp) => {
                    failures.succeeded();
                    self.forward(resp, slots).await
                }
                Err(e) if failures.failed() => {
                    let msg = format!(
                        "SQS ReceiveMessage failed {RECEIVE_ERROR_MAX} consecutive times; \
                         last error: {e}"
                    );
                    self.send(Err(anyhow!(msg))).await;
                    false
                }
                Err(e) => {
                    warn!("SQS ReceiveMessage error: {e}; retrying");
                    self.backoff().await
                }
            };
            if !more {
                return;
            }
        }
    }

    /// Waits for at least one free in-flight slot (woken the moment a settled
    /// message releases one), then takes the rest of the room up to one batch.
    /// `None` on stop.
    async fn reserve(&mut self) -> Option<Vec<OwnedSemaphorePermit>> {
        let first = tokio::select! {
            biased;
            () = stopped(&mut self.stop) => return None,
            permit = self.slots.clone().acquire_owned() => permit.ok()?,
        };
        Some(take_room(
            &self.slots,
            first,
            self.p.max_messages.max(1) as usize,
        ))
    }

    /// Hands each received message (with its slot) to the loop; unused slots
    /// are released. `false` when stopping.
    async fn forward(
        &mut self,
        resp: ReceiveMessageOutput,
        mut slots: Vec<OwnedSemaphorePermit>,
    ) -> bool {
        for m in resp.messages.unwrap_or_default() {
            let Some(slot) = slots.pop() else {
                break;
            };
            let Some(msg) = sqs_message(m, slot) else {
                continue;
            };
            let tag = self.next_tag;
            self.next_tag += 1;
            if !self.send(Ok((tag, msg))).await {
                return false;
            }
        }
        true
    }

    /// Bounded channel send, cancelable by stop. `false` when stopping or the
    /// transport is gone.
    async fn send(&mut self, item: Received) -> bool {
        tokio::select! {
            biased;
            () = stopped(&mut self.stop) => false,
            res = self.tx.send(item) => res.is_ok(),
        }
    }

    /// Sleeps [`RECEIVE_ERROR_BACKOFF`]; `false` if stopped meanwhile.
    async fn backoff(&mut self) -> bool {
        tokio::select! {
            biased;
            () = stopped(&mut self.stop) => false,
            () = tokio::time::sleep(RECEIVE_ERROR_BACKOFF) => true,
        }
    }
}

/// `first` plus every other free slot, up to `batch` in total: the receive
/// size is `min(batch, prefetch - outstanding)`.
fn take_room(
    slots: &Arc<Semaphore>,
    first: OwnedSemaphorePermit,
    batch: usize,
) -> Vec<OwnedSemaphorePermit> {
    let mut taken = vec![first];
    while taken.len() < batch {
        match slots.clone().try_acquire_owned() {
            Ok(permit) => taken.push(permit),
            Err(_) => break,
        }
    }
    taken
}

/// One `ReceiveMessage` for at most `room` messages. SDK-internal retries are
/// disabled: the poller is the retry policy, so the fatal window stays
/// [`RECEIVE_ERROR_MAX`] x [`RECEIVE_ERROR_BACKOFF`] (with SDK retries each
/// failed call would add its own jittered backoff and draw on the client's
/// retry quota, making the window nondeterministic).
async fn receive_batch(
    client: &Client,
    p: &PollParams,
    room: usize,
) -> Result<ReceiveMessageOutput> {
    client
        .receive_message()
        .queue_url(&p.queue_url)
        .max_number_of_messages(room as i32)
        .wait_time_seconds(p.wait_time)
        .visibility_timeout(p.visibility_timeout)
        .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
        .customize()
        .config_override(aws_sdk_sqs::config::Builder::new().retry_config(RetryConfig::disabled()))
        .send()
        .await
        .map_err(|e| anyhow!("{}", aws_sdk_sqs::error::DisplayErrorContext(e)))
}

/// A received SQS message ready to track; `None` (slot released) when it
/// lacks a body or receipt handle.
fn sqs_message(m: Message, slot: OwnedSemaphorePermit) -> Option<SqsMessage> {
    let message_group_id = m
        .attributes()
        .and_then(|a| a.get(&MessageSystemAttributeName::MessageGroupId))
        .cloned();
    Some(SqsMessage {
        receipt_handle: m.receipt_handle?,
        body: m.body?.into_bytes(),
        message_group_id,
        message_id: m.message_id,
        received: Instant::now(),
        extended: 0,
        slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region};

    /// An SQS endpoint that accepts connections and never answers (a hung
    /// API): `close` must still return, bounded by `CLOSE_FLUSH_TIMEOUT`,
    /// with a delete pending.
    #[tokio::test]
    async fn close_bounds_a_hung_delete_flush() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let conf = aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("id", "secret", None, None, "test"))
            .endpoint_url(&endpoint)
            .build();
        let poll = PollParams {
            queue_url: format!("{endpoint}/000000000000/hung"),
            visibility_timeout: 30,
            wait_time: 1,
            max_messages: 1,
            cap: 1,
        };
        let transport = SqsTransport::start(Client::from_conf(conf), poll, None, 300);
        let slot = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        transport
            .delete_tx
            .send(PendingDelete {
                tag: 1,
                receipt_handle: "receipt".to_string(),
                _slot: slot,
            })
            .expect("queue a delete");
        let started = Instant::now();
        let bound = CLOSE_FLUSH_TIMEOUT + Duration::from_secs(5);
        assert!(
            tokio::time::timeout(bound, transport.close()).await.is_ok(),
            "close hung on the delete flush"
        );
        assert!(
            started.elapsed() >= CLOSE_FLUSH_TIMEOUT,
            "the flush was attempted"
        );
    }

    #[test]
    fn sz_reason_value_sanitizes_and_fits_sqs_limits() {
        assert_eq!(
            sz_reason_value("SENZ2207|bad\u{1}source", 10).as_deref(),
            Some("SENZ2207|bad source")
        );
        let long = "é".repeat(SZ_REASON_MAX_BYTES);
        let v = sz_reason_value(&long, 0).expect("value");
        assert!(v.len() <= SZ_REASON_MAX_BYTES && v.len() > SZ_REASON_MAX_BYTES - 2);
        let full = SQS_MAX_MESSAGE_BYTES - SZ_REASON_ATTRIBUTE.len() - "String".len() - 3;
        assert_eq!(sz_reason_value("abcdef", full).as_deref(), Some("abc"));
        assert_eq!(sz_reason_value("abc", SQS_MAX_MESSAGE_BYTES), None);
        assert_eq!(sz_reason_value("", 0), None);
    }

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

    #[test]
    fn receive_failures_are_fatal_only_after_max_consecutive() {
        let mut f = ReceiveFailures::default();
        for _ in 1..RECEIVE_ERROR_MAX {
            assert!(!f.failed());
        }
        f.succeeded();
        for _ in 1..RECEIVE_ERROR_MAX {
            assert!(!f.failed(), "a success resets the count");
        }
        assert!(
            f.failed(),
            "the {RECEIVE_ERROR_MAX}th consecutive failure is fatal"
        );
    }

    #[test]
    fn sqs_message_requires_body_and_receipt_handle() {
        let slots = Arc::new(Semaphore::new(2));
        let full = Message::builder()
            .body("{}")
            .receipt_handle("h")
            .message_id("m")
            .build();
        let m = sqs_message(full, slots.clone().try_acquire_owned().unwrap()).unwrap();
        assert_eq!(
            (m.receipt_handle.as_str(), m.body.as_slice()),
            ("h", &b"{}"[..])
        );
        assert_eq!(m.message_id.as_deref(), Some("m"));
        assert_eq!(
            slots.available_permits(),
            1,
            "a tracked message holds its slot"
        );
        let no_handle = Message::builder().body("{}").build();
        assert!(sqs_message(no_handle, slots.clone().try_acquire_owned().unwrap()).is_none());
        assert_eq!(
            slots.available_permits(),
            1,
            "a skipped message frees its slot"
        );
        drop(m);
        assert_eq!(slots.available_permits(), 2, "settling frees the slot");
    }

    #[test]
    fn receive_room_is_prefetch_minus_outstanding_capped_by_batch() {
        let prefetch = 6;
        let slots = Arc::new(Semaphore::new(prefetch));
        let take = |batch| {
            let first = slots.clone().try_acquire_owned().expect("a free slot");
            take_room(&slots, first, batch)
        };
        let outstanding = take(2);
        assert_eq!(outstanding.len(), 2, "capped by the batch size");
        let rest = take(10);
        assert_eq!(
            rest.len(),
            prefetch - outstanding.len(),
            "prefetch - outstanding"
        );
        assert_eq!(slots.available_permits(), 0, "never beyond the total cap");
        drop(outstanding);
        assert_eq!(take(10).len(), 2, "settled slots are room again");
    }
}
