//! Publishing one file onto the Artemis queue.
//!
//! ## Flow and memory bound
//! A blocking reader task streams records into a channel of `max_pending`
//! slots; the async side keeps at most `max_pending` transfers unsettled
//! (FIFO), sending while there is room and otherwise awaiting the oldest
//! outcome. Records in memory per file ≤ channel + in-flight + retry queue
//! ≤ ~3 × `max_pending`; `--parallel N` multiplies that by N.
//!
//! ## Broker back pressure (address full, BLOCK policy)
//! Artemis withholds AMQP link credit while an address is over its
//! `max-size-bytes` with `address-full-policy=BLOCK`. `send_batchable` then
//! waits for credit. That wait is NOT link loss: there is deliberately no send
//! or settlement timeout (one would reconnect and republish → duplicates).
//! The wait is unbounded and logged every `blocked_log_interval` instead. A
//! genuinely dead broker is still detected: the connection idle timeout
//! ([`sz_activemq_amqp::IDLE_TIMEOUT`]) fails the link, which the broker's
//! heartbeats prevent while it is merely blocking. The same holds while
//! awaiting settlements: that wait is raced against the session/connection
//! ending ([`settle_or_link_down`]), because fe2o3-amqp never completes an
//! unsettled transfer's outcome when its connection stops.
//!
//! ## Outcomes
//! * `accepted` — settled (the broker owns the record).
//! * `released` / `modified` — counted, retried after `retry_delay`.
//! * `rejected` (or any other outcome) — counted, retried after
//!   `retry_delay`.
//!   A record not accepted after `max_attempts` attempts is FAILED: logged
//!   with its file and line, counted, and the run exits non-zero at the end.
//! * A message that can never be sent (larger than the link's max message
//!   size, unencodable, or needing more than ONE AMQP frame — see below)
//!   fails immediately the same way.
//!
//! ## Single-frame transfers only
//! fe2o3-amqp 0.18.2 numbers deliveries with the session's per-FRAME
//! transfer-id (`session/mod.rs`, `on_outgoing_transfer_inner`), so a message
//! split over k frames skips k-1 delivery-ids. Artemis (proton-j) answers with
//! "Expected delivery-id N, got N+k-1", closes the connection, and its AMQP
//! event-loop thread can then spin in `verifyNewDeliveryIdSequence`, starving
//! every other connection on that thread. So a record is sent only when its
//! transfer fits one frame of the broker's advertised `max-frame-size` (read
//! off the wire at connect); larger records are FAILED without being sent.
//! The broker must raise its AMQP acceptor `maxFrameSize` above the largest
//! record (`--min-max-frame-size` refuses to start against one that has not).
//! * Link loss (send/settlement error): reconnect (bounded by
//!   `reconnect_timeout`) and resend every unsettled record — at-least-once,
//!   so a record the broker accepted just before the loss can be queued
//!   twice (add_record is idempotent per DATA_SOURCE + RECORD_ID).

use std::collections::VecDeque;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::SendError;
use fe2o3_amqp::link::delivery::DeliveryFut;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::Outcome;
use fe2o3_amqp::{Sender, Session};
use sz_activemq_amqp::{
    BrokerParams, open_connection_observed, open_error_is_permanent, queue_target, record_message,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::input::LineReader;

/// Bound on the orderly link/session/connection close.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on one connect attempt (TCP + SASL + open + begin + attach). A
/// broker whose AMQP event loop is wedged still accepts TCP (the kernel does)
/// but never answers the protocol header; without this bound the attempt, and
/// the publisher, wait forever. A timed-out attempt is transient (retried
/// until `--reconnect-timeout`).
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
/// Bytes reserved, beyond the record, for the transfer performative, the
/// frame header and the message's header + data-section encoding (~80 bytes
/// in fe2o3 0.18.2's encoding; a record within this margin of the frame
/// limit is refused although it might just fit).
const FRAME_OVERHEAD_BYTES: usize = 1024;
/// Bytes of a failed record quoted in its error line.
const FAILED_PREVIEW_BYTES: usize = 200;

/// Everything a file publisher needs (shared by all files).
#[derive(Debug, Clone)]
pub struct PublishConfig {
    pub broker: BrokerParams,
    /// Prefix of the AMQP container id and link names.
    pub instance: String,
    pub max_pending: usize,
    pub retry_delay: Duration,
    /// Attempts per record before it is FAILED; `0` = unlimited.
    pub max_attempts: u32,
    /// Give up reconnecting after this long; `None` = never.
    pub reconnect_timeout: Option<Duration>,
    pub blocked_log_interval: Duration,
    /// AMQP header `durable` (persistent) flag.
    pub durable: bool,
    /// Refuse to publish (exit 1) when the broker advertises a smaller AMQP
    /// `max-frame-size`; `0` = no floor (oversize records still FAIL).
    pub min_max_frame_size: u32,
}

/// Per-file (and, merged, overall) counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileStats {
    /// Non-blank lines read.
    pub read: u64,
    /// Accepted by the broker.
    pub settled: u64,
    /// `rejected` (or other non-accept) outcomes, retries included.
    pub rejected: u64,
    /// `released` / `modified` outcomes, retries included.
    pub released: u64,
    /// Records re-sent after a link loss.
    pub resent: u64,
    /// Link losses recovered by reconnecting.
    pub reconnects: u64,
    /// Records given up on (never accepted).
    pub failed: u64,
}

impl FileStats {
    pub fn merge(&mut self, o: &FileStats) {
        self.read += o.read;
        self.settled += o.settled;
        self.rejected += o.rejected;
        self.released += o.released;
        self.resent += o.resent;
        self.reconnects += o.reconnects;
        self.failed += o.failed;
    }

    pub fn summary(&self) -> String {
        format!(
            "read={} settled={} rejected={} released={} resent={} reconnects={} failed={}",
            self.read,
            self.settled,
            self.rejected,
            self.released,
            self.resent,
            self.reconnects,
            self.failed
        )
    }
}

/// Run-wide progress reporting every `interval` settled records.
pub struct Progress {
    settled: AtomicU64,
    interval: u64,
    start: Instant,
    /// `(time, settled)` at the previous report.
    last: Mutex<(Instant, u64)>,
}

impl Progress {
    pub fn new(interval: u64) -> Self {
        let now = Instant::now();
        Self {
            settled: AtomicU64::new(0),
            interval,
            start: now,
            last: Mutex::new((now, 0)),
        }
    }

    fn settled_one(&self) {
        let n = self.settled.fetch_add(1, Ordering::Relaxed) + 1;
        if self.interval == 0 || !n.is_multiple_of(self.interval) {
            return;
        }
        let now = Instant::now();
        let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        let dt = now.duration_since(last.0).as_secs_f64().max(1e-9);
        let rate = (n - last.1) as f64 / dt;
        let overall = n as f64 / now.duration_since(self.start).as_secs_f64().max(1e-9);
        *last = (now, n);
        info!("progress: {n} settled ({rate:.0}/s interval, {overall:.0}/s overall)");
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// One record read from the input.
struct Record {
    line: u64,
    bytes: Vec<u8>,
    /// Non-accepted outcomes so far (link-loss resends do not count).
    attempts: u32,
}

/// A transfer awaiting its outcome.
struct InFlight {
    record: Record,
    outcome: DeliveryFut<Result<Outcome, SendError>>,
}

/// An attached sender. Field order is the teardown order.
struct Link {
    sender: Sender,
    /// The broker's advertised `max-frame-size` (this client's send limit).
    max_frame: u32,
    session: SessionHandle<()>,
    connection: ConnectionHandle<()>,
}

/// Why a connect attempt failed.
enum ConnectError {
    /// Retrying cannot help (bad credentials, bad URL).
    Permanent(anyhow::Error),
    Transient(anyhow::Error),
}

/// Awaits `fut`, logging `what(waited)` every `every` until it completes. Never
/// cancels it: a send blocked on broker credit must keep waiting.
async fn await_logging<F: Future>(fut: F, every: Duration, what: impl Fn(Duration)) -> F::Output {
    tokio::pin!(fut);
    let start = Instant::now();
    let mut tick = tokio::time::interval_at(start + every, every);
    loop {
        tokio::select! {
            biased;
            out = &mut fut => return out,
            _ = tick.tick() => what(start.elapsed()),
        }
    }
}

/// Whether a record of `record_len` bytes goes out as ONE transfer frame
/// under the broker's `max_frame` (with [`FRAME_OVERHEAD_BYTES`] reserved).
fn fits_one_frame(record_len: usize, max_frame: u32) -> bool {
    record_len.saturating_add(FRAME_OVERHEAD_BYTES) <= max_frame as usize
}

/// Awaits a transfer's outcome, failing as soon as the session or connection
/// stops.
///
/// fe2o3-amqp 0.18.2 never completes the outcome of a transfer that is still
/// unsettled when its connection stops (remote close, idle timeout, TCP loss):
/// the outcome's oneshot sender lives in the link's unsettled map, which the
/// still-alive [`Sender`] co-owns, so the session engine dropping its relay
/// does not drop it (`session/engine.rs`, end of `event_loop`). Awaiting the
/// outcome alone therefore hangs forever on a dead link; racing the
/// session/connection end turns that into a link loss (reconnect + resend).
async fn settle_or_link_down(
    outcome: DeliveryFut<Result<Outcome, SendError>>,
    link: &mut Link,
) -> Result<Outcome> {
    tokio::select! {
        biased;
        r = outcome => r.map_err(|e| anyhow!("settlement failed: {e}")),
        r = link.connection.on_close() => Err(anyhow!(
            "connection stopped while awaiting settlement: {}",
            r.map_or_else(|e| e.to_string(), |()| "closed".into())
        )),
        r = link.session.on_end() => Err(anyhow!(
            "session ended while awaiting settlement: {}",
            r.map_or_else(|e| e.to_string(), |()| "ended".into())
        )),
    }
}

/// Publishes one file; `Ok` when every record was settled or FAILED (counted).
pub struct FilePublisher<'a> {
    cfg: &'a PublishConfig,
    path: &'a Path,
    container_id: String,
    progress: &'a Progress,
    stats: FileStats,
}

impl<'a> FilePublisher<'a> {
    pub fn new(
        cfg: &'a PublishConfig,
        path: &'a Path,
        index: usize,
        progress: &'a Progress,
    ) -> Self {
        Self {
            cfg,
            path,
            container_id: format!("{}-{}-{index}", cfg.instance, std::process::id()),
            progress,
            stats: FileStats::default(),
        }
    }

    pub async fn run(mut self) -> Result<FileStats> {
        let path: PathBuf = self.path.to_path_buf();
        // Open before connecting: an unreadable file fails without a link.
        let mut reader = LineReader::open(&path)?;
        let (tx, mut rx) = mpsc::channel::<(u64, Vec<u8>)>(self.cfg.max_pending);
        let reading = tokio::task::spawn_blocking(move || -> Result<()> {
            while let Some(rec) = reader.next_record()? {
                if tx.blocking_send(rec).is_err() {
                    return Ok(()); // publisher gave up; its error is reported
                }
            }
            Ok(())
        });

        let mut link = self.connect().await?;
        info!(
            "broker max-frame-size {} bytes: records over {} bytes are FAILED unsent",
            link.max_frame,
            (link.max_frame as usize).saturating_sub(FRAME_OVERHEAD_BYTES)
        );
        info!(
            "publishing {} to {} (queue {})",
            path.display(),
            self.cfg.broker.url,
            self.cfg.broker.queue
        );
        let mut inflight: VecDeque<InFlight> = VecDeque::with_capacity(self.cfg.max_pending);
        let mut retry: VecDeque<Record> = VecDeque::new();
        let mut eof = false;

        loop {
            // Fill: retries first, then new records, while there is room.
            let mut link_lost: Option<(Record, anyhow::Error)> = None;
            while inflight.len() < self.cfg.max_pending {
                let record = if let Some(r) = retry.pop_front() {
                    r
                } else if eof {
                    break;
                } else {
                    let next = if inflight.is_empty() {
                        rx.recv().await
                    } else {
                        match rx.try_recv() {
                            Ok(r) => Some(r),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => None,
                        }
                    };
                    match next {
                        Some((line, bytes)) => {
                            self.stats.read += 1;
                            Record {
                                line,
                                bytes,
                                attempts: 0,
                            }
                        }
                        None => {
                            eof = true;
                            break;
                        }
                    }
                };
                if !fits_one_frame(record.bytes.len(), link.max_frame) {
                    self.fail(
                        &record,
                        &format!(
                            "{} bytes needs more than one AMQP frame (broker max-frame-size {}); \
                             not sent: raise the broker acceptor maxFrameSize",
                            record.bytes.len(),
                            link.max_frame
                        ),
                    );
                    continue;
                }
                let message = record_message(record.bytes.clone(), self.cfg.durable);
                let inflight_now = inflight.len();
                let sent = await_logging(
                    link.sender.send_batchable(message),
                    self.cfg.blocked_log_interval,
                    |waited| {
                        warn!(
                            "blocked on broker credit for {}s ({}: {inflight_now} in flight; \
                             address full? waiting, not reconnecting)",
                            waited.as_secs(),
                            path.display()
                        )
                    },
                )
                .await;
                match sent {
                    Ok(outcome) => inflight.push_back(InFlight { record, outcome }),
                    Err(
                        e @ (SendError::MessageSizeExceeded(_) | SendError::MessageEncodeError),
                    ) => {
                        self.fail(&record, &format!("cannot be sent: {e}"));
                    }
                    Err(e) => {
                        link_lost = Some((record, anyhow!("send failed: {e}")));
                        break;
                    }
                }
            }

            // Settle the oldest transfer.
            if link_lost.is_none() {
                let Some(InFlight { record, outcome }) = inflight.pop_front() else {
                    if eof && retry.is_empty() {
                        break;
                    }
                    continue;
                };
                let line = record.line;
                let settling = settle_or_link_down(outcome, &mut link);
                let result = await_logging(settling, self.cfg.blocked_log_interval, |waited| {
                    warn!(
                        "waiting {}s for the broker to settle {} line {line}",
                        waited.as_secs(),
                        path.display()
                    )
                })
                .await;
                match result {
                    Ok(outcome) => {
                        if let Some(r) = self.on_outcome(record, outcome) {
                            tokio::time::sleep(self.cfg.retry_delay).await;
                            retry.push_back(r);
                        }
                    }
                    Err(e) => link_lost = Some((record, e)),
                }
            }

            if let Some((record, why)) = link_lost {
                // Salvage outcomes that already arrived; resend the rest.
                let mut resend: VecDeque<Record> = VecDeque::new();
                for InFlight { record, outcome } in inflight.drain(..) {
                    match tokio::time::timeout(Duration::ZERO, outcome).await {
                        Ok(Ok(Outcome::Accepted(_))) => self.settled(),
                        _ => resend.push_back(record),
                    }
                }
                resend.push_back(record);
                self.stats.resent += resend.len() as u64;
                self.stats.reconnects += 1;
                warn!(
                    "broker link lost ({why:#}) on {}; reconnecting, {} record(s) to resend",
                    path.display(),
                    resend.len()
                );
                resend.extend(retry.drain(..));
                retry = resend;
                Self::close(link).await;
                link = self.connect().await?;
            }
        }

        Self::close(link).await;
        reading
            .await
            .context("file reader task panicked")?
            .with_context(|| format!("reading {}", path.display()))?;
        let s = self.stats;
        if s.settled + s.failed != s.read {
            bail!(
                "internal accounting error on {}: {} (settled + failed != read)",
                path.display(),
                s.summary()
            );
        }
        Ok(s)
    }

    fn settled(&mut self) {
        self.stats.settled += 1;
        self.progress.settled_one();
    }

    /// Accounts one outcome; returns the record when it must be retried.
    fn on_outcome(&mut self, mut record: Record, outcome: Outcome) -> Option<Record> {
        let kind = match outcome {
            Outcome::Accepted(_) => {
                self.settled();
                return None;
            }
            Outcome::Released(_) | Outcome::Modified(_) => {
                self.stats.released += 1;
                "released"
            }
            other => {
                self.stats.rejected += 1;
                warn!(
                    "broker did not accept {} line {}: {other:?}",
                    self.path.display(),
                    record.line
                );
                "rejected"
            }
        };
        record.attempts += 1;
        if self.cfg.max_attempts != 0 && record.attempts >= self.cfg.max_attempts {
            self.fail(
                &record,
                &format!("{kind} on all {} attempts", record.attempts),
            );
            return None;
        }
        Some(record)
    }

    fn fail(&mut self, record: &Record, why: &str) {
        self.stats.failed += 1;
        let mut end = record.bytes.len().min(FAILED_PREVIEW_BYTES);
        let preview = loop {
            match std::str::from_utf8(&record.bytes[..end]) {
                Ok(s) => break s,
                Err(e) => end = e.valid_up_to(),
            }
        };
        error!(
            "FAILED record {} line {} ({why}): {preview}",
            self.path.display(),
            record.line
        );
    }

    /// Connects, retrying transient failures until `reconnect_timeout`.
    async fn connect(&self) -> Result<Link> {
        let start = Instant::now();
        loop {
            let attempt = tokio::time::timeout(CONNECT_ATTEMPT_TIMEOUT, self.try_connect())
                .await
                .unwrap_or_else(|_| {
                    Err(ConnectError::Transient(anyhow!(
                        "no AMQP handshake from {} within {CONNECT_ATTEMPT_TIMEOUT:?}",
                        self.cfg.broker.url
                    )))
                });
            match attempt {
                Ok(link) => return Ok(link),
                Err(ConnectError::Permanent(e)) => return Err(e),
                Err(ConnectError::Transient(e)) => {
                    if let Some(limit) = self.cfg.reconnect_timeout
                        && start.elapsed() >= limit
                    {
                        return Err(e.context(format!(
                            "broker unreachable for {}s (--reconnect-timeout)",
                            limit.as_secs()
                        )));
                    }
                    warn!(
                        "ActiveMQ connect failed: {e:#}; retrying in {:?}",
                        self.cfg.retry_delay
                    );
                    tokio::time::sleep(self.cfg.retry_delay).await;
                }
            }
        }
    }

    async fn try_connect(&self) -> Result<Link, ConnectError> {
        let (mut connection, max_frame) =
            open_connection_observed(&self.cfg.broker, self.container_id.clone())
                .await
                .map_err(|e| {
                    let permanent = open_error_is_permanent(&e);
                    let e = anyhow!(e).context(format!(
                        "failed to connect to ActiveMQ at {}",
                        self.cfg.broker.url
                    ));
                    if permanent {
                        ConnectError::Permanent(e)
                    } else {
                        ConnectError::Transient(e)
                    }
                })?;
        if max_frame < self.cfg.min_max_frame_size {
            let _ = connection.close().await;
            return Err(ConnectError::Permanent(anyhow!(
                "broker {} advertises AMQP max-frame-size {max_frame} bytes, below \
                 --min-max-frame-size {}: larger records would be split over several frames, \
                 which fe2o3-amqp numbers wrongly (Artemis closes the connection). Raise the \
                 broker's AMQP acceptor maxFrameSize (e.g. broker.properties \
                 acceptorConfigurations.amqp.params.maxFrameSize=268435456) or lower \
                 --min-max-frame-size",
                self.cfg.broker.url,
                self.cfg.min_max_frame_size
            )));
        }
        let mut session = match Session::begin(&mut connection).await {
            Ok(s) => s,
            Err(e) => {
                let _ = connection.close().await;
                return Err(ConnectError::Transient(
                    anyhow!(e).context("failed to begin AMQP session"),
                ));
            }
        };
        let attached = Sender::builder()
            .name(format!("{}-sender", self.container_id))
            .target(queue_target(&self.cfg.broker.queue))
            .attach(&mut session)
            .await;
        match attached {
            Ok(sender) => Ok(Link {
                sender,
                max_frame,
                session,
                connection,
            }),
            Err(e) => {
                let _ = session.end().await;
                let _ = connection.close().await;
                Err(ConnectError::Transient(anyhow!(e).context(format!(
                    "failed to attach a sender to queue '{}'",
                    self.cfg.broker.queue
                ))))
            }
        }
    }

    /// Detach, end, close — best-effort and bounded.
    async fn close(link: Link) {
        let Link {
            sender,
            max_frame: _,
            mut session,
            mut connection,
        } = link;
        let closing = async {
            let _ = sender.close().await;
            let _ = session.end().await;
            let _ = connection.close().await;
        };
        if tokio::time::timeout(CLOSE_TIMEOUT, closing).await.is_err() {
            warn!("AMQP close timed out after {CLOSE_TIMEOUT:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_merge_sums_every_counter() {
        let a = FileStats {
            read: 1,
            settled: 2,
            rejected: 3,
            released: 4,
            resent: 5,
            reconnects: 6,
            failed: 7,
        };
        let mut m = a;
        m.merge(&a);
        assert_eq!(
            m.summary(),
            "read=2 settled=4 rejected=6 released=8 resent=10 reconnects=12 failed=14"
        );
    }

    #[test]
    fn frame_guard_reserves_the_overhead() {
        let max = 131_072;
        assert!(fits_one_frame(max as usize - FRAME_OVERHEAD_BYTES, max));
        assert!(!fits_one_frame(
            max as usize - FRAME_OVERHEAD_BYTES + 1,
            max
        ));
        assert!(!fits_one_frame(usize::MAX, u32::MAX), "no overflow");
        assert!(fits_one_frame(150_100_000, 268_435_456));
    }

    #[tokio::test]
    async fn await_logging_logs_while_waiting_and_never_cancels() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let out = await_logging(
            async {
                tokio::time::sleep(Duration::from_millis(350)).await;
                42
            },
            Duration::from_millis(100),
            |_| {
                calls.fetch_add(1, Ordering::Relaxed);
            },
        )
        .await;
        assert_eq!(out, 42, "the blocked future still completes");
        let n = calls.load(Ordering::Relaxed);
        assert!(
            (2..=3).contains(&n),
            "logged ~every 100ms while blocked: {n}"
        );
    }
}
