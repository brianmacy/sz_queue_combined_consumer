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
//!   size or unencodable) fails immediately the same way.
//! * A record larger than `side_max_bytes` is not published: it is appended to
//!   the side file and logged `sided <file> line <n> bytes <b>` (loaded later
//!   by the consumer's file mode); with no side file it is FAILED.
//!
//! ## Multi-frame transfers
//! A record larger than the negotiated `max-frame-size` goes out as several
//! transfer frames. Upstream fe2o3-amqp 0.18.2/0.18.3 numbered deliveries with
//! the per-FRAME transfer-id, so the next delivery-id skipped and Artemis
//! closed the link ("Expected delivery-id N, got N+k-1") and could spin its
//! AMQP event loop. The workspace uses a vendored fe2o3-amqp with a
//! per-delivery counter (`vendor/VENDORED.md`), so multi-frame deliveries are
//! numbered correctly and need no broker `maxFrameSize` change.
//!
//! ## Resume and progress
//! `fileprogress <file> <n>` (stdout, every `progress_every` records) means the
//! file's first `n` non-blank records are all settled, FAILED or sided — the
//! oldest still-pending record bounds it, so a resume never skips an
//! unacknowledged record. `resume` maps a file name to that `n`: those records
//! are skipped unread (same format as the Artemis CORE publisher's RESUME).
//! * Link loss (send/settlement error): reconnect (bounded by
//!   `reconnect_timeout`) and resend every unsettled record — at-least-once,
//!   so a record the broker accepted just before the loss can be queued
//!   twice (add_record is idempotent per DATA_SOURCE + RECORD_ID).

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    /// `max-frame-size`; `0` = no floor (multi-frame deliveries are fine).
    pub min_max_frame_size: u32,
    /// `max-frame-size` this client advertises (`None` = fe2o3 default); the
    /// negotiated frame is the smaller of the two peers'.
    pub local_max_frame_size: Option<u32>,
    /// Records larger than this are sided, not published.
    pub side_max_bytes: usize,
    /// Where sided records go (one per line); `None` = FAIL them.
    pub side: Option<Arc<Mutex<std::io::BufWriter<std::fs::File>>>>,
    /// File name -> leading non-blank records already published (skipped).
    pub resume: Arc<HashMap<String, u64>>,
    /// `fileprogress` cadence in records.
    pub progress_every: u64,
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
    /// Records over `side_max_bytes` written to the side file instead.
    pub sided: u64,
    /// Leading records skipped by `resume` (not counted in `read`).
    pub skipped: u64,
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
        self.sided += o.sided;
        self.skipped += o.skipped;
    }

    pub fn summary(&self) -> String {
        format!(
            "read={} settled={} rejected={} released={} resent={} reconnects={} failed={} sided={} skipped={}",
            self.read,
            self.settled,
            self.rejected,
            self.released,
            self.resent,
            self.reconnects,
            self.failed,
            self.sided,
            self.skipped
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
    /// 1-based ordinal among the file's non-blank records (resume unit).
    seq: u64,
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

/// Parses a resume file: one `<file name> <records already published>` per
/// line (the `fileprogress` lines with that word stripped also parse). Later
/// lines win; blank and `#` lines are ignored.
pub fn parse_resume(text: &str) -> Result<HashMap<String, u64>> {
    let mut map = HashMap::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let fields = match fields.as_slice() {
            ["fileprogress", rest @ ..] => rest.to_vec(),
            other => other.to_vec(),
        };
        let [name, n] = fields.as_slice() else {
            bail!(
                "resume line {}: expected '<file> <count>', got {raw:?}",
                i + 1
            );
        };
        let n: u64 = n
            .parse()
            .with_context(|| format!("resume line {}: bad count in {raw:?}", i + 1))?;
        map.insert((*name).to_string(), n);
    }
    Ok(map)
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
    /// File name used in `fileprogress` / `sided` lines and the resume key.
    name: String,
    /// Next `fileprogress` threshold.
    next_mark: u64,
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
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string()),
            next_mark: cfg.progress_every.max(1),
        }
    }

    pub async fn run(mut self) -> Result<FileStats> {
        let path: PathBuf = self.path.to_path_buf();
        // Open before connecting: an unreadable file fails without a link.
        let mut reader = LineReader::open(&path)?;
        let skip = self.cfg.resume.get(&self.name).copied().unwrap_or(0);
        let (tx, mut rx) = mpsc::channel::<(u64, u64, Vec<u8>)>(self.cfg.max_pending);
        let reading = tokio::task::spawn_blocking(move || -> Result<u64> {
            let mut seq = 0u64;
            while let Some((line, bytes)) = reader.next_record()? {
                seq += 1;
                if seq <= skip {
                    continue; // already published (resume)
                }
                if tx.blocking_send((seq, line, bytes)).is_err() {
                    return Ok(seq.min(skip)); // publisher gave up; its error is reported
                }
            }
            Ok(seq.min(skip))
        });
        if skip > 0 {
            info!("{}: resuming after {skip} record(s)", self.name);
        }
        let mut last_seq = skip;

        let mut link = self.connect().await?;
        info!(
            "broker max-frame-size {} bytes (larger records go out as several frames); \
             records over {} bytes are {}",
            link.max_frame,
            self.cfg.side_max_bytes,
            if self.cfg.side.is_some() {
                "sided"
            } else {
                "FAILED"
            }
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
                        Some((seq, line, bytes)) => {
                            self.stats.read += 1;
                            last_seq = seq;
                            Record {
                                seq,
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
                if record.bytes.len() > self.cfg.side_max_bytes {
                    self.side_or_fail(&record)?;
                    self.mark_progress(&inflight, &retry, last_seq);
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
                        self.mark_progress(&inflight, &retry, last_seq);
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
        self.stats.skipped = reading
            .await
            .context("file reader task panicked")?
            .with_context(|| format!("reading {}", path.display()))?;
        println!("fileprogress {} {last_seq}", self.name);
        let s = self.stats;
        if s.settled + s.failed + s.sided != s.read {
            bail!(
                "internal accounting error on {}: {} (settled + failed + sided != read)",
                path.display(),
                s.summary()
            );
        }
        Ok(s)
    }

    /// Writes an oversized record to the side file (FAILED when there is none).
    fn side_or_fail(&mut self, record: &Record) -> Result<()> {
        let Some(side) = &self.cfg.side else {
            self.fail(
                record,
                &format!(
                    "{} bytes exceeds --side-max-bytes {} and no --side-file is set",
                    record.bytes.len(),
                    self.cfg.side_max_bytes
                ),
            );
            return Ok(());
        };
        {
            let mut w = side.lock().expect("side-file lock poisoned");
            w.write_all(&record.bytes)
                .and_then(|()| w.write_all(b"\n"))
                .and_then(|()| w.flush())
                .context("writing the side file")?;
        }
        self.stats.sided += 1;
        println!(
            "sided {} line {} bytes {}",
            self.name,
            record.seq,
            record.bytes.len()
        );
        Ok(())
    }

    /// Prints `fileprogress` when the completed prefix crosses the next mark:
    /// every record before the oldest pending one is settled, FAILED or sided.
    fn mark_progress(
        &mut self,
        inflight: &VecDeque<InFlight>,
        retry: &VecDeque<Record>,
        last_seq: u64,
    ) {
        let oldest = inflight
            .iter()
            .map(|f| f.record.seq)
            .chain(retry.iter().map(|r| r.seq))
            .min();
        let done = oldest.map_or(last_seq, |seq| seq - 1);
        if done >= self.next_mark {
            println!("fileprogress {} {done}", self.name);
            let every = self.cfg.progress_every.max(1);
            self.next_mark = (done / every + 1) * every;
        }
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
        let (mut connection, max_frame) = open_connection_observed(
            &self.cfg.broker,
            self.container_id.clone(),
            self.cfg.local_max_frame_size,
        )
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
                 --min-max-frame-size {}",
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
            sided: 8,
            skipped: 9,
        };
        let mut m = a;
        m.merge(&a);
        assert_eq!(
            m.summary(),
            "read=2 settled=4 rejected=6 released=8 resent=10 reconnects=12 failed=14 sided=16 skipped=18"
        );
    }

    #[test]
    fn resume_parses_counts_and_fileprogress_lines_and_later_wins() {
        let m = parse_resume(
            "# comment\n\npart-00001.txt 100000\nfileprogress part-00002.txt 7\npart-00001.txt 200000\n",
        )
        .unwrap();
        assert_eq!(m.get("part-00001.txt"), Some(&200_000));
        assert_eq!(m.get("part-00002.txt"), Some(&7));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn resume_rejects_malformed_lines() {
        assert!(parse_resume("part-00001.txt\n").is_err(), "missing count");
        assert!(
            parse_resume("part-00001.txt ten\n").is_err(),
            "non-numeric count"
        );
        assert!(parse_resume("a b c\n").is_err(), "extra field");
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
