//! File-input load mode: read newline-delimited JSON (JSONL) records from a
//! single file and feed them through the SAME worker pool the RabbitMQ path
//! uses (`worker::worker_loop`, load-preferring, no redo). Selected by
//! `--file`/`SENZING_INPUT_FILE`; mutually exclusive with the AMQP `--url`.
//!
//! There is no message broker and therefore no ack/redelivery: a file cannot be
//! "requeued". Instead, `--skip-lines N`/`SENZING_SKIP_LINES` skips the first N
//! physical lines so an interrupted load can resume. Because engine
//! `add_record` is idempotent (re-adding the same record is an update, not a
//! duplicate), resuming is at-least-once and safe.
//!
//! ## Safe resume offset (contiguous-ack watermark)
//! Records are processed out of order by N workers, so "lines read" is NOT a
//! safe resume point — a late line may finish before an earlier one is even
//! dispatched. We therefore track a watermark: the highest line L such that
//! EVERY line up to and including L has completed (been added, dead-lettered as
//! bad data, or skipped as blank). On shutdown we report `skip + watermark`, so
//! a resume never skips an unprocessed line; at most a few still-in-flight lines
//! past the watermark are reprocessed (idempotent).
//!
//! ## Reject file
//! A file has no dead-letter queue, so every rejected line — unparseable JSON,
//! engine bad-input, retry timeout, SENZ0082 — is appended VERBATIM to a JSONL
//! side file (`--reject-file`, default `<input>.rejected.jsonl`) so it can be
//! reprocessed later simply by pointing `--file` at it. The file is created
//! lazily on the first reject (a clean load leaves nothing behind) and opened
//! in append mode so a resumed run adds to it. WHY each record was rejected is
//! in the application log (the worker logs the engine error text).
//!
//! ## redo
//! File mode is a PURE loader — it does not process the engine redo queue
//! (`redo%` is ignored, with a warning). Drain redo separately with a
//! `redo% = 100` run once the file load completes.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sz_rust_sdk::prelude::*;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::record::parse_record;
use crate::stats::{ADDS_PROCESSED, ADDS_REJECTED, ERRORS, RUNNING, WORKER_FATAL};
use crate::worker::{
    Action, Class, LoadItem, LoadSide, Outcome, SHUTDOWN_GRACE, WorkerCtx, add_record_flags,
    worker_loop,
};

/// Progress log cadence, in physical lines read.
const PROGRESS_EVERY: u64 = 50_000;

/// Tracks the contiguous-completion watermark for safe `--skip-lines` resume.
///
/// `next` is the lowest line number not yet known-complete; `out_of_order`
/// holds completed lines above a gap. `watermark()` = `next - 1`.
struct ResumeTracker {
    next: u64,
    out_of_order: HashSet<u64>,
}

impl ResumeTracker {
    /// `first_line` is the first line number this run will read (skip + 1).
    fn new(first_line: u64) -> Self {
        Self {
            next: first_line,
            out_of_order: HashSet::new(),
        }
    }

    /// Record that physical line `line` has completed (added, dead-lettered, or
    /// skipped). Advances `next` across any now-contiguous completed lines.
    fn complete(&mut self, line: u64) {
        if line < self.next {
            return; // already accounted for
        }
        self.out_of_order.insert(line);
        while self.out_of_order.remove(&self.next) {
            self.next += 1;
        }
    }

    /// Highest line number with every preceding line (from the run start)
    /// complete. `skip + watermark` is the safe resume offset. 0 = none yet.
    fn watermark(&self) -> u64 {
        self.next - 1
    }
}

/// Append-only JSONL sink for rejected input lines. Opened lazily on the first
/// write so a clean load creates no file. Every write is one original line +
/// `\n` in a single unbuffered `write_all` (a SIGTERM must not lose rejects,
/// and a buffered writer would re-emit retained bytes after a failed flush,
/// duplicating a line).
struct RejectSink {
    path: PathBuf,
    file: Option<File>,
    written: u64,
}

impl RejectSink {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file: None,
            written: 0,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn written(&self) -> u64 {
        self.written
    }

    /// Appends `line` (no trailing newline expected) as one JSONL record.
    fn write_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?,
            );
        }
        let mut buf = Vec::with_capacity(line.len() + 1);
        buf.extend_from_slice(line);
        buf.push(b'\n');
        self.file
            .as_mut()
            .expect("file opened above")
            .write_all(&buf)?;
        self.written += 1;
        Ok(())
    }
}

/// Bodies of lines dispatched to workers but not yet reported back, keyed by
/// line number, so an engine reject can be written to the reject file
/// verbatim. Bounded by workers + work-channel capacity.
type InFlightBodies = Arc<Mutex<HashMap<u64, Vec<u8>>>>;

/// Writes one rejected line to the sink and counts it. A sink I/O failure is
/// logged loudly (with the line so it is not lost silently) but does not abort
/// the load — the add itself already failed; stopping would lose more.
fn reject_line(sink: &Arc<Mutex<RejectSink>>, line_no: u64, body: &[u8]) {
    ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
    let mut sink = sink.lock().unwrap_or_else(PoisonError::into_inner);
    if let Err(e) = sink.write_line(body) {
        warn!(
            "cannot write rejected line {line_no} to {:?}: {e}; record follows: {}",
            sink.path(),
            String::from_utf8_lossy(body)
        );
    }
}

/// Runs the file loader to EOF (or SIGTERM) and returns
/// `(workers_clean, result)` mirroring [`crate::pure_redoer::run`], so `main`
/// can share the bounded-teardown exit path.
///
/// `workers_clean` is `true` iff every worker thread finished within the
/// shutdown grace (safe to destroy the environment); `false` means a worker is
/// still in an uninterruptible engine call (leak-on-exit).
pub fn run(config: &Config, env: Arc<SzEnvironmentCore>) -> (bool, Result<()>) {
    let path = config
        .input_file
        .clone()
        .expect("file_loader::run requires an input file (validated at startup)");
    let skip = config.skip_lines;
    let n_workers = config.threads;
    let reject_path = config
        .reject_file
        .clone()
        .expect("file_loader::run requires a reject file (resolved at startup)");

    info!(
        "File loader: reading {path:?} with {n_workers} workers (skip-lines: {skip}); \
         rejected lines are appended to {reject_path:?}; redo is NOT processed in file mode"
    );
    if config.redo_percent != 0 {
        warn!(
            "redo% = {} is ignored in file mode (pure loader); drain redo separately \
             with a redo% = 100 run",
            config.redo_percent
        );
    }
    crate::config_reload::log_startup_config(&env);

    // --- Bridge channels (same shapes as the AMQP path) ----------------------
    let (work_tx, work_rx) = mpsc::channel::<LoadItem>(n_workers);
    let (result_tx, result_rx) = mpsc::channel::<Outcome>(n_workers * 2);
    let work_rx = Arc::new(Mutex::new(work_rx));
    // `started` is required by the shared worker code (AMQP shutdown split); in
    // file mode it is harmless bookkeeping.
    let started: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let add_flags = add_record_flags(config.info);
    let want_info = config.info;

    // --- Spawn load-preferring workers ---------------------------------------
    let mut workers = Vec::with_capacity(n_workers);
    for worker_id in 0..n_workers {
        let ctx = WorkerCtx {
            worker_id,
            class: Class::LoadPreferring,
            env: env.clone(),
            load: Some(LoadSide {
                work_rx: work_rx.clone(),
                result_tx: result_tx.clone(),
                started: started.clone(),
                shutdown_notify: shutdown_notify.clone(),
            }),
            redo: None,
            add_flags,
            redo_flags: None,
            want_info,
        };
        match std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || worker_loop(ctx))
        {
            Ok(h) => workers.push(h),
            Err(e) => {
                RUNNING.store(false, Ordering::Relaxed);
                return (
                    true,
                    Err(anyhow::anyhow!("failed to spawn worker thread: {e}")),
                );
            }
        }
    }

    // --- Result consumer (counts outcomes; maintains the resume watermark) ---
    let resume = Arc::new(Mutex::new(ResumeTracker::new(skip + 1)));
    let sink = Arc::new(Mutex::new(RejectSink::new(&reject_path)));
    let in_flight: InFlightBodies = Arc::new(Mutex::new(HashMap::new()));
    let consumer_resume = resume.clone();
    let consumer_sink = sink.clone();
    let consumer_in_flight = in_flight.clone();
    let consumer = std::thread::Builder::new()
        .name("sz-file-result".to_string())
        .spawn(move || {
            result_consumer(
                result_rx,
                want_info,
                consumer_resume,
                consumer_sink,
                consumer_in_flight,
            )
        });

    // Drop our extra result sender so the channel closes once all workers exit.
    drop(result_tx);

    // --- Reader: skip, then feed each line to the worker pool ----------------
    let read_result = read_and_feed(Path::new(&path), skip, &work_tx, &resume, &sink, &in_flight);

    // EOF / stop: closing work_tx lets idle workers observe end-of-stream.
    drop(work_tx);

    // --- Bounded join over workers (parity with the other run paths) ---------
    let join_deadline = Instant::now() + SHUTDOWN_GRACE;
    while Instant::now() < join_deadline && workers.iter().any(|h| !h.is_finished()) {
        std::thread::sleep(Duration::from_millis(20));
    }
    let workers_clean = workers.iter().all(|h| h.is_finished());
    if workers_clean {
        for h in workers {
            let _ = h.join();
        }
    } else {
        warn!(
            "shutdown grace elapsed with workers still in engine calls; \
             skipping environment destroy to avoid use-after-free (leak-on-exit)"
        );
    }
    // The consumer exits once all worker senders have dropped.
    if let Ok(handle) = consumer {
        let _ = handle.join();
    }

    let watermark = resume
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .watermark();
    let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
    let rejected = ADDS_REJECTED.load(Ordering::Relaxed);
    let errors = ERRORS.load(Ordering::Relaxed);
    let written = sink
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .written();

    // "Processed total of N adds ..." keeps the prefix the e2e tests / tooling
    // scrape; the resume hint is file-mode specific.
    println!("Processed total of {adds} adds, 0 redo records (0 redo dropped, {errors} errors)");
    println!(
        "File load: {rejected} record(s) dead-lettered ({written} written to {reject_path}); \
         safe resume with --skip-lines {watermark}"
    );
    let unwritten = (rejected as u64).saturating_sub(written);

    let result = match &read_result {
        Ok(lines) => {
            info!("File loader finished: {lines} physical line(s) read from {path:?}");
            if WORKER_FATAL.load(Ordering::Relaxed) {
                Err(anyhow::anyhow!(
                    "a worker reported a fatal engine error during file load"
                ))
            } else if unwritten != 0 {
                // The load itself completed, but rejects exist only in the log
                // (e.g. a mistyped --reject-file directory). Exit non-zero so
                // the operator notices before the log rotates away.
                Err(anyhow::anyhow!(
                    "{unwritten} rejected record(s) could NOT be written to {reject_path}; \
                     their bodies are in the log"
                ))
            } else {
                Ok(())
            }
        }
        Err(e) => Err(anyhow::anyhow!("file read failed: {e:#}")),
    };
    (workers_clean, result)
}

/// Reads `path`, skips the first `skip` physical lines, and feeds each remaining
/// non-blank line to the worker pool as a [`LoadItem`] keyed by absolute line
/// number. Blank and unparseable lines are completed immediately (blank =
/// skipped, unparseable = written to the reject file) so the resume watermark
/// can advance past them. Returns the total physical line count read
/// (including skipped).
fn read_and_feed(
    path: &Path,
    skip: u64,
    work_tx: &mpsc::Sender<LoadItem>,
    resume: &Arc<Mutex<ResumeTracker>>,
    sink: &Arc<Mutex<RejectSink>>,
    in_flight: &InFlightBodies,
) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("cannot open input file {path:?}"))?;
    let reader = BufReader::new(file);

    let mut line_no: u64 = 0;
    for line in reader.lines() {
        if !RUNNING.load(Ordering::Relaxed) {
            info!("shutdown requested; stopping file read at line {line_no}");
            break;
        }
        let line = line.with_context(|| format!("read error at line {}", line_no + 1))?;
        line_no += 1;

        if line_no <= skip {
            continue; // skip-lines: not part of this run's watermark window
        }
        if line_no.is_multiple_of(PROGRESS_EVERY) {
            info!("file load progress: {line_no} lines read");
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            complete(resume, line_no); // blank line: nothing to load
            continue;
        }

        match parse_record(trimmed.as_bytes()) {
            Ok(info) => {
                let body = trimmed.as_bytes().to_vec();
                in_flight
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(line_no, body.clone());
                let item = LoadItem {
                    delivery_tag: line_no,
                    body,
                    info,
                };
                // Backpressure: blocks when the work channel is full. An Err
                // means every worker has exited (e.g. fatal) — stop reading.
                if work_tx.blocking_send(item).is_err() {
                    warn!("worker pool closed; stopping file read at line {line_no}");
                    in_flight
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&line_no);
                    break;
                }
            }
            Err(e) => {
                // Bad record: reject (log + count + side file), do not stop.
                warn!("REJECTING unparseable record at line {line_no}: {e}");
                reject_line(sink, line_no, trimmed.as_bytes());
                complete(resume, line_no);
            }
        }
    }
    Ok(line_no)
}

/// Drains worker outcomes, updates the global add counters, prints WithInfo
/// responses when requested, writes engine rejects to the reject file, and
/// advances the resume watermark. Exits when all worker result senders have
/// dropped (channel closed).
fn result_consumer(
    mut result_rx: mpsc::Receiver<Outcome>,
    want_info: bool,
    resume: Arc<Mutex<ResumeTracker>>,
    sink: Arc<Mutex<RejectSink>>,
    in_flight: InFlightBodies,
) {
    while let Some(outcome) = result_rx.blocking_recv() {
        let Outcome {
            delivery_tag,
            info,
            action,
        } = outcome;
        let body = in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&delivery_tag);
        match action {
            Action::Ack(maybe_info) => {
                if want_info && let Some(resp) = maybe_info {
                    println!("{resp}");
                }
                ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
                complete(&resume, delivery_tag);
            }
            Action::RejectNoRequeue => {
                // The worker already logged WHY (engine error text); log WHERE.
                warn!(
                    "REJECTING line {delivery_tag} ({} : {}) -> {:?}",
                    info.data_source,
                    info.record_id,
                    sink.lock().unwrap_or_else(PoisonError::into_inner).path()
                );
                match body {
                    Some(body) => reject_line(&sink, delivery_tag, &body),
                    None => {
                        // Cannot happen (inserted before dispatch); count it
                        // and say so rather than lose the reject silently.
                        ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
                        warn!("no in-flight body for rejected line {delivery_tag}; not written");
                    }
                }
                complete(&resume, delivery_tag);
            }
            Action::Fatal(msg) => {
                // Worker already set WORKER_FATAL + RUNNING=false; record it and
                // do NOT advance the watermark past this line (it did not load).
                ERRORS.fetch_add(1, Ordering::Relaxed);
                warn!("fatal engine error during file load: {msg}");
            }
        }
    }
}

fn complete(resume: &Arc<Mutex<ResumeTracker>>, line: u64) {
    resume
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .complete(line);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_advances_contiguously_and_handles_out_of_order() {
        // Run starts at line 6 (skip = 5).
        let mut t = ResumeTracker::new(6);
        assert_eq!(t.watermark(), 5, "no lines done yet -> watermark = skip");

        // Out-of-order completion: 8 done before 6/7 -> watermark stays at 5.
        t.complete(8);
        assert_eq!(t.watermark(), 5);

        // 6 done -> advances to 6 (7 still missing).
        t.complete(6);
        assert_eq!(t.watermark(), 6);

        // 7 done -> now 6,7,8 contiguous -> jumps to 8.
        t.complete(7);
        assert_eq!(t.watermark(), 8);
    }

    #[test]
    fn reject_sink_is_lazy_appends_and_counts() {
        let dir = std::env::temp_dir().join(format!("sz_reject_sink_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("in.jsonl.rejected.jsonl");
        let _ = std::fs::remove_file(&path);

        let mut sink = RejectSink::new(&path);
        assert!(
            !path.exists(),
            "sink must not create the file until first write"
        );
        assert_eq!(sink.written(), 0);

        sink.write_line(br#"{"A":1}"#).expect("write 1");
        sink.write_line(br#"{"B":2}"#).expect("write 2");
        assert_eq!(sink.written(), 2);
        drop(sink);

        // A second sink (resumed run) appends rather than truncating.
        let mut sink2 = RejectSink::new(&path);
        sink2.write_line(br#"{"C":3}"#).expect("write 3");
        drop(sink2);

        let content = std::fs::read_to_string(&path).expect("read back");
        assert_eq!(content, "{\"A\":1}\n{\"B\":2}\n{\"C\":3}\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reject_sink_reports_unwritable_path() {
        let mut sink = RejectSink::new("/nonexistent-dir-for-sz-test/x.jsonl");
        assert!(sink.write_line(b"{}").is_err());
        assert_eq!(sink.written(), 0);
    }

    #[test]
    fn watermark_ignores_lines_at_or_below_start() {
        let mut t = ResumeTracker::new(1); // skip = 0
        t.complete(1);
        t.complete(2);
        t.complete(3);
        assert_eq!(t.watermark(), 3);
        // A stale/duplicate completion below `next` is a no-op.
        t.complete(2);
        assert_eq!(t.watermark(), 3);
    }
}
