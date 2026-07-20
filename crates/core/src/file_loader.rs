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
//! ## redo
//! File mode is a PURE loader — it does not process the engine redo queue
//! (`redo%` is ignored, with a warning). Drain redo separately with a
//! `redo% = 100` run once the file load completes.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
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

    info!(
        "File loader: reading {path:?} with {n_workers} workers (skip-lines: {skip}); \
         redo is NOT processed in file mode"
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
    let consumer_resume = resume.clone();
    let consumer = std::thread::Builder::new()
        .name("sz-file-result".to_string())
        .spawn(move || result_consumer(result_rx, want_info, consumer_resume));

    // Drop our extra result sender so the channel closes once all workers exit.
    drop(result_tx);

    // --- Reader: skip, then feed each line to the worker pool ----------------
    let read_result = read_and_feed(Path::new(&path), skip, &work_tx, &resume);

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

    // "Processed total of N adds ..." keeps the prefix the e2e tests / tooling
    // scrape; the resume hint is file-mode specific.
    println!("Processed total of {adds} adds, 0 redo records (0 redo dropped, {errors} errors)");
    println!(
        "File load: {rejected} record(s) dead-lettered; safe resume with --skip-lines {watermark}"
    );

    let result = match &read_result {
        Ok(lines) => {
            info!("File loader finished: {lines} physical line(s) read from {path:?}");
            if WORKER_FATAL.load(Ordering::Relaxed) {
                Err(anyhow::anyhow!(
                    "a worker reported a fatal engine error during file load"
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
/// skipped, unparseable = dead-lettered) so the resume watermark can advance
/// past them. Returns the total physical line count read (including skipped).
fn read_and_feed(
    path: &Path,
    skip: u64,
    work_tx: &mpsc::Sender<LoadItem>,
    resume: &Arc<Mutex<ResumeTracker>>,
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
                let item = LoadItem {
                    delivery_tag: line_no,
                    body: trimmed.as_bytes().to_vec(),
                    info,
                };
                // Backpressure: blocks when the work channel is full. An Err
                // means every worker has exited (e.g. fatal) — stop reading.
                if work_tx.blocking_send(item).is_err() {
                    warn!("worker pool closed; stopping file read at line {line_no}");
                    break;
                }
            }
            Err(e) => {
                // Bad record: dead-letter (log + count), do not stop the load.
                warn!("dead-lettering unparseable record at line {line_no}: {e}");
                ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
                complete(resume, line_no);
            }
        }
    }
    Ok(line_no)
}

/// Drains worker outcomes, updates the global add counters, prints WithInfo
/// responses when requested, and advances the resume watermark. Exits when all
/// worker result senders have dropped (channel closed).
fn result_consumer(
    mut result_rx: mpsc::Receiver<Outcome>,
    want_info: bool,
    resume: Arc<Mutex<ResumeTracker>>,
) {
    while let Some(outcome) = result_rx.blocking_recv() {
        let Outcome {
            delivery_tag,
            info: _,
            action,
        } = outcome;
        match action {
            Action::Ack(maybe_info) => {
                if want_info && let Some(resp) = maybe_info {
                    println!("{resp}");
                }
                ADDS_PROCESSED.fetch_add(1, Ordering::Relaxed);
                complete(&resume, delivery_tag);
            }
            Action::RejectNoRequeue => {
                ADDS_REJECTED.fetch_add(1, Ordering::Relaxed);
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
