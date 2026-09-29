//! File-input mode: read newline-delimited JSON (JSONL) records from a single
//! file and feed them through the SAME worker pool, redo fetcher and split rule
//! the queue paths use (`worker::worker_loop`, `redo::fetcher_loop`,
//! `config::redo_preferring_count`). Selected by `--file`/`SENZING_INPUT_FILE`;
//! mutually exclusive with the AMQP `--url`.
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
//! ## redo (shared exactly as in queue mode)
//! The point of this driver is to do redo IN PARALLEL with load and then
//! switch to ALL redo when the load is done — file input included. So in file
//! mode redo% of the threads process redo during the load, every thread moves
//! to redo at end-of-file, and the process exits when redo is drained.
//!
//! * redo% = 0: pure loader — no fetcher, zero redo calls; exits at EOF.
//! * redo% > 0: |B| = `redo_preferring_count(N, redo%)` workers prefer redo,
//!   the rest prefer load, all with cross-over (`mixed_loop`), and one redo
//!   fetcher runs for the whole process — identical to queue mode. While the
//!   file loads, redo gets its |B|/N share. At EOF the load channel closes, so
//!   every load-preferring worker falls through to redo: the pool ramps to
//!   100% redo with no mode switch (the same mechanism as an empty MQ).
//! * Termination (file mode only — queue mode never exits on idle): once the
//!   file is exhausted and every dispatched record has reported an outcome, the
//!   fetcher stops after `redo::DRAIN_EXIT_EMPTY_PROBES` consecutive empty
//!   `get_redo_record()` probes `--redo-sleep-secs` apart with no redo
//!   outstanding in this process (see `redo` module docs). That closes the redo
//!   channel, the workers exit, and the process exits 0.
//! * SIGTERM / fatal: stop reading and fetching; bounded grace as elsewhere.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sz_rust_sdk::prelude::*;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{Config, redo_preferring_count};
use crate::record::parse_record;
use crate::redo::{DrainExit, fetcher_loop};
use crate::stats::{
    self, ADDS_PROCESSED, ADDS_REJECTED, ERRORS, REDOS_DROPPED, REDOS_PROCESSED, RUNNING,
    WORKER_FATAL,
};
use crate::worker::{
    Action, Class, LoadItem, LoadSide, Outcome, RedoInFlight, RedoJob, RedoSide, SHUTDOWN_GRACE,
    WorkerCtx, add_record_flags, monitor_redo_in_flight, redo_flags, worker_class, worker_loop,
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

/// Worker plan for file mode: the class of each of `threads` workers and
/// whether a redo side (fetcher + redo channel) exists. Identical to the queue
/// split: |B| = `redo_preferring_count(threads, redo%)` redo-preferring
/// workers, a redo side iff redo% > 0.
pub fn worker_plan(threads: usize, redo_percent: u8) -> (Vec<Class>, bool) {
    let redo_pref = redo_preferring_count(threads, redo_percent);
    let classes = (0..threads).map(|id| worker_class(id, redo_pref)).collect();
    (classes, redo_percent > 0)
}

/// The load input is done (redo drain may begin to count toward exit) once the
/// reader has stopped AND every record it dispatched has reported an outcome:
/// an `add_record` still in flight can enqueue redo.
fn load_input_done(reader_finished: bool, dispatched: u64, outcomes: u64) -> bool {
    reader_finished && outcomes >= dispatched
}

/// Runs file mode to completion (EOF, plus redo drain when redo% > 0) or
/// SIGTERM, and returns `(workers_clean, result)` mirroring
/// [`crate::pure_redoer::run`], so `main` can share the bounded-teardown exit
/// path.
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

    let (classes, redo_enabled) = worker_plan(n_workers, config.redo_percent);
    let redo_pref = config.redo_pref_workers();

    info!(
        "File loader: reading {path:?} with {n_workers} workers (load-preferring: {}, \
         redo-preferring: {redo_pref}, redo%: {}, skip-lines: {skip}); \
         rejected lines are appended to {reject_path:?}",
        n_workers - redo_pref,
        config.redo_percent
    );
    if redo_enabled {
        info!(
            "redo is processed while the file loads; at EOF all workers drain redo and \
             the process exits once redo reads empty on {} consecutive probes {}s apart",
            crate::redo::DRAIN_EXIT_EMPTY_PROBES,
            config.redo_sleep_secs
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

    let mut handles = Vec::with_capacity(n_workers + 1);

    // --- Redo side (only when redo% > 0; queue-mode shape verbatim) ----------
    let redo_in_flight: Arc<Mutex<RedoInFlight>> = Arc::new(Mutex::new(HashMap::new()));
    let input_done = Arc::new(AtomicBool::new(false));
    let redo_side = if redo_enabled {
        let (redo_tx, redo_rx) = std::sync::mpsc::sync_channel::<RedoJob>(redo_pref + 2);
        let fetcher_env = env.clone();
        let sleep_secs = config.redo_sleep_secs;
        let drain = DrainExit {
            input_done: input_done.clone(),
        };
        // No result_tx/notify: the main thread polls RUNNING/WORKER_FATAL (the
        // pure-redoer arrangement), so the result channel closes with the
        // workers alone.
        match std::thread::Builder::new()
            .name("sz-redo-fetcher".to_string())
            .spawn(move || fetcher_loop(fetcher_env, redo_tx, sleep_secs, None, None, Some(drain)))
        {
            Ok(h) => handles.push(h),
            Err(e) => {
                RUNNING.store(false, Ordering::Relaxed);
                return (
                    true,
                    Err(anyhow::anyhow!("failed to spawn redo fetcher: {e}")),
                );
            }
        }
        Some(RedoSide {
            redo_rx: Arc::new(Mutex::new(redo_rx)),
            in_flight: redo_in_flight.clone(),
        })
    } else {
        None
    };

    // --- Spawn workers (queue-mode split) ------------------------------------
    for (worker_id, class) in classes.into_iter().enumerate() {
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
            redo_flags: redo_flags(config.info),
            want_info,
            transform: config.transform.clone(),
        };
        match std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || worker_loop(ctx))
        {
            Ok(h) => handles.push(h),
            Err(e) => {
                RUNNING.store(false, Ordering::Relaxed);
                drop(redo_side);
                drop(work_tx);
                let workers_clean = crate::worker::join_workers_bounded(handles);
                return (
                    workers_clean,
                    Err(anyhow::anyhow!("failed to spawn worker thread: {e}")),
                );
            }
        }
    }
    drop(redo_side);

    // --- Result consumer (counts outcomes; maintains the resume watermark) ---
    let resume = Arc::new(Mutex::new(ResumeTracker::new(skip + 1)));
    let sink = Arc::new(Mutex::new(RejectSink::new(&reject_path)));
    let in_flight: InFlightBodies = Arc::new(Mutex::new(HashMap::new()));
    let outcomes = Arc::new(AtomicU64::new(0));
    let consumer_resume = resume.clone();
    let consumer_sink = sink.clone();
    let consumer_in_flight = in_flight.clone();
    let consumer_outcomes = outcomes.clone();
    let consumer = std::thread::Builder::new()
        .name("sz-file-result".to_string())
        .spawn(move || {
            result_consumer(
                result_rx,
                want_info,
                consumer_resume,
                consumer_sink,
                consumer_in_flight,
                consumer_outcomes,
            )
        });

    // Drop our extra result sender so the channel closes once all workers exit.
    drop(result_tx);

    // --- Reader: skip, then feed each line to the worker pool ----------------
    let read_result = read_and_feed(Path::new(&path), skip, &work_tx, &resume, &sink, &in_flight);
    let dispatched = read_result.as_ref().map_or(0, |r| r.dispatched);
    if read_result.is_err() {
        // A broken input is a failed run: stop the fetcher/workers promptly
        // instead of draining redo first and reporting the error afterwards.
        RUNNING.store(false, Ordering::Relaxed);
    }

    // EOF / stop: closing work_tx lets the workers observe end-of-stream on the
    // load channel; with a redo side they now fall through to redo (the ramp).
    drop(work_tx);
    if redo_enabled {
        info!(
            "file input exhausted ({dispatched} record(s) dispatched); ramping all workers to redo"
        );
    }

    // --- Wait for the pool: unbounded while running, bounded after a stop ----
    let monitor_interval = Duration::from_secs((config.long_record_secs / 2).max(1));
    let mut last_monitor = Instant::now();
    let mut prev_adds = 0usize;
    let mut prev_redos = 0usize;
    let mut stop_deadline: Option<Instant> = None;
    let mut input_done_set = false;
    while handles.iter().any(|h| !h.is_finished()) {
        if !input_done_set
            && (load_input_done(true, dispatched, outcomes.load(Ordering::Acquire))
                || !RUNNING.load(Ordering::Relaxed))
        {
            input_done.store(true, Ordering::Release);
            input_done_set = true;
            if redo_enabled {
                info!("all file records completed; draining redo");
            }
        }
        if !RUNNING.load(Ordering::Relaxed) {
            let deadline = *stop_deadline.get_or_insert_with(|| Instant::now() + SHUTDOWN_GRACE);
            if Instant::now() >= deadline {
                break;
            }
        }
        if redo_enabled && last_monitor.elapsed() >= monitor_interval {
            let dt = last_monitor.elapsed().as_secs_f64().max(0.001);
            let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
            let redos = REDOS_PROCESSED.load(Ordering::Relaxed);
            stats::emit_status_line(&stats::StatusLine {
                redo_percent: config.redo_percent,
                load_pref: n_workers - redo_pref,
                redo_pref,
                adds,
                adds_rate: adds.saturating_sub(prev_adds) as f64 / dt,
                redos,
                redos_rate: redos.saturating_sub(prev_redos) as f64 / dt,
                mq_depth: None, // no MQ in file mode
                redo_backlog: None,
                redo_backlog_slope: None,
            });
            monitor_redo_in_flight(&redo_in_flight, config.long_record_secs, redo_pref);
            prev_adds = adds;
            prev_redos = redos;
            last_monitor = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let workers_clean = handles.iter().all(|h| h.is_finished());
    if workers_clean {
        for h in handles {
            let _ = h.join();
        }
    } else {
        warn!(
            "shutdown grace elapsed with workers still in engine calls; \
             skipping environment destroy to avoid use-after-free (leak-on-exit)"
        );
    }
    // The consumer exits once all worker senders have dropped.
    if workers_clean && let Ok(handle) = consumer {
        let _ = handle.join();
    }

    let watermark = resume
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .watermark();
    let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
    let rejected = ADDS_REJECTED.load(Ordering::Relaxed);
    let redos = REDOS_PROCESSED.load(Ordering::Relaxed);
    let redos_dropped = REDOS_DROPPED.load(Ordering::Relaxed);
    let errors = ERRORS.load(Ordering::Relaxed);
    let written = sink
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .written();

    // "Processed total of N adds, M redo records (...)" is the SAME line the
    // queue paths print (e2e tests / tooling scrape it); the resume hint is
    // file-mode specific.
    println!(
        "Processed total of {adds} adds, {redos} redo records ({redos_dropped} redo dropped, \
         {errors} errors)"
    );
    println!(
        "File load: {rejected} record(s) dead-lettered ({written} written to {reject_path}); \
         safe resume with --skip-lines {watermark}"
    );
    let unwritten = (rejected as u64).saturating_sub(written);

    let result = match &read_result {
        Ok(read) => {
            info!(
                "File loader finished: {} physical line(s) read from {path:?}",
                read.lines
            );
            if WORKER_FATAL.load(Ordering::Relaxed) {
                Err(anyhow::anyhow!(
                    "a worker or the redo fetcher reported a fatal engine error in file mode"
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

/// What [`read_and_feed`] got through: physical lines read (incl. skipped) and
/// records handed to the worker pool (each owes exactly one [`Outcome`]).
struct ReadSummary {
    lines: u64,
    dispatched: u64,
}

/// Reads `path`, skips the first `skip` physical lines, and feeds each remaining
/// non-blank line to the worker pool as a [`LoadItem`] keyed by absolute line
/// number. Blank and unparseable lines are completed immediately (blank =
/// skipped, unparseable = written to the reject file) so the resume watermark
/// can advance past them.
fn read_and_feed(
    path: &Path,
    skip: u64,
    work_tx: &mpsc::Sender<LoadItem>,
    resume: &Arc<Mutex<ResumeTracker>>,
    sink: &Arc<Mutex<RejectSink>>,
    in_flight: &InFlightBodies,
) -> Result<ReadSummary> {
    let file = File::open(path).with_context(|| format!("cannot open input file {path:?}"))?;
    let reader = BufReader::new(file);

    let mut line_no: u64 = 0;
    let mut dispatched: u64 = 0;
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
                dispatched += 1;
            }
            Err(e) => {
                // Bad record: reject (log + count + side file), do not stop.
                warn!("REJECTING unparseable record at line {line_no}: {e}");
                reject_line(sink, line_no, trimmed.as_bytes());
                complete(resume, line_no);
            }
        }
    }
    Ok(ReadSummary {
        lines: line_no,
        dispatched,
    })
}

/// Drains worker outcomes, updates the global add counters, prints WithInfo
/// responses when requested, writes engine rejects to the reject file,
/// advances the resume watermark and counts per-record outcomes into
/// `outcomes` (the input-done signal). Exits when all worker result senders
/// have dropped (channel closed).
fn result_consumer(
    mut result_rx: mpsc::Receiver<Outcome>,
    want_info: bool,
    resume: Arc<Mutex<ResumeTracker>>,
    sink: Arc<Mutex<RejectSink>>,
    in_flight: InFlightBodies,
    outcomes: Arc<AtomicU64>,
) {
    while let Some(outcome) = result_rx.blocking_recv() {
        let Outcome {
            delivery_tag,
            info,
            action,
        } = outcome;
        // delivery_tag 0 is the engine-init / redo fatal sentinel (lines start
        // at 1), not a dispatched record.
        if delivery_tag != 0 {
            outcomes.fetch_add(1, Ordering::Release);
        }
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
                warn!("fatal engine error in file mode: {msg}");
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

    /// Owner ruling 2026-09-29: file mode must share redo exactly like queue
    /// mode. The old pure loader gave every worker `Class::LoadPreferring` and
    /// no redo side regardless of redo%.
    #[test]
    fn file_mode_worker_plan_matches_queue_split() {
        // The measured fleet arm: 16 threads, redo% 20 -> |B| = 3.
        let (classes, redo) = worker_plan(16, 20);
        assert!(redo, "redo% > 0 must start the redo fetcher in file mode");
        let b = classes
            .iter()
            .filter(|c| **c == Class::RedoPreferring)
            .count();
        assert_eq!(b, redo_preferring_count(16, 20));
        assert_eq!(b, 3);
        assert!(classes[..3].iter().all(|c| *c == Class::RedoPreferring));
        assert!(classes[3..].iter().all(|c| *c == Class::LoadPreferring));

        // redo% = 0 keeps the pure loader: no redo side, all load-preferring.
        let (classes, redo) = worker_plan(12, 0);
        assert!(!redo);
        assert!(classes.iter().all(|c| *c == Class::LoadPreferring));

        // redo% = 100: every worker prefers redo; the file still loads via
        // cross-over whenever the redo channel is empty.
        let (classes, redo) = worker_plan(4, 100);
        assert!(redo);
        assert!(classes.iter().all(|c| *c == Class::RedoPreferring));
    }

    #[test]
    fn input_done_waits_for_every_dispatched_outcome() {
        assert!(!load_input_done(false, 0, 0), "reader still running");
        assert!(
            !load_input_done(true, 10, 9),
            "one add_record still in flight"
        );
        assert!(load_input_done(true, 10, 10));
        assert!(load_input_done(true, 0, 0), "empty file");
    }

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
