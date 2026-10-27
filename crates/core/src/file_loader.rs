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
//! * Drain cap (`--redo-drain-max-secs`, opt-in: default 0 = off): the clock
//!   starts when the reader reaches end of file. If redo has not drained by
//!   then, the fetcher stops fetching (already-fetched records still run),
//!   the workers finish, the remaining redo count is logged and the process
//!   exits `runtime::EXIT_REDO_DRAIN_CAP` (3) — non-zero although nothing
//!   failed, so a scheduler sees the backlog was left behind.
//! * SIGTERM / fatal: stop reading and fetching; bounded grace as elsewhere.
//!   A SIGTERM before the drain cap expires is a clean stop (exit 0), never
//!   a cap expiry; after the cap expired the exit is already 3 and a SIGTERM
//!   does not change it.
//! * INFO sink (optional): flushed every `--info-sink-flush-secs` by its own
//!   timer thread (so timed seals also happen while the reader is blocked
//!   feeding a busy pool), stopped before the final close, and closed explicitly after the pool is joined (or the grace
//!   ran out), before `main` tears the engine down. Any sink error is fatal.
//!   With a sink, the printed resume point is the watermark captured just
//!   before the last SUCCESSFUL seal (flush or close): every line up to it
//!   has its INFO in a sealed file. After a sink failure the run exits
//!   non-zero and the resume point stays there, never past it.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sz_rust_sdk::prelude::*;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{Config, redo_preferring_count};
use crate::info_sink::{CLOSE_TIMEOUT, FlushTimer, InfoSinkHandle, fail_fatal};
use crate::record::parse_record;
use crate::redo::{DrainExit, fetcher_loop};
use crate::stats::{
    self, ADDS_PROCESSED, ADDS_REJECTED, ERRORS, REDO_DRAIN_CAP_EXPIRED, REDOS_PROCESSED, RUNNING,
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

/// The `--skip-lines` value to print. Without a sink it is the completion
/// watermark; with one it is capped at `sealed`, the watermark captured just
/// before the last successful seal, so a resume never skips a line whose
/// INFO did not reach a sealed file.
fn resume_point(sink_enabled: bool, sealed: u64, watermark: u64) -> u64 {
    if sink_enabled {
        sealed.min(watermark)
    } else {
        watermark
    }
}

/// Poll step of the flush thread (bounds how late a stop is noticed).
const FLUSH_POLL: Duration = Duration::from_millis(100);

/// Timed INFO sink flushes on their own thread. Each successful flush
/// publishes the watermark captured just before it into `sealed`; a failed
/// flush is fatal and ends the thread.
fn flush_loop(
    sink: InfoSinkHandle,
    every: Duration,
    resume: Arc<Mutex<ResumeTracker>>,
    sealed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let mut timer = FlushTimer::new(every);
    let running = || !stop.load(Ordering::Acquire) && RUNNING.load(Ordering::Relaxed);
    while running() {
        std::thread::sleep(FLUSH_POLL.min(every));
        if !running() || !timer.due() {
            continue;
        }
        // Captured BEFORE the seal: every line up to it was acknowledged
        // after its INFO write returned, so the seal covers it.
        let wm = watermark(&resume);
        match sink.flush() {
            Ok(()) => sealed.store(wm, Ordering::Release),
            Err(e) => {
                fail_fatal(&e);
                break;
            }
        }
    }
}

/// Stops the flush thread and waits up to `timeout` for it (a flush in
/// progress finishes first, so it can never race the final close). `false`:
/// it is stuck inside a flush.
fn stop_flusher(
    flusher: Option<(Arc<AtomicBool>, std::thread::JoinHandle<()>)>,
    timeout: Duration,
) -> bool {
    let Some((stop, handle)) = flusher else {
        return true;
    };
    stop.store(true, Ordering::Release);
    let deadline = Instant::now() + timeout;
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = handle.join();
    true
}

/// Current completion watermark.
fn watermark(resume: &Arc<Mutex<ResumeTracker>>) -> u64 {
    resume
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .watermark()
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
             the process exits once redo reads empty on {} consecutive probes {}s apart \
             (drain cap: {})",
            crate::redo::DRAIN_EXIT_EMPTY_PROBES,
            config.redo_sleep_secs,
            config
                .redo_drain_max
                .map_or("none".to_string(), |d| format!(
                    "{}s after EOF",
                    d.as_secs()
                ))
        );
    }
    if redo_enabled
        && crate::redo::cap_shorter_than_drain_rule(config.redo_drain_max, config.redo_sleep_secs)
    {
        warn!(
            "--redo-drain-max-secs ({}s) is shorter than the drain rule's {} x \
             --redo-sleep-secs ({}s): the cap will usually expire before an empty redo queue \
             is confirmed; a final probe then decides (exit 0 if drained, 3 if not)",
            config.redo_drain_max.map_or(0, |d| d.as_secs()),
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
    let add_flags = add_record_flags(config.with_info());
    let want_info = config.info;

    let mut handles = Vec::with_capacity(n_workers + 1);

    // --- Redo side (only when redo% > 0; queue-mode shape verbatim) ----------
    let redo_in_flight: Arc<Mutex<RedoInFlight>> = Arc::new(Mutex::new(HashMap::new()));
    let drain = DrainExit::new(config.redo_drain_max);
    let input_done = drain.input_done.clone();
    let redo_side = if redo_enabled {
        let (redo_tx, redo_rx) = std::sync::mpsc::sync_channel::<RedoJob>(redo_pref + 2);
        let fetcher_env = env.clone();
        let sleep_secs = config.redo_sleep_secs;
        let drain = drain.clone();
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
            redo_flags: redo_flags(config.with_info()),
            want_info,
            transform: config.transform.clone(),
            info_sink: config.info_sink.clone(),
            redo_stats_interval: config.redo_stats_interval,
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

    // --- INFO sink flush timer (own thread: runs during the load too) --------
    // Watermark covered by sealed INFO files (see `resume_point`).
    let sealed = Arc::new(AtomicU64::new(skip));
    let flusher = if config.info_sink.is_enabled() {
        let stop = Arc::new(AtomicBool::new(false));
        let (sink, every, resume, sealed, stop2) = (
            config.info_sink.clone(),
            config.info_sink_flush,
            resume.clone(),
            sealed.clone(),
            stop.clone(),
        );
        match std::thread::Builder::new()
            .name("sz-info-flush".to_string())
            .spawn(move || flush_loop(sink, every, resume, sealed, stop2))
        {
            Ok(h) => Some((stop, h)),
            Err(e) => {
                fail_fatal(&format!("failed to spawn the INFO sink flush thread: {e}"));
                None
            }
        }
    } else {
        None
    };

    // --- Reader: skip, then feed each line to the worker pool ----------------
    let read_result = read_and_feed(Path::new(&path), skip, &work_tx, &resume, &sink, &in_flight);
    let dispatched = read_result.as_ref().map_or(0, |r| r.dispatched);
    if read_result.is_ok() {
        // End of input: the redo drain cap's clock starts now.
        drain.mark_input_read();
    } else {
        // A broken input is a failed run: stop the fetcher/workers promptly
        // instead of draining redo first and reporting the error afterwards.
        RUNNING.store(false, Ordering::Relaxed);
    }

    // EOF / stop: closing work_tx lets the workers observe end-of-stream on the
    // load channel; with a redo side they now fall through to redo (the ramp).
    drop(work_tx);
    info!(
        "file input exhausted ({dispatched} record(s) dispatched){}",
        if redo_enabled {
            "; ramping all workers to redo"
        } else {
            ""
        }
    );

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
    // Final seal: after the join (or the grace), before the engine teardown in
    // `main`. Explicit, because the forced-exit paths never run destructors.
    // Bounded (CLOSE_TIMEOUT); a stuck close is fatal and takes the
    // leak-on-exit path.
    let flusher_done = stop_flusher(flusher, CLOSE_TIMEOUT);
    if !flusher_done {
        fail_fatal("INFO sink is stuck: a timed sz_is_flush did not return");
    }
    let wm = watermark(&resume);
    let (closed, stuck) = if flusher_done {
        config.info_sink.close_or_fail()
    } else {
        (false, true)
    };
    if closed {
        sealed.store(wm, Ordering::Release);
    }
    let workers_clean = workers_clean && !stuck;
    let sealed = sealed.load(Ordering::Acquire);
    if REDO_DRAIN_CAP_EXPIRED.load(Ordering::Relaxed) {
        log_remaining_redo(&env, workers_clean, config.redo_drain_max);
    }

    let completed = watermark(&resume);
    let watermark = resume_point(config.info_sink.is_enabled(), sealed, completed);
    if watermark < completed {
        warn!(
            "INFO sink failed: resume point held at line {watermark}, the last line covered \
             by a sealed INFO file (lines up to {completed} completed)"
        );
    }
    let adds = ADDS_PROCESSED.load(Ordering::Relaxed);
    let rejected = ADDS_REJECTED.load(Ordering::Relaxed);
    let written = sink
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .written();

    // The SAME "Processed total of N adds, ..." line the queue paths print
    // (e2e tests / tooling scrape it); the resume hint is file-mode specific.
    crate::stats::print_final_totals(adds as u64);
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

/// Logs how much redo the drain cap left queued. Counted once, after the pool
/// stopped (nothing in this process can add more), with a fresh engine handle;
/// skipped when the run is not clean — a worker still inside the engine (the
/// database may be the reason it is stuck) or a stuck INFO sink close — since
/// that run takes the leak-on-exit path and must not start new engine work.
fn log_remaining_redo(env: &Arc<SzEnvironmentCore>, workers_clean: bool, cap: Option<Duration>) {
    let cap = cap.map_or(0, |d| d.as_secs());
    let remaining = if workers_clean {
        env.get_engine()
            .and_then(|engine| engine.count_redo_records())
            .map_or_else(|e| format!("unknown ({e})"), |n| n.to_string())
    } else {
        "unknown (a worker or the INFO sink close is still running)".to_string()
    };
    warn!(
        "REDO DRAIN CAP EXPIRED: redo still not drained {cap}s after the input was read; \
         stopped fetching redo with {remaining} redo record(s) remaining in the queue"
    );
}

/// What [`read_and_feed`] got through: physical lines read (incl. skipped) and
/// records handed to the worker pool (each owes exactly one [`Outcome`]).
struct ReadSummary {
    lines: u64,
    dispatched: u64,
}

/// zstd frame magic `0xFD2FB528` (little-endian on disk).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// True when `prefix` (the file's first bytes) opens a zstd frame or skippable
/// frame (`0x184D2A50..=0x184D2A5F`).
fn is_zstd_magic(prefix: &[u8]) -> bool {
    prefix.len() == 4
        && (prefix == ZSTD_MAGIC || (prefix[0] & 0xF0 == 0x50 && prefix[1..] == [0x2A, 0x4D, 0x18]))
}

/// Opens the input for line reading. Plain JSONL is read as-is; zstd (`*.zst`,
/// one or many concatenated frames, e.g. Spark/zstd-jni part files or `zstd`
/// CLI output) is decoded on the fly. Detection is by magic bytes, with a
/// `.zst`/`.zstd` extension as a fallback so a non-empty file with a damaged
/// header fails loudly rather than loading binary as records. Physical line
/// numbers (resume, reject file) count decoded lines, so they are identical
/// to the same data uncompressed.
fn open_input(path: &Path) -> Result<Box<dyn BufRead>> {
    let mut file = File::open(path).with_context(|| format!("cannot open input file {path:?}"))?;
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        let n = file
            .read(&mut prefix[filled..])
            .with_context(|| format!("cannot read input file {path:?}"))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    let by_extension = filled > 0
        && path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("zst") || e.eq_ignore_ascii_case("zstd"));
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("cannot rewind input file {path:?}"))?;
    if is_zstd_magic(&prefix[..filled]) || by_extension {
        // The Decoder reads concatenated frames to the end (single-frame mode is opt-in).
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("cannot start zstd decoding of {path:?}"))?;
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
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
    let reader = open_input(path)?;

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
            Action::RejectNoRequeue(_) => {
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

    /// The real file-sink plugin (a dev-dependency cdylib next to this test).
    fn file_sink_plugin() -> String {
        let deps = std::env::current_exe().unwrap();
        let p = deps.parent().unwrap().join(format!(
            "{}sz_info_sink_file.{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_EXTENSION
        ));
        assert!(p.exists(), "file-sink plugin not built at {p:?}");
        p.to_string_lossy().into_owned()
    }

    /// M7: the flush thread seals on its own timer and publishes the
    /// watermark captured before the seal; stopping it waits for it.
    #[test]
    fn flush_thread_seals_on_its_timer_and_publishes_the_watermark() {
        use crate::info_sink::InfoKind;
        let dir = std::env::temp_dir().join(format!("sz_flush_thread_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = serde_json::json!({"dir": dir, "prefix": "t-", "rotate_records": 1000, "rotate_secs": 3600})
            .to_string();
        let sink = InfoSinkHandle::load(Some(&file_sink_plugin()), Some(&cfg)).unwrap();
        let resume = Arc::new(Mutex::new(ResumeTracker::new(1)));
        let sealed = Arc::new(AtomicU64::new(0));
        sink.write(InfoKind::Add, "{}").unwrap();
        complete(&resume, 1);
        complete(&resume, 2);
        let stop = Arc::new(AtomicBool::new(false));
        let (s2, r2, sl2, st2) = (sink.clone(), resume.clone(), sealed.clone(), stop.clone());
        let h = std::thread::spawn(move || flush_loop(s2, Duration::from_millis(50), r2, sl2, st2));
        let deadline = Instant::now() + Duration::from_secs(5);
        while sealed.load(Ordering::Acquire) != 2 {
            assert!(Instant::now() < deadline, "no timed flush");
            std::thread::sleep(Duration::from_millis(20));
        }
        // The seal happened while "the reader" (this thread) did nothing.
        assert!(dir.join("t-000001.jsonl").exists());
        sink.write(InfoKind::Add, "{}").unwrap();
        assert!(stop_flusher(Some((stop, h)), Duration::from_secs(5)));
        sink.close().unwrap();
        assert!(dir.join("t-000002.jsonl").exists(), "a second timed seal");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn stop_flusher_reports_a_stuck_flush() {
        assert!(stop_flusher(None, Duration::ZERO), "no flusher");
        let (release, rx) = std::sync::mpsc::channel::<()>();
        let stuck = std::thread::spawn(move || {
            let _ = rx.recv();
        });
        let stop = Arc::new(AtomicBool::new(false));
        assert!(!stop_flusher(
            Some((stop.clone(), stuck)),
            Duration::from_millis(50)
        ));
        assert!(stop.load(Ordering::Acquire), "told to stop");
        release.send(()).unwrap();
    }

    /// Everything `read_and_feed` produces for one input file.
    struct FeedResult {
        lines: u64,
        dispatched: u64,
        items: Vec<(u64, Vec<u8>)>,
        watermark: u64,
        rejected: String,
    }

    fn feed(path: &Path, skip: u64, tag: &str) -> FeedResult {
        let reject = path.with_extension(format!("{tag}.rejected"));
        let _ = std::fs::remove_file(&reject);
        let (tx, mut rx) = mpsc::channel::<LoadItem>(100_000);
        let resume = Arc::new(Mutex::new(ResumeTracker::new(skip + 1)));
        let sink = Arc::new(Mutex::new(RejectSink::new(&reject)));
        let in_flight: InFlightBodies = Arc::new(Mutex::new(HashMap::new()));
        let summary =
            read_and_feed(path, skip, &tx, &resume, &sink, &in_flight).expect("read_and_feed");
        drop(tx);
        let mut items = Vec::new();
        while let Some(i) = rx.blocking_recv() {
            items.push((i.delivery_tag, i.body));
        }
        let watermark = resume.lock().unwrap().watermark();
        let rejected = std::fs::read_to_string(&reject).unwrap_or_default();
        let _ = std::fs::remove_file(&reject);
        FeedResult {
            lines: summary.lines,
            dispatched: summary.dispatched,
            items,
            watermark,
            rejected,
        }
    }

    /// A `.zst` input (crate-written multi-frame and, when present, `zstd` CLI
    /// output) must give the same lines, dispatched records, reject output and
    /// resume watermark as the plain file, including with a resume skip.
    #[test]
    fn zstd_input_matches_plain_input_for_feed_resume_and_rejects() {
        let dir = std::env::temp_dir().join(format!("sz_zstd_input_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 6,000 records > one decoder buffer, with blank and unparseable lines.
        let mut text = String::new();
        for i in 0..6000 {
            match i % 1000 {
                7 => text.push('\n'),
                13 => text.push_str("not json\n"),
                _ => text.push_str(&format!(
                    "{{\"DATA_SOURCE\":\"T\",\"RECORD_ID\":\"{i}\",\"NAME_FULL\":\"n\u{e9} {i}\"}}\n"
                )),
            }
        }
        let plain = dir.join("in.jsonl");
        std::fs::write(&plain, &text).unwrap();

        // Crate-written, three concatenated frames split mid-line.
        let b = text.as_bytes();
        let mut multi = Vec::new();
        for part in [&b[..1001], &b[1001..200_001], &b[200_001..]] {
            multi.extend(zstd::encode_all(part, 3).unwrap());
        }
        let zst = dir.join("in.jsonl.zst");
        std::fs::write(&zst, &multi).unwrap();
        let mut variants = vec![zst];
        // The same bytes under a non-.zst name: detected by magic.
        let magic_only = dir.join("in.dat");
        std::fs::write(&magic_only, &multi).unwrap();
        variants.push(magic_only);
        // Real `zstd` CLI output, when the CLI is installed.
        let cli = dir.join("cli.jsonl.zst");
        if let Ok(st) = std::process::Command::new("zstd")
            .args(["-q", "-3", "-o"])
            .arg(&cli)
            .arg(&plain)
            .status()
        {
            assert!(st.success(), "zstd CLI failed");
            variants.push(cli);
        }

        for skip in [0u64, 2500] {
            let want = feed(&plain, skip, "plain");
            assert!(want.dispatched > 3000 && !want.rejected.is_empty());
            for v in &variants {
                let got = feed(v, skip, "zst");
                assert_eq!(got.lines, want.lines, "{v:?} skip {skip}: line count");
                assert_eq!(got.dispatched, want.dispatched, "{v:?} skip {skip}");
                assert_eq!(got.items, want.items, "{v:?} skip {skip}: records");
                assert_eq!(got.watermark, want.watermark, "{v:?} skip {skip}");
                assert_eq!(got.rejected, want.rejected, "{v:?} skip {skip}: rejects");
            }
        }

        // A damaged .zst is a read error, never records.
        let bad = dir.join("bad.zst");
        std::fs::write(&bad, b"{\"DATA_SOURCE\":\"T\"}\n").unwrap();
        let (tx, _rx) = mpsc::channel::<LoadItem>(10);
        let resume = Arc::new(Mutex::new(ResumeTracker::new(1)));
        let sink = Arc::new(Mutex::new(RejectSink::new(dir.join("bad.rejected"))));
        let in_flight: InFlightBodies = Arc::new(Mutex::new(HashMap::new()));
        assert!(read_and_feed(&bad, 0, &tx, &resume, &sink, &in_flight).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_point_never_passes_the_last_seal_with_a_sink() {
        assert_eq!(resume_point(false, 0, 42), 42, "no sink: the watermark");
        assert_eq!(resume_point(true, 42, 42), 42, "sink sealed everything");
        assert_eq!(resume_point(true, 10, 42), 10, "seal failed after line 10");
        assert_eq!(resume_point(true, 50, 42), 42, "never past the watermark");
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
