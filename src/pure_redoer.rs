//! The redo% = 100 endpoint: pure redoer, no AMQP, no tokio.
//!
//! The binary degenerates to `sz_simple_redoer_rust`'s pure `std::thread`
//! shape (design §1.1): the AMQP connection is never opened, the tokio runtime
//! is never built, and `SENZING_AMQP_URL` / `SENZING_RABBITMQ_QUEUE` may be
//! entirely unset. One fetcher thread + N redo-preferring workers + a monitor
//! loop on the main thread.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tracing::{info, warn};

use crate::config::Config;
use crate::redo::{fetcher_loop, interruptible_sleep};
use crate::stats::{
    self, ERRORS, Ewma, FloorGuard, GuardTransition, REDOS_DROPPED, REDOS_PROCESSED, RUNNING,
    SAMPLE_REDO_RECORDS, WORKER_FATAL,
};
use crate::worker::{
    Class, RedoInFlight, RedoJob, RedoSide, WorkerCtx, join_workers_bounded,
    monitor_redo_in_flight, redo_flags, worker_loop,
};

/// EWMA smoothing for the redo-backlog slope (same as the mixed path).
const SLOPE_EWMA_ALPHA: f64 = 0.3;

/// Runs the pure-redoer topology until SIGINT/SIGTERM (handled by `main`'s
/// dedicated `sigwait` thread, which flips [`RUNNING`]) or a fatal error.
///
/// Returns `(workers_clean, result)`: `workers_clean` is `true` iff every
/// engine thread finished within the shutdown grace window; when `false`,
/// `main` must SKIP the native environment destroy (redoer FIX-3).
pub fn run(config: &Config, env: Arc<SzEnvironmentCore>) -> (bool, anyhow::Result<()>) {
    let n_workers = config.threads;
    info!(
        "Pure redoer (redo% = 100): {n_workers} workers, no AMQP connection, \
         no tokio runtime"
    );

    // Channel capacity |B| + 2 with |B| = N at this endpoint (design §2.3):
    // small on purpose — fetched-but-unprocessed redo records are lost on
    // crash, so the fetcher must not run far ahead of the workers.
    let (redo_tx, redo_rx) = std::sync::mpsc::sync_channel::<RedoJob>(n_workers + 2);
    let redo_rx = Arc::new(Mutex::new(redo_rx));
    let in_flight: Arc<Mutex<RedoInFlight>> = Arc::new(Mutex::new(HashMap::new()));
    let rflags = redo_flags(config.info);

    // --- Workers --------------------------------------------------------------
    let mut handles = Vec::with_capacity(n_workers + 1);
    for worker_id in 0..n_workers {
        let ctx = WorkerCtx {
            worker_id,
            class: Class::RedoPreferring,
            env: env.clone(),
            load: None,
            redo: Some(RedoSide {
                redo_rx: redo_rx.clone(),
                in_flight: in_flight.clone(),
            }),
            add_flags: None,
            redo_flags: rflags,
            want_info: config.info,
        };
        match std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || worker_loop(ctx))
        {
            Ok(handle) => handles.push(handle),
            Err(e) => {
                RUNNING.store(false, Ordering::Relaxed);
                drop(redo_tx);
                let workers_clean = join_workers_bounded(handles);
                return (
                    workers_clean,
                    Err(anyhow::anyhow!("failed to spawn worker thread: {e}")),
                );
            }
        }
    }

    // --- Fetcher ---------------------------------------------------------------
    // The fetcher derives its own engine handle inside its thread (handles are
    // not Send); a failure there flips WORKER_FATAL/RUNNING, which the monitor
    // loop below observes.
    let fetcher_env = env.clone();
    let sleep_secs = config.redo_sleep_secs;
    match std::thread::Builder::new()
        .name("sz-redo-fetcher".to_string())
        .spawn(move || fetcher_loop(fetcher_env, redo_tx, sleep_secs, None, None))
    {
        Ok(handle) => handles.push(handle),
        Err(e) => {
            RUNNING.store(false, Ordering::Relaxed);
            let workers_clean = join_workers_bounded(handles);
            return (
                workers_clean,
                Err(anyhow::anyhow!("failed to spawn redo fetcher: {e}")),
            );
        }
    }

    // --- Monitor loop (main thread, own engine handle) -------------------------
    let monitor_engine = match env.get_engine() {
        Ok(engine) => engine,
        Err(e) => {
            RUNNING.store(false, Ordering::Relaxed);
            let workers_clean = join_workers_bounded(handles);
            return (workers_clean, Err(anyhow::Error::from(e)));
        }
    };

    let interval = Duration::from_secs((config.long_record_secs / 2).max(1));
    let mut slope = Ewma::new(SLOPE_EWMA_ALPHA);
    let mut prev_backlog: Option<i64> = None;
    let mut floor_guard = FloorGuard::default();
    let mut last_status_at = Instant::now();
    let mut prev_redos: usize = 0;

    while RUNNING.load(Ordering::Relaxed) {
        interruptible_sleep(interval);
        if !RUNNING.load(Ordering::Relaxed) {
            break;
        }

        match monitor_engine.get_stats() {
            // The prefix is MANDATORY: the harness scrapes on "Engine stats:".
            Ok(engine_stats) => println!("Engine stats: {engine_stats}"),
            Err(e) => warn!("Could not retrieve engine stats: {e}"),
        }

        // Monitoring ONLY (one table scan per interval); emptiness is always
        // detected by the fetcher's get_redo_record() coming back empty.
        let backlog = match monitor_engine.count_redo_records() {
            Ok(n) => Some(n),
            Err(e) => {
                warn!("count_redo_records failed: {e}");
                None
            }
        };

        let now = Instant::now();
        let dt = now.duration_since(last_status_at).as_secs_f64().max(0.001);
        let redos = REDOS_PROCESSED.load(Ordering::Relaxed);
        let slope_val = match (backlog, prev_backlog) {
            (Some(b), Some(p)) => Some(slope.update((b - p) as f64)),
            _ => slope.value(),
        };
        stats::emit_status_line(&stats::StatusLine {
            redo_percent: 100,
            load_pref: 0,
            redo_pref: n_workers,
            adds: 0,
            adds_rate: 0.0,
            redos,
            redos_rate: (redos - prev_redos) as f64 / dt,
            mq_depth: None, // no AMQP exists; field omitted (design §7)
            redo_backlog: backlog,
            redo_backlog_slope: slope_val,
        });

        // The redo-floor guard's "MQ is empty" conjunct is vacuously TRUE here
        // (no AMQP exists) — the 4.2.4 __REPAIR__ floor was hit precisely by
        // pure redoers at the drain tail (design §6).
        match floor_guard.observe(redos, backlog, None) {
            GuardTransition::Tripped => {
                warn!(
                    "redo-floor suspected (possible __REPAIR__ loop): redo progressing \
                     while backlog stays flat at a small value; sampling raw redo \
                     records to the log"
                );
                SAMPLE_REDO_RECORDS.store(true, Ordering::Relaxed);
            }
            GuardTransition::Cleared => {
                info!("redo-floor condition cleared; stopping redo-record sampling");
                SAMPLE_REDO_RECORDS.store(false, Ordering::Relaxed);
            }
            GuardTransition::Unchanged => {}
        }

        monitor_redo_in_flight(&in_flight, config.long_record_secs, n_workers);

        prev_backlog = backlog;
        prev_redos = redos;
        last_status_at = now;
    }

    // --- Teardown ---------------------------------------------------------------
    RUNNING.store(false, Ordering::Relaxed);
    let workers_clean = join_workers_bounded(handles);

    let redos = REDOS_PROCESSED.load(Ordering::Relaxed);
    let dropped = REDOS_DROPPED.load(Ordering::Relaxed);
    let errors = ERRORS.load(Ordering::Relaxed);
    info!("Completed processing {redos} redo records ({dropped} dropped, {errors} errors)");
    if let Ok(engine_stats) = monitor_engine.get_stats() {
        println!("Engine stats: {engine_stats}");
    }

    if WORKER_FATAL.load(Ordering::Relaxed) {
        return (
            workers_clean,
            Err(anyhow::anyhow!(
                "a worker or the redo fetcher failed fatally (could not obtain an \
                 engine handle, or hit an unrecoverable engine/DB error) — see log"
            )),
        );
    }
    (workers_clean, Ok(()))
}
