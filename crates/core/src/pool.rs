//! Queue-mode engine-pool bring-up shared by every async backend (RabbitMQ,
//! SQS): the bounded tokio bridge channels, the optional redo fetcher (redo% >
//! 0), the `sz-worker-N` OS threads, and the bounded final join that decides
//! whether tearing down the native environment is safe (consumer FIX-2).
//!
//! WHEN the pool is spawned (before or after the backend connects) and what
//! the async loop does with it stay in each backend; only the identical
//! wiring lives here.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};

use crate::config::Config;
use crate::redo::fetcher_loop;
use crate::worker::{
    LoadItem, LoadSide, Outcome, RedoInFlight, RedoJob, RedoSide, WorkerCtx, add_record_flags,
    redo_flags, worker_class, worker_loop,
};

/// Poll cadence of the bounded final join.
const JOIN_POLL: Duration = Duration::from_millis(20);

/// The running engine pool, handed back to the backend's async loop.
pub struct EnginePool {
    /// Load work into the pool (bounded at `threads`). Dropping it lets idle
    /// workers observe end-of-stream.
    pub work_tx: mpsc::Sender<LoadItem>,
    /// Load outcomes (and durable fatal signals) from workers + the fetcher.
    /// Yields `None` once every worker and the fetcher has exited.
    pub result_rx: mpsc::Receiver<Outcome>,
    /// Delivery tags some worker has picked up (design §6 shutdown split).
    pub started: Arc<Mutex<HashSet<u64>>>,
    /// Fatal-error wakeup shared with workers and the fetcher.
    pub shutdown_notify: Arc<Notify>,
    /// In-flight redo records, scanned by the long-record monitor.
    pub redo_in_flight: Arc<Mutex<RedoInFlight>>,
    /// Engine-owning thread handles, for the bounded final join.
    pub threads: EngineThreads,
}

/// Every thread holding an engine handle: destroying the environment under
/// any of them is a use-after-free, so all must finish before teardown.
pub struct EngineThreads {
    workers: Vec<JoinHandle<()>>,
    fetcher: Option<JoinHandle<()>>,
}

impl EngineThreads {
    /// Bounded join over the workers, `extra` (if any) and the redo fetcher,
    /// waiting until `deadline` at most. Returns `true` only if EVERY thread
    /// finished in time (they are then joined); `false` means at least one is
    /// still inside an uninterruptible engine call, so the caller must SKIP the
    /// native environment destroy (leak-on-exit over use-after-free).
    pub async fn join_bounded(self, deadline: Instant, extra: Option<JoinHandle<()>>) -> bool {
        let mut handles = self.workers;
        handles.extend(extra);
        handles.extend(self.fetcher);
        let all_joined = wait_until_finished(&handles, deadline).await;
        if all_joined {
            for handle in handles {
                let _ = handle.join();
            }
            tracing::info!("all engine workers finished; safe to destroy environment");
        } else {
            tracing::warn!(
                "shutdown grace elapsed with workers still in engine calls; \
                 skipping environment destroy to avoid use-after-free (leak-on-exit)"
            );
        }
        all_joined
    }
}

/// Sleeps (async) until every handle has finished or `deadline` passes, then
/// reports whether all finished.
async fn wait_until_finished(handles: &[JoinHandle<()>], deadline: Instant) -> bool {
    while Instant::now() < deadline && handles.iter().any(|h| !h.is_finished()) {
        tokio::time::sleep(JOIN_POLL).await;
    }
    handles.iter().all(|h| h.is_finished())
}

/// Spawns the redo fetcher (only when redo% > 0) and the `threads` engine
/// workers wired to fresh bridge channels. At redo% = 0 the process issues
/// ZERO redo-related calls: no channel, no fetcher (design §3).
pub fn spawn_engine_pool(config: &Config, env: &Arc<SzEnvironmentCore>) -> Result<EnginePool> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();

    // --- Bridge channels -----------------------------------------------------
    let (work_tx, work_rx) = mpsc::channel::<LoadItem>(threads);
    let (result_tx, result_rx) = mpsc::channel::<Outcome>(threads * 2);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let started: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let shutdown_notify = Arc::new(Notify::new());

    let add_flags = add_record_flags(config.info);
    let rflags = redo_flags(config.info);
    let want_info = config.info;

    // --- Redo side (only when redo% > 0) -------------------------------------
    let redo_in_flight: Arc<Mutex<RedoInFlight>> = Arc::new(Mutex::new(HashMap::new()));
    let (redo_side, fetcher) = if config.redo_percent > 0 {
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
                    None, // queue mode never self-terminates on an empty redo queue
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
        let class = worker_class(worker_id, redo_pref);
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
            transform: config.transform.clone(),
        };
        let handle = std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || worker_loop(ctx))
            .context("failed to spawn worker thread")?;
        workers.push(handle);
    }
    // Drop our extra clones so the channels close once their users exit.
    drop(result_tx);
    drop(redo_side);

    Ok(EnginePool {
        work_tx,
        result_rx,
        started,
        shutdown_notify,
        redo_in_flight,
        threads: EngineThreads { workers, fetcher },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc as std_mpsc;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime")
    }

    /// A real thread that exits only when `release` fires (or its sender drops).
    fn gated_thread() -> (JoinHandle<()>, std_mpsc::Sender<()>) {
        let (tx, rx) = std_mpsc::channel::<()>();
        let h = std::thread::spawn(move || {
            let _ = rx.recv();
        });
        (h, tx)
    }

    #[test]
    fn join_bounded_true_when_all_threads_finish_in_time() {
        let (w, w_tx) = gated_thread();
        let (f, f_tx) = gated_thread();
        let (x, x_tx) = gated_thread();
        let threads = EngineThreads {
            workers: vec![w],
            fetcher: Some(f),
        };
        // Release everything shortly after the join starts waiting.
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(60));
            drop((w_tx, f_tx, x_tx));
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(rt().block_on(threads.join_bounded(deadline, Some(x))));
        assert!(
            Instant::now() < deadline,
            "returned as soon as all finished"
        );
        releaser.join().unwrap();
    }

    #[test]
    fn join_bounded_false_at_deadline_when_a_thread_is_stuck() {
        let (stuck, stuck_tx) = gated_thread();
        let threads = EngineThreads {
            workers: vec![std::thread::spawn(|| {}), stuck],
            fetcher: None,
        };
        let start = Instant::now();
        let deadline = start + Duration::from_millis(150);
        assert!(!rt().block_on(threads.join_bounded(deadline, None)));
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(150),
            "waited the full grace"
        );
        assert!(waited < Duration::from_secs(5), "did not wait past it");
        drop(stuck_tx);
    }

    #[test]
    fn join_bounded_checks_extra_and_fetcher_too() {
        // Workers done, but the extra (stats) thread is stuck: not all joined.
        let (stuck, stuck_tx) = gated_thread();
        let threads = EngineThreads {
            workers: vec![std::thread::spawn(|| {})],
            fetcher: Some(std::thread::spawn(|| {})),
        };
        let deadline = Instant::now() + Duration::from_millis(80);
        assert!(!rt().block_on(threads.join_bounded(deadline, Some(stuck))));
        drop(stuck_tx);

        // Fetcher stuck, everything else done.
        let (stuck, stuck_tx) = gated_thread();
        let threads = EngineThreads {
            workers: vec![],
            fetcher: Some(stuck),
        };
        let deadline = Instant::now() + Duration::from_millis(80);
        assert!(!rt().block_on(threads.join_bounded(deadline, None)));
        drop(stuck_tx);
    }

    #[test]
    fn join_bounded_past_deadline_still_reports_finished_threads() {
        // An already-elapsed deadline must not wait, but finished threads count.
        let h = std::thread::spawn(|| {});
        while !h.is_finished() {
            std::thread::sleep(Duration::from_millis(1));
        }
        let threads = EngineThreads {
            workers: vec![h],
            fetcher: None,
        };
        assert!(rt().block_on(threads.join_bounded(Instant::now(), None)));
    }
}
