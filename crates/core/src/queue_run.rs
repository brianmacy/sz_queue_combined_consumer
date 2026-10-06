//! Queue-mode (redo% < 100) run plumbing shared by every async backend
//! (RabbitMQ, SQS): the [`RunOutcome`] a backend loop reports, the tokio
//! runtime bring-up + use-after-free exit discipline in [`run_queue_mode`],
//! and the monitor cadence shared by the loop.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use tokio::time::{Interval, MissedTickBehavior};

use crate::runtime;

/// Outcome of a backend's queue-mode run, reported to `main` so it can decide
/// whether tearing down the global Senzing environment is safe and what exit
/// code to use (consumer parity).
pub struct RunOutcome {
    /// `true` only if EVERY engine thread (workers + redo fetcher) actually
    /// finished before the shutdown grace elapsed. When `false`, `main` must
    /// SKIP the native environment `destroy()` (leak-on-exit over use-after-free).
    pub all_workers_joined: bool,
    /// `Some(message)` if shutting down due to a non-recoverable error.
    pub fatal: Option<String>,
}

/// Backend-specific wording of the two exit-path warnings (kept per backend so
/// operators' log searches keep matching).
pub struct ExitLogs {
    /// Warned when a worker did not join and the destroy is skipped.
    pub leak: &'static str,
    /// Warned after the run future itself returned `Err`.
    pub run_failed: &'static str,
}

/// Builds the tokio runtime for the backend's I/O layer only, drives `run` to
/// completion, then applies the shared use-after-free exit discipline. Never
/// returns.
///
/// worker_threads is pinned to 2 (not the num_cpus default): this runtime only
/// drives broker I/O + a few timers and hands every record to the dedicated
/// `sz-worker` OS threads via channels — no Senzing/libSz FFI ever runs on a
/// tokio worker. On a high-core host the default (one worker per core, e.g. 64)
/// spawns dozens of idle runtime threads per process, each able to seed its own
/// glibc malloc arena; 2 is ample for the I/O layer.
pub fn run_queue_mode<F>(run: F, logs: &ExitLogs) -> !
where
    F: Future<Output = Result<RunOutcome>>,
{
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build tokio runtime: {e}");
            runtime::leak_and_exit(255);
        }
    };

    // Use-after-free guard (consumer FIX-2): only tear down the Senzing
    // environment when EVERY engine thread actually finished. A startup `Err`
    // from `run` is also treated conservatively as "do not destroy". In every
    // case we terminate via the `runtime` exit helpers rather than returning:
    // returning would drop the tokio runtime and `Arc<env>`, either of which can
    // wedge on a stuck native thread and overrun the SIGTERM grace (issue #4).
    match rt.block_on(run) {
        Ok(outcome) => {
            let code = exit_code(&outcome);
            if outcome.all_workers_joined {
                runtime::teardown_and_exit(code);
            } else {
                tracing::warn!("{}", logs.leak);
                runtime::leak_and_exit(code);
            }
        }
        Err(e) => {
            eprintln!("{e:#}");
            tracing::warn!("{}", logs.run_failed);
            runtime::leak_and_exit(255);
        }
    }
}

/// Process exit code for a completed run: 0, or 255 (after printing the
/// reason) when shutting down due to a fatal error.
fn exit_code(outcome: &RunOutcome) -> u8 {
    match &outcome.fatal {
        None => 0,
        Some(msg) => {
            eprintln!("Shutting down due to error: {msg}");
            255
        }
    }
}

/// Long-record monitor cadence: `LONG_RECORD / 2` (min 1 s), skipping missed
/// ticks. Must be called inside the tokio runtime.
pub fn monitor_interval(long_record_secs: u64) -> Interval {
    let mut monitor = tokio::time::interval(monitor_period(long_record_secs));
    monitor.set_missed_tick_behavior(MissedTickBehavior::Skip);
    monitor
}

fn monitor_period(long_record_secs: u64) -> Duration {
    Duration::from_secs(long_record_secs.max(2) / 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_is_zero_clean_and_255_on_fatal() {
        let clean = RunOutcome {
            all_workers_joined: true,
            fatal: None,
        };
        assert_eq!(exit_code(&clean), 0);
        let fatal = RunOutcome {
            all_workers_joined: false,
            fatal: Some("boom".into()),
        };
        assert_eq!(exit_code(&fatal), 255);
    }

    #[test]
    fn monitor_period_is_half_long_record_with_one_second_floor() {
        assert_eq!(monitor_period(300), Duration::from_secs(150));
        assert_eq!(monitor_period(3), Duration::from_secs(1));
        assert_eq!(monitor_period(0), Duration::from_secs(1));
    }

    #[test]
    fn monitor_interval_skips_missed_ticks() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let monitor = rt.block_on(async { monitor_interval(10) });
        assert_eq!(monitor.period(), Duration::from_secs(5));
        assert_eq!(monitor.missed_tick_behavior(), MissedTickBehavior::Skip);
    }
}
