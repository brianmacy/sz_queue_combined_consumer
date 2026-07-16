//! Live engine-config reload — adopt a new registered DEFAULT config WITHOUT a
//! process restart.
//!
//! Senzing does not auto-update a running engine when the registered default
//! config changes: the engine keeps the config it was initialized with until
//! [`SzEnvironment::reinitialize`] is called. That call is documented
//! thread-safe and leaves existing `get_engine()` handles valid. This module
//! lets an operator bump the registered default (e.g. apply a
//! `setGenericThreshold ... behavior:NAME ... sendToRedo:"No"` tweak) and have
//! every worker in every process converge onto it within one poll interval — no
//! bounce, no benchmark-contaminating restart.
//!
//! Two triggers feed one double-checked reconcile:
//!   1. PERIODIC — every looping engine thread calls [`poll`] as it processes. A
//!      PROCESS-GLOBAL throttle collapses that to a single `get_default_config_id`
//!      SQL round-trip per `SENZING_CONFIG_RELOAD_SECS` (default 60) per process.
//!   2. ERROR-DRIVEN — on an `add_record` / `process_redo_record` error the
//!      caller asks [`reinit_if_stale`] whether the active config drifted from
//!      the registered default; if so the engine is reinitialized and the caller
//!      RETRIES the operation once (a stale-config error is transient). If active
//!      already equals default the error is genuine and is propagated unchanged.
//!
//! The reinitialize is behind a PROCESS-GLOBAL lock that RE-CHECKS
//! `active != default` INSIDE the lock (double-checked): if 12 threads all trip a
//! stale-config error at once, the first reinitializes and the other 11 observe
//! `active == default` under the lock and skip — no stacked reinit calls.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tracing::{info, warn};

/// Serializes the actual `reinitialize` so concurrent stale-config errors do not
/// stack reinit calls. The double-check inside is what makes it correct.
static REINIT_LOCK: Mutex<()> = Mutex::new(());

/// Millis-since-[`poll_base`] of the last periodic default-config check claimed
/// by SOME thread in this process (0 = never).
static LAST_POLL_MS: AtomicU64 = AtomicU64::new(0);

/// Process-start anchor for the periodic-poll throttle.
fn poll_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

/// Poll interval from `SENZING_CONFIG_RELOAD_SECS` (default 60, min 1), read
/// once. `0` disables the PERIODIC trigger (the error-driven trigger stays on).
fn poll_interval() -> Option<Duration> {
    static IV: OnceLock<Option<Duration>> = OnceLock::new();
    *IV.get_or_init(|| {
        let secs = std::env::var("SENZING_CONFIG_RELOAD_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(60);
        (secs > 0).then(|| Duration::from_secs(secs))
    })
}

/// PERIODIC trigger: called by every looping engine thread. A cheap no-op (one
/// relaxed atomic load) unless this thread wins the once-per-interval CAS race,
/// in which case it does the default-config check and reinitializes if drifted.
pub fn poll(env: &SzEnvironmentCore) {
    let Some(interval) = poll_interval() else {
        return;
    };
    let now = poll_base().elapsed().as_millis() as u64;
    let last = LAST_POLL_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < interval.as_millis() as u64 {
        return;
    }
    // Claim this interval's slot; a losing CAS means a peer is handling it.
    if LAST_POLL_MS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    if let Err(e) = reconcile(env) {
        warn!("periodic config check failed: {e}");
    }
}

/// ERROR-DRIVEN trigger: returns `true` if the active config had drifted from
/// the registered default (in which case the engine has now been reinitialized —
/// by this call or a concurrent one — and the caller should RETRY the operation
/// once). Returns `false` if active already equals default (the error is genuine;
/// propagate it) or the check itself failed.
pub fn reinit_if_stale(env: &SzEnvironmentCore) -> bool {
    match reconcile(env) {
        Ok(reinit_warranted) => reinit_warranted,
        Err(e) => {
            warn!("config staleness check failed: {e}");
            false
        }
    }
}

/// Cheap active-vs-default pre-check outside the lock; if stale, take the
/// process-global lock and RE-CHECK before reinitializing (double-checked, so
/// concurrent callers do not stack reinit calls). Returns `Ok(true)` iff the
/// config was stale at entry (a reinit was warranted — performed here or by a
/// peer), `Ok(false)` if already current.
fn reconcile(env: &SzEnvironmentCore) -> Result<bool, SzError> {
    // Each id read is one SQL select; the pre-check keeps the common (current)
    // case off the lock entirely.
    let active = env.get_active_config_id()?;
    let default = env.get_config_manager()?.get_default_config_id()?;
    if active == default {
        return Ok(false);
    }

    let _guard = REINIT_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    // Re-check under the lock: a peer may have reinitialized while we waited.
    let active = env.get_active_config_id()?;
    let default = env.get_config_manager()?.get_default_config_id()?;
    if active != default {
        env.reinitialize(default)?;
        // Log EVERY refresh with both the old and new config IDs (Master
        // directive): this line is the audit trail that a live config change
        // propagated to this process.
        info!("CONFIG REFRESHED: engine reinitialized from config {active} -> {default}");
    }
    Ok(true)
}
