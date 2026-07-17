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

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tracing::{info, warn};

/// Serializes the actual `reinitialize` so concurrent stale-config errors do not
/// stack reinit calls. The double-check inside is what makes it correct.
static REINIT_LOCK: Mutex<()> = Mutex::new(());

/// The registered default config id this process has actually reinitialized onto
/// (0 = none applied yet). Drift is detected by comparing the current registered
/// default against THIS, NOT against `get_active_config_id()`: on the settings-JSON
/// (default-config) init path the engine's `Sz_getActiveConfigID` reports 0 even
/// after a successful `reinitialize`, so keying off it made every poll see
/// "active(0) != default" and reinitialize forever (a ~60s reinit storm). Tracking
/// what we applied here makes reinit fire exactly once per real default change.
static LAST_APPLIED: AtomicI64 = AtomicI64::new(0);

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

/// Cheap applied-vs-default pre-check outside the lock; if the registered default
/// differs from what this process last applied, take the process-global lock and
/// RE-CHECK before reinitializing (double-checked, so concurrent callers do not
/// stack reinit calls). Returns `Ok(true)` iff a reinit was warranted at entry
/// (performed here or by a peer), `Ok(false)` if already current.
///
/// Drift is measured against [`LAST_APPLIED`] — the id we last reinitialized onto —
/// NOT `get_active_config_id()`, which reports 0 on this init path and would make
/// every poll reinitialize forever (see the static's doc).
fn reconcile(env: &SzEnvironmentCore) -> Result<bool, SzError> {
    // One SQL select for the registered default; the pre-check keeps the common
    // (unchanged) case off the lock entirely.
    let default = env.get_config_manager()?.get_default_config_id()?;
    if LAST_APPLIED.load(Ordering::Relaxed) == default {
        return Ok(false);
    }

    let _guard = REINIT_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    // Re-read under the lock: a peer may have applied it while we waited.
    let default = env.get_config_manager()?.get_default_config_id()?;
    let prev = LAST_APPLIED.load(Ordering::Relaxed);
    if prev != default {
        env.reinitialize(default)?;
        // Record what we applied only AFTER a successful reinit, so a failed
        // reinit is retried on the next poll rather than silently skipped.
        LAST_APPLIED.store(default, Ordering::Relaxed);
        // Audit trail that a live config change propagated to this process.
        info!("CONFIG REFRESHED: engine reinitialized from config {prev} -> {default}");
        // DIAGNOSTIC: does reinitialize() preserve the license? If recordLimit here
        // flips to a small demo value (e.g. 500), reinit dropped the init-JSON
        // LICENSESTRINGBASE64 and the engine fell back to the demo license.
        match env.get_product().and_then(|p| p.get_license()) {
            Ok(lic) => info!("LICENSE AFTER REINIT: {lic}"),
            Err(e) => warn!("get_license after reinit failed: {e}"),
        }
    }
    Ok(true)
}
