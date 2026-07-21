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
//! ## Why we key off the REGISTERED DEFAULT changing, not `get_active_config_id()`
//! The obvious design is "reinitialize when `get_active_config_id() != default`".
//! It does NOT work on the settings-JSON init path (the one this driver uses):
//! **`get_active_config_id()` returns 0 even after a valid init that loaded the
//! registered default, and stays 0 after `reinitialize(default)` succeeds**
//! (engine defect GDEV-4313, proven by before/after logging: `active 0 ->
//! reinitialize(4113232042) -> now 0`). Keying on the active id therefore sees a
//! permanent `0 != default` mismatch and reinitializes on EVERY poll forever — a
//! ~60s reinit storm whose destroy+reinit churns DB connections and produces a
//! prepared-statement 8179 storm (GDEV-4314).
//!
//! So instead we track the registered default we have ADOPTED (via
//! [`ADOPTED_DEFAULT`], seeded at startup to the default the engine loaded) and
//! reinitialize only when `get_config_manager().get_default_config_id()` — which
//! IS reliable — differs from it. This is immune to GDEV-4313 and reinitializes
//! exactly once per real registered-default change.
//!
//! Two triggers feed one double-checked reconcile:
//!   1. PERIODIC — every looping engine thread calls [`poll`] as it processes. A
//!      PROCESS-GLOBAL throttle collapses that to a single `get_default_config_id`
//!      SQL round-trip per `SENZING_CONFIG_RELOAD_SECS` (default 60) per process.
//!   2. ERROR-DRIVEN — on an `add_record` / `process_redo_record` error the caller
//!      asks [`reinit_if_stale`] whether the registered default changed since we
//!      adopted it; if so the engine is reinitialized and the caller RETRIES the
//!      operation once. If the default is unchanged the error is genuine and is
//!      propagated unchanged.
//!
//! The reinitialize is behind a PROCESS-GLOBAL lock that RE-CHECKS
//! `default != adopted` INSIDE the lock (double-checked): if 12 threads all trip a
//! stale-config error at once, the first reinitializes and adopts, and the other
//! 11 observe `default == adopted` under the lock and skip — no stacked reinits.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sz_rust_sdk::prelude::*;
use tracing::{info, warn};

/// Serializes the actual `reinitialize` so concurrent stale-config errors do not
/// stack reinit calls. The double-check inside is what makes it correct.
static REINIT_LOCK: Mutex<()> = Mutex::new(());

/// Sentinel for "no registered default adopted yet" (before the first observation).
/// Real Senzing config ids are positive, so `i64::MIN` can never collide.
const NO_DEFAULT: i64 = i64::MIN;

/// The registered DEFAULT config id this process has adopted — i.e. the config the
/// engine is currently running. Seeded at startup (in [`log_startup_config`]) to the
/// default present at init (which settings-JSON init already loaded), and updated on
/// every successful reinitialize. We compare the live `get_default_config_id()`
/// against THIS, never against `get_active_config_id()` (unreliable — GDEV-4313).
static ADOPTED_DEFAULT: AtomicI64 = AtomicI64::new(NO_DEFAULT);

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

/// ERROR-DRIVEN trigger: returns `true` if the registered default had changed since
/// we adopted it (in which case the engine has now been reinitialized — by this call
/// or a concurrent one — and the caller should RETRY the operation once). Returns
/// `false` if the default is unchanged (the error is genuine; propagate it) or the
/// check itself failed.
pub fn reinit_if_stale(env: &SzEnvironmentCore) -> bool {
    match reconcile(env) {
        Ok(reinit_warranted) => reinit_warranted,
        Err(e) => {
            warn!("config staleness check failed: {e}");
            false
        }
    }
}

/// Reconcile the registered DEFAULT config against the one this process has ADOPTED;
/// reinitialize only if the registered default actually changed. Keyed on
/// `get_default_config_id()` (reliable) vs [`ADOPTED_DEFAULT`] — NOT on
/// `get_active_config_id()`, which is unreliably 0 on the settings-JSON init path
/// (GDEV-4313) and would cause a perpetual reinit storm. Cheap pre-check outside the
/// lock (the common unchanged case never locks), then double-checked under a
/// process-global lock so concurrent callers do not stack reinit calls.
///
/// Returns `Ok(true)` iff a reinit was warranted at entry (performed here or by a
/// peer), `Ok(false)` if the process was already on the registered default.
fn reconcile(env: &SzEnvironmentCore) -> Result<bool, SzError> {
    let default = env.get_config_manager()?.get_default_config_id()?;
    let adopted = ADOPTED_DEFAULT.load(Ordering::Relaxed);
    // First observation before startup seeding ran: adopt the current default WITHOUT
    // reinitializing — the engine is already running it (settings-JSON init loaded it).
    if adopted == NO_DEFAULT {
        ADOPTED_DEFAULT.store(default, Ordering::Relaxed);
        return Ok(false);
    }
    if default == adopted {
        return Ok(false);
    }
    let _guard = REINIT_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    // Re-read under the lock; a peer may have adopted the new default while we waited.
    let default = env.get_config_manager()?.get_default_config_id()?;
    if default == ADOPTED_DEFAULT.load(Ordering::Relaxed) {
        return Ok(true);
    }
    env.reinitialize(default)?;
    ADOPTED_DEFAULT.store(default, Ordering::Relaxed);
    // Also log the engine's reported active id (expected to remain 0 until GDEV-4313
    // is fixed) so we keep visibility into that engine defect without acting on it.
    let active = env.get_active_config_id().unwrap_or(-1);
    info!(
        "CONFIG REFRESHED: registered default {adopted} -> {default}; reinitialize({default}) done \
         (engine active id reports {active}; see GDEV-4313)"
    );
    match env.get_product().and_then(|p| p.get_license()) {
        Ok(lic) => info!("LICENSE AFTER REINIT: {lic}"),
        Err(e) => warn!("get_license after reinit failed: {e}"),
    }
    Ok(true)
}

/// Seed [`ADOPTED_DEFAULT`] with the registered default the engine loaded at init, and
/// log the active-vs-default ids once per process (after init, before workers poll).
/// Seeding here means the first periodic [`poll`] is a clean no-op instead of a
/// spurious reinit. The active id is logged only for GDEV-4313 visibility — it is NOT
/// used for the reload decision.
pub fn log_startup_config(env: &SzEnvironmentCore) {
    let active = env.get_active_config_id();
    let default = env
        .get_config_manager()
        .and_then(|m| m.get_default_config_id());
    if let Ok(d) = default {
        ADOPTED_DEFAULT.store(d, Ordering::Relaxed);
    }
    info!(
        "CONFIG AT INIT: active={active:?} default={default:?} \
         (config-reload keys off registered-default change; active id is unreliable per GDEV-4313)"
    );
}
