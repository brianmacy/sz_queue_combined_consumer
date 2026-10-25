//! INFO sink hook: an optional plugin that receives every WITH_INFO payload
//! (contract: the `sz-info-sink` crate).
//!
//! Mirrors the record-transform loader (`transform.rs`): the plugin is
//! `dlopen`ed once at startup (fail-fast, before `Sz_init`; the ABI version is
//! checked at load, BEFORE any other symbol is resolved), shared by every worker thread, and never `dlclose`d.
//!
//! Lifecycle in every run mode (file, queue, pure redoer):
//!
//! * a configured sink forces WITH_INFO on `add_record` and
//!   `process_redo_record` ([`crate::config::Config::with_info`]); each worker
//!   writes the payload right after the engine call returns (kind ADD or
//!   REDO; DELETE is reserved and never written by this consumer);
//! * the main loop flushes it every `--info-sink-flush-secs` ([`FlushTimer`]);
//! * it is closed EXPLICITLY once the workers have joined (or the shutdown
//!   grace ran out) and BEFORE the engine is torn down — on the SIGTERM path
//!   too. `Drop` only destroys the handle: the forced-exit paths `_exit`
//!   without running destructors, so nothing may rely on it.
//! * the close runs on its own thread and is bounded by [`CLOSE_TIMEOUT`]
//!   ([`InfoSinkHandle::close_within`]): a close that does not return in time
//!   is FATAL (the stuck sink is named in the log) and the run takes the
//!   leak-on-exit path, so a wedged plugin cannot overrun the SIGTERM grace.
//!
//! Every sink error (create, write, flush, close) is FATAL: orderly shutdown,
//! non-zero exit. A record whose INFO could not be written is never reported
//! as loaded (file mode does not advance the resume watermark past it; queue
//! mode leaves the delivery unsettled for redelivery). A redo record is
//! dequeued at fetch, so its INFO is lost if that write fails.
//!
//! Durability contract the consumer relies on: `sz_is_write` returning 0
//! means the line has left the plugin's process memory (handed to the OS), so
//! an ack / watermark advance after it survives a SIGKILL or OOM kill; a seal
//! (`sz_is_flush` / `sz_is_close`) is what makes files complete. File mode
//! therefore prints a resume point no further than the watermark captured
//! before the last successful seal.

use std::ffi::{CString, c_char, c_void};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Upper bound on the final `sz_is_close` (see the module docs).
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

use sz_info_sink::{
    ABI_VERSION, AbiVersionFn, CreateFn, DestroyFn, FreeFn, SYM_ABI_VERSION, SYM_CLOSE, SYM_CREATE,
    SYM_DESTROY, SYM_FLUSH, SYM_FREE, SYM_WRITE, SZ_IS_OK, SealFn, WriteFn,
};
pub use sz_info_sink::{InfoKind, InfoSink};

use crate::stats::{ERRORS, RUNNING, WORKER_FATAL};
use crate::transform::{dl_error, sym, take_string};

/// Shared, optional sink as stored in `Config` / `WorkerCtx`, with the name
/// (plugin path) used in log messages.
#[derive(Clone, Default)]
pub struct InfoSinkHandle {
    sink: Option<Arc<dyn InfoSink>>,
    name: Arc<str>,
}

impl fmt::Debug for InfoSinkHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.sink.is_some() {
            "InfoSinkHandle(Some)"
        } else {
            "InfoSinkHandle(None)"
        })
    }
}

/// How the bounded final close ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseOutcome {
    /// Sealed (or no sink configured).
    Closed,
    /// The sink reported an error (or its close panicked).
    Failed(String),
    /// The close did not return within the limit; it is still running on a
    /// detached thread.
    TimedOut(String),
}

impl InfoSinkHandle {
    /// Loads `path` (if given) with `config` (empty when `None`).
    pub fn load(path: Option<&str>, config: Option<&str>) -> Result<Self, String> {
        match path.filter(|p| !p.is_empty()) {
            None => Ok(Self::default()),
            Some(p) => {
                let plugin = PluginInfoSink::open(p, config.unwrap_or(""))?;
                tracing::info!("INFO sink plugin loaded: {p}");
                Ok(Self::from_sink(p, Arc::new(plugin)))
            }
        }
    }

    /// Wraps an in-process sink (`name` is used in log messages).
    pub fn from_sink(name: &str, sink: Arc<dyn InfoSink>) -> Self {
        Self {
            sink: Some(sink),
            name: name.into(),
        }
    }

    /// `true` when a sink is configured.
    pub fn is_enabled(&self) -> bool {
        self.sink.is_some()
    }

    /// Writes one payload; a no-op without a sink or for an empty payload
    /// (the engine returned no INFO).
    pub fn write(&self, kind: InfoKind, info: &str) -> Result<(), String> {
        match &self.sink {
            Some(sink) if !info.is_empty() => sink
                .write(kind, info)
                .map_err(|e| format!("INFO sink write failed: {e}")),
            _ => Ok(()),
        }
    }

    /// Seals the sink's current output (no-op without a sink).
    pub fn flush(&self) -> Result<(), String> {
        self.sink.as_ref().map_or(Ok(()), |sink| {
            sink.flush()
                .map_err(|e| format!("INFO sink flush failed: {e}"))
        })
    }

    /// Final seal bounded by [`CLOSE_TIMEOUT`]; see [`Self::close_within`].
    pub fn close(&self) -> Result<(), String> {
        match self.close_within(CLOSE_TIMEOUT) {
            CloseOutcome::Closed => Ok(()),
            CloseOutcome::Failed(e) | CloseOutcome::TimedOut(e) => Err(e),
        }
    }

    /// Final seal (no-op without a sink) run on its own thread and waited
    /// for at most `timeout`. Logs a successful close.
    pub fn close_within(&self, timeout: Duration) -> CloseOutcome {
        let Some(sink) = self.sink.clone() else {
            return CloseOutcome::Closed;
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("sz-info-sink-close".to_string())
            .spawn(move || {
                let _ = tx.send(sink.close());
            });
        if let Err(e) = spawned {
            return CloseOutcome::Failed(format!("INFO sink close: cannot spawn thread: {e}"));
        }
        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {
                tracing::info!("INFO sink closed");
                CloseOutcome::Closed
            }
            Ok(Err(e)) => CloseOutcome::Failed(format!("INFO sink close failed: {e}")),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => CloseOutcome::TimedOut(format!(
                "INFO sink {} is stuck: sz_is_close did not return within {timeout:?}",
                self.name
            )),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                CloseOutcome::Failed("INFO sink close failed: the close thread panicked".into())
            }
        }
    }

    /// The final close on a std-thread run path: any failure is
    /// [`fail_fatal`]; returns `(sealed, stuck)` — `stuck` means the close
    /// is still running and the caller must take the leak-on-exit path.
    pub fn close_or_fail(&self) -> (bool, bool) {
        match self.close_within(CLOSE_TIMEOUT) {
            CloseOutcome::Closed => (true, false),
            CloseOutcome::Failed(e) => {
                fail_fatal(&e);
                (false, false)
            }
            CloseOutcome::TimedOut(e) => {
                fail_fatal(&e);
                (false, true)
            }
        }
    }
}

/// Marks the process fatal for a sink error outside a worker (flush / close
/// on the std-thread run paths): logged, counted, `WORKER_FATAL` set and the
/// run stopped, exactly like a fatal engine error.
pub fn fail_fatal(msg: &str) {
    tracing::error!("FATAL: {msg}");
    ERRORS.fetch_add(1, Ordering::Relaxed);
    WORKER_FATAL.store(true, Ordering::Relaxed);
    RUNNING.store(false, Ordering::Relaxed);
}

/// The main loop's flush cadence.
pub struct FlushTimer {
    every: Duration,
    last: Instant,
}

impl FlushTimer {
    /// Starts the interval now.
    pub fn new(every: Duration) -> Self {
        Self {
            every,
            last: Instant::now(),
        }
    }

    /// `true` (and restarts the interval) once `every` has elapsed.
    pub fn due(&mut self) -> bool {
        if self.last.elapsed() < self.every {
            return false;
        }
        self.last = Instant::now();
        true
    }
}

/// A sink backed by a `dlopen`ed shared library.
pub struct PluginInfoSink {
    lib: *mut c_void,
    handle: *mut c_void,
    write: WriteFn,
    flush: SealFn,
    close: SealFn,
    free: FreeFn,
    destroy: DestroyFn,
}

// SAFETY: the ABI requires every entry point to be thread-safe on one handle;
// the library and handle are immutable after construction.
unsafe impl Send for PluginInfoSink {}
// SAFETY: see above.
unsafe impl Sync for PluginInfoSink {}

impl PluginInfoSink {
    pub fn open(path: &str, config: &str) -> Result<Self, String> {
        let cpath = CString::new(path).map_err(|_| "plugin path contains NUL".to_string())?;
        let cconfig = CString::new(config).map_err(|_| "plugin config contains NUL".to_string())?;
        // SAFETY: valid C string; RTLD_LOCAL keeps plugin symbols private.
        let lib = unsafe { libc::dlopen(cpath.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if lib.is_null() {
            return Err(format!("dlopen {path}: {}", dl_error()));
        }
        // On any failure below the library stays loaded (process exits anyway).
        let fail = |e: String| format!("INFO sink plugin {path}: {e}");
        // The version FIRST: another ABI version may name its other entry
        // points differently, and must be reported as a version mismatch.
        // SAFETY: lib is live; the type matches the (version-stable) symbol.
        let abi = unsafe { sym::<AbiVersionFn>(lib, SYM_ABI_VERSION) }.map_err(fail)?;
        // SAFETY: resolved above.
        let v = unsafe { abi() };
        if v != ABI_VERSION {
            return Err(fail(format!(
                "ABI version {v}, consumer requires {ABI_VERSION}"
            )));
        }
        // SAFETY: lib is live; types match ABI version 1.
        let (create, write, flush, close, free, destroy) = unsafe {
            (
                sym::<CreateFn>(lib, SYM_CREATE).map_err(fail)?,
                sym::<WriteFn>(lib, SYM_WRITE).map_err(fail)?,
                sym::<SealFn>(lib, SYM_FLUSH).map_err(fail)?,
                sym::<SealFn>(lib, SYM_CLOSE).map_err(fail)?,
                sym::<FreeFn>(lib, SYM_FREE).map_err(fail)?,
                sym::<DestroyFn>(lib, SYM_DESTROY).map_err(fail)?,
            )
        };
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: valid config string and err out-pointer.
        let handle = unsafe { create(cconfig.as_ptr(), &mut err) };
        if handle.is_null() {
            // SAFETY: err is null or plugin-owned.
            let msg = unsafe { take_string(free, err) }
                .unwrap_or_else(|| "sz_is_create returned NULL".to_string());
            return Err(fail(format!("create failed: {msg}")));
        }
        Ok(Self {
            lib,
            handle,
            write,
            flush,
            close,
            free,
            destroy,
        })
    }

    /// Maps an ABI return code (+ optional plugin message) to a `Result`.
    fn status(&self, rc: i32, err: *mut c_char, what: &str) -> Result<(), String> {
        // SAFETY: err is null or plugin-owned.
        let msg = unsafe { take_string(self.free, err) };
        match rc {
            SZ_IS_OK => Ok(()),
            _ => Err(msg.unwrap_or_else(|| format!("{what} returned {rc} without a message"))),
        }
    }

    fn seal(&self, f: SealFn, what: &str) -> Result<(), String> {
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: handle is live; err is a valid out-pointer.
        let rc = unsafe { f(self.handle, &mut err) };
        self.status(rc, err, what)
    }
}

impl InfoSink for PluginInfoSink {
    fn write(&self, kind: InfoKind, info: &str) -> Result<(), String> {
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: handle is live; info is valid for its length for the call.
        let rc = unsafe {
            (self.write)(
                self.handle,
                kind.code(),
                info.as_ptr().cast(),
                info.len(),
                &mut err,
            )
        };
        self.status(rc, err, "sz_is_write")
    }

    fn flush(&self) -> Result<(), String> {
        self.seal(self.flush, "sz_is_flush")
    }

    fn close(&self) -> Result<(), String> {
        self.seal(self.close, "sz_is_close")
    }
}

impl Drop for PluginInfoSink {
    fn drop(&mut self) {
        // SAFETY: handle from sz_is_create, destroyed exactly once. The library
        // is intentionally NOT dlclose'd (plugin threads / TLS destructors).
        unsafe { (self.destroy)(self.handle) };
        let _ = self.lib;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// The file-sink plugin cdylib, built as a dev-dependency next to this test.
    fn file_sink_plugin() -> String {
        let deps = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let p = deps.join(format!(
            "{}sz_info_sink_file.{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_EXTENSION
        ));
        assert!(p.exists(), "file-sink plugin not built at {p:?}");
        p.to_string_lossy().into_owned()
    }

    /// The record-transform example: a real library WITHOUT the sz_is_ symbols.
    fn transform_plugin() -> String {
        file_sink_plugin().replace("sz_info_sink_file", "sz_record_transform_example")
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("sz_core_info_sink_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("mkdir");
        d
    }

    fn file_config(dir: &Path, rotate_records: u64) -> String {
        serde_json::json!({
            "dir": dir,
            "prefix": "info-c0-",
            "rotate_records": rotate_records,
            "rotate_secs": 3600,
        })
        .to_string()
    }

    fn jsonl_files(dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn no_path_is_disabled_and_every_call_is_a_no_op() {
        for h in [
            InfoSinkHandle::load(None, None).unwrap(),
            InfoSinkHandle::load(Some(""), Some("x")).unwrap(),
        ] {
            assert!(!h.is_enabled());
            assert_eq!(format!("{h:?}"), "InfoSinkHandle(None)");
            assert!(h.write(InfoKind::Add, "{}").is_ok());
            assert!(h.flush().is_ok());
            assert!(h.close().is_ok());
        }
    }

    #[test]
    fn missing_library_or_symbols_fail_loudly() {
        let e = InfoSinkHandle::load(Some("/nonexistent/libnope.so"), None).expect_err("must fail");
        assert!(e.contains("dlopen"), "{e}");
        let e = InfoSinkHandle::load(Some(&transform_plugin()), None).expect_err("must fail");
        assert!(e.contains("missing symbol sz_is_abi_version"), "{e}");
        let e = InfoSinkHandle::load(Some("bad\0path"), None).expect_err("must fail");
        assert!(e.contains("NUL"), "{e}");
    }

    /// L10: a plugin from another ABI version is rejected as a VERSION
    /// mismatch, even when its other symbols differ (checked first).
    #[test]
    fn a_v2_plugin_reports_a_version_mismatch() {
        let v2 = file_sink_plugin().replace("sz_info_sink_file", "sz_info_sink_abi_v2_fixture");
        let e = InfoSinkHandle::load(Some(&v2), None).expect_err("must fail");
        assert!(e.contains("ABI version 2, consumer requires 1"), "{e}");
    }

    #[test]
    fn create_failure_carries_the_plugin_message() {
        let e = InfoSinkHandle::load(Some(&file_sink_plugin()), Some("{}")).expect_err("no dir");
        assert!(
            e.contains("create failed: info-sink-file config is invalid"),
            "{e}"
        );
        let e = InfoSinkHandle::load(Some(&file_sink_plugin()), Some("a\0b")).expect_err("NUL");
        assert!(e.contains("NUL"), "{e}");
    }

    /// Harness: the real file-sink plugin through the real loader, from many
    /// threads, with rotation, flush and the final close.
    #[test]
    fn file_sink_round_trip_through_the_abi() {
        let dir = temp_dir("roundtrip");
        let h = InfoSinkHandle::load(Some(&file_sink_plugin()), Some(&file_config(&dir, 10)))
            .expect("load file sink");
        assert!(h.is_enabled());
        assert_eq!(format!("{h:?}"), "InfoSinkHandle(Some)");
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let h = h.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        let kind = if i % 2 == 0 {
                            InfoKind::Add
                        } else {
                            InfoKind::Redo
                        };
                        h.write(kind, &format!(r#"{{"T":{t},"I":{i}}}"#)).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        // Empty payloads are not written.
        h.write(InfoKind::Add, "").unwrap();
        h.write(InfoKind::Add, r#"{"LAST":1}"#).unwrap();
        h.flush().unwrap();
        h.close().unwrap();
        let e = h.write(InfoKind::Add, "{}").unwrap_err();
        assert!(
            e.contains("INFO sink write failed: info-sink-file is closed"),
            "{e}"
        );
        let e = h.flush().unwrap_err();
        assert!(e.contains("INFO sink flush failed"), "{e}");

        let files = jsonl_files(&dir);
        assert_eq!(
            files.len(),
            11,
            "100 lines / 10 per file + the flushed tail"
        );
        let lines: Vec<String> = files
            .iter()
            .flat_map(|f| {
                std::fs::read_to_string(f)
                    .unwrap()
                    .lines()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(lines.len(), 101);
        assert!(
            lines
                .iter()
                .all(|l| serde_json::from_str::<serde_json::Value>(l).is_ok())
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn sink_errors_surface_with_context() {
        let dir = temp_dir("errors");
        let h =
            InfoSinkHandle::load(Some(&file_sink_plugin()), Some(&file_config(&dir, 10))).unwrap();
        let e = h.write(InfoKind::Redo, "{\n}").unwrap_err();
        assert!(e.starts_with("INFO sink write failed: "), "{e}");
        assert!(e.contains("line break"), "{e}");
        h.close().unwrap();
        // close is idempotent in the file plugin; still Ok.
        assert!(h.close().is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A real in-process sink whose close blocks until released: a wedged
    /// plugin (e.g. a hung network volume).
    struct StuckOnClose(std::sync::Mutex<std::sync::mpsc::Receiver<()>>);

    impl InfoSink for StuckOnClose {
        fn write(&self, _: InfoKind, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
        fn close(&self) -> Result<(), String> {
            let _ = self.0.lock().unwrap().recv();
            Ok(())
        }
    }

    /// M3: a close that never returns is bounded, reported as stuck with the
    /// sink's name, and fatal.
    #[test]
    fn a_stuck_close_times_out_and_names_the_sink() {
        let (release, rx) = std::sync::mpsc::channel();
        let h = InfoSinkHandle::from_sink(
            "/plugins/libstuck.so",
            Arc::new(StuckOnClose(std::sync::Mutex::new(rx))),
        );
        let t0 = Instant::now();
        let out = h.close_within(Duration::from_millis(200));
        assert!(t0.elapsed() < Duration::from_secs(2), "bounded");
        match out {
            CloseOutcome::TimedOut(e) => {
                assert!(
                    e.contains("/plugins/libstuck.so") && e.contains("stuck"),
                    "{e}"
                )
            }
            other => panic!("expected a timeout, got {other:?}"),
        }
        release.send(()).unwrap();
    }

    /// A real in-process sink (an `InfoSink` linked into the consumer, which
    /// has no `catch_unwind` boundary in front of it) whose close panics.
    struct PanicsOnClose;

    impl InfoSink for PanicsOnClose {
        fn write(&self, _: InfoKind, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
        fn close(&self) -> Result<(), String> {
            panic!("in-process sink close panicked")
        }
    }

    /// A close that panics on its thread is a failed close, not a hang.
    #[test]
    fn a_panicking_close_is_reported_as_failed() {
        let h = InfoSinkHandle::from_sink("in-process", Arc::new(PanicsOnClose));
        match h.close_within(CLOSE_TIMEOUT) {
            CloseOutcome::Failed(e) => assert!(e.contains("close thread panicked"), "{e}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// The success path of `close_or_fail` and the `Failed` outcome. (The
    /// failure paths of `close_or_fail` flip the process-global RUNNING /
    /// WORKER_FATAL flags that concurrent unit tests read, so they are
    /// exercised by the e2e suite instead.)
    #[test]
    fn close_reports_sealed_and_failed() {
        let dir = temp_dir("close_outcomes");
        assert_eq!(InfoSinkHandle::default().close_or_fail(), (true, false));
        let h =
            InfoSinkHandle::load(Some(&file_sink_plugin()), Some(&file_config(&dir, 10))).unwrap();
        h.write(InfoKind::Add, "{}").unwrap();
        assert_eq!(h.close_or_fail(), (true, false), "sealed");
        // A sink failed by a create error reports it at close.
        let h =
            InfoSinkHandle::load(Some(&file_sink_plugin()), Some(&file_config(&dir, 10))).unwrap();
        std::fs::create_dir(dir.join("info-c0-000002.jsonl")).unwrap();
        assert!(h.write(InfoKind::Add, "{}").is_err());
        match h.close_within(CLOSE_TIMEOUT) {
            CloseOutcome::Failed(e) => assert!(e.contains("FAILED"), "{e}"),
            other => panic!("expected Failed, got {other:?}"),
        }
        let e = h.close().unwrap_err();
        assert!(e.starts_with("INFO sink close failed: "), "{e}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn flush_timer_fires_once_per_interval() {
        let mut t = FlushTimer::new(Duration::from_millis(50));
        assert!(!t.due());
        std::thread::sleep(Duration::from_millis(60));
        assert!(t.due());
        assert!(!t.due(), "restarted");
        let mut t = FlushTimer::new(Duration::ZERO);
        assert!(t.due());
    }
}
