//! INFO sink plugin contract.
//!
//! An INFO sink is a shared library (any language) that receives every
//! WITH_INFO payload the consumer gets back from the engine. The consumer
//! `dlopen`s it once per process (`--info-sink`) and calls `sz_is_write`
//! concurrently from every worker thread, so the plugin MUST be thread-safe on
//! a single handle. `sz_is_write` may block (that is the backpressure: the
//! worker waits); any non-zero return from `sz_is_create`, `sz_is_write`,
//! `sz_is_flush` or `sz_is_close` is FATAL for the consumer (orderly shutdown,
//! non-zero exit) — INFO is never silently dropped.
//!
//! # C ABI (version [`ABI_VERSION`])
//!
//! ```c
//! uint32_t sz_is_abi_version(void);
//! // NULL on failure; *err_out then holds a message freed with sz_is_free.
//! void*    sz_is_create(const char* cfg_utf8, char** err_out);
//! // kind: 0 ADD, 1 DELETE, 2 REDO. info/len: the INFO JSON exactly as the
//! // engine returned it (consumer-owned, valid for the call, not
//! // NUL-terminated). 0 ok, -1 fatal (*err_out set).
//! int32_t  sz_is_write(void* h, uint32_t kind, const char* info, size_t len,
//!                      char** err_out);
//! // Seal the current output; the next write starts a NEW one. 0 / -1.
//! int32_t  sz_is_flush(void* h, char** err_out);
//! // Final seal. Called once, after the workers joined and before the engine
//! // is torn down (also on the SIGTERM path). Writes after it fail. 0 / -1.
//! int32_t  sz_is_close(void* h, char** err_out);
//! void     sz_is_free(char* p);
//! void     sz_is_destroy(void* h);   // the library is never dlclose'd
//! ```
//!
//! Ownership: every `char*` the plugin returns is plugin-allocated and is
//! released only through `sz_is_free`. `cfg_utf8` is NUL-terminated (empty
//! string when no config is given). No panic/exception may cross the boundary.
//! The consumer never calls `sz_is_write` with kind 1 (DELETE): it is reserved
//! for a delete program sharing the same plugins.
//!
//! Rust plugins implement [`InfoSink`] and invoke [`export_info_sink!`], which
//! generates all seven symbols, with `catch_unwind` around every entry point
//! that runs plugin code.

/// Bumped on any incompatible change to the symbol set or semantics.
pub const ABI_VERSION: u32 = 1;

pub const SZ_IS_OK: i32 = 0;
pub const SZ_IS_ERROR: i32 = -1;

pub const SZ_IS_KIND_ADD: u32 = 0;
pub const SZ_IS_KIND_DELETE: u32 = 1;
pub const SZ_IS_KIND_REDO: u32 = 2;

pub const SYM_ABI_VERSION: &[u8] = b"sz_is_abi_version\0";
pub const SYM_CREATE: &[u8] = b"sz_is_create\0";
pub const SYM_WRITE: &[u8] = b"sz_is_write\0";
pub const SYM_FLUSH: &[u8] = b"sz_is_flush\0";
pub const SYM_CLOSE: &[u8] = b"sz_is_close\0";
pub const SYM_FREE: &[u8] = b"sz_is_free\0";
pub const SYM_DESTROY: &[u8] = b"sz_is_destroy\0";

pub type AbiVersionFn = unsafe extern "C" fn() -> u32;
pub type CreateFn = unsafe extern "C" fn(
    config: *const std::ffi::c_char,
    err_out: *mut *mut std::ffi::c_char,
) -> *mut std::ffi::c_void;
pub type WriteFn = unsafe extern "C" fn(
    handle: *mut std::ffi::c_void,
    kind: u32,
    info: *const std::ffi::c_char,
    len: usize,
    err_out: *mut *mut std::ffi::c_char,
) -> i32;
/// Shared by `sz_is_flush` and `sz_is_close`.
pub type SealFn =
    unsafe extern "C" fn(handle: *mut std::ffi::c_void, err_out: *mut *mut std::ffi::c_char) -> i32;
pub type FreeFn = unsafe extern "C" fn(p: *mut std::ffi::c_char);
pub type DestroyFn = unsafe extern "C" fn(handle: *mut std::ffi::c_void);

/// Which engine call produced an INFO payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InfoKind {
    Add,
    Delete,
    Redo,
}

impl InfoKind {
    /// The ABI code of this kind.
    pub fn code(self) -> u32 {
        match self {
            Self::Add => SZ_IS_KIND_ADD,
            Self::Delete => SZ_IS_KIND_DELETE,
            Self::Redo => SZ_IS_KIND_REDO,
        }
    }

    /// Decodes an ABI kind code; `None` for an unknown code.
    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            SZ_IS_KIND_ADD => Some(Self::Add),
            SZ_IS_KIND_DELETE => Some(Self::Delete),
            SZ_IS_KIND_REDO => Some(Self::Redo),
            _ => None,
        }
    }
}

/// An INFO sink. Every method is called on one shared instance from many
/// threads; `Err` is fatal for the consumer.
pub trait InfoSink: Send + Sync {
    /// Persists one INFO payload (`info` is exactly the engine's JSON).
    fn write(&self, kind: InfoKind, info: &str) -> Result<(), String>;
    /// Seals the current output; the next write starts a new one.
    fn flush(&self) -> Result<(), String>;
    /// Final seal; later writes must fail.
    fn close(&self) -> Result<(), String>;
}

/// Plugin-side helpers used by [`export_info_sink!`] (the record-transform
/// ABI's, reused so both plugin kinds share one set of conventions). Not part
/// of the consumer-facing API.
#[doc(hidden)]
pub mod plugin {
    pub use sz_record_transform::plugin::{config_str, free, panic_msg, set_err};

    /// Maps a plugin result onto the ABI return code, writing the message to
    /// `err_out` on failure.
    ///
    /// # Safety
    /// `err_out` must be null or valid for a pointer write.
    pub unsafe fn status(
        r: std::thread::Result<Result<(), String>>,
        what: &str,
        err_out: *mut *mut std::ffi::c_char,
    ) -> i32 {
        let msg = match r {
            Ok(Ok(())) => return crate::SZ_IS_OK,
            Ok(Err(e)) => e,
            Err(p) => format!("panic in {what}: {}", panic_msg(&*p)),
        };
        // SAFETY: forwarded contract.
        unsafe { set_err(err_out, &msg) };
        crate::SZ_IS_ERROR
    }
}

/// Exports the seven ABI symbols for a type implementing [`InfoSink`] plus a
/// constructor `fn(&str) -> Result<T, String>` taking the plugin config string.
#[macro_export]
macro_rules! export_info_sink {
    ($ty:ty, $ctor:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn sz_is_abi_version() -> u32 {
            $crate::ABI_VERSION
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_create(
            config: *const ::std::ffi::c_char,
            err_out: *mut *mut ::std::ffi::c_char,
        ) -> *mut ::std::ffi::c_void {
            let r = ::std::panic::catch_unwind(|| {
                // SAFETY: consumer passes a NUL-terminated string.
                let cfg = unsafe { $crate::plugin::config_str(config) }?;
                let t: $ty = $ctor(cfg)?;
                Ok::<_, String>(Box::into_raw(Box::new(t)) as *mut ::std::ffi::c_void)
            });
            let msg = match r {
                Ok(Ok(h)) => return h,
                Ok(Err(e)) => e,
                Err(p) => format!("panic in sz_is_create: {}", $crate::plugin::panic_msg(&*p)),
            };
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::set_err(err_out, &msg) };
            ::std::ptr::null_mut()
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_write(
            handle: *mut ::std::ffi::c_void,
            kind: u32,
            info: *const ::std::ffi::c_char,
            len: usize,
            err_out: *mut *mut ::std::ffi::c_char,
        ) -> i32 {
            let r = ::std::panic::catch_unwind(|| {
                if handle.is_null() || info.is_null() {
                    return Err("null handle or info".to_string());
                }
                let kind = $crate::InfoKind::from_code(kind)
                    .ok_or_else(|| format!("unknown INFO kind {kind}"))?;
                // SAFETY: handle came from sz_is_create; info is valid for len bytes.
                let t = unsafe { &*(handle as *const $ty) };
                let bytes = unsafe { ::std::slice::from_raw_parts(info as *const u8, len) };
                let s = ::std::str::from_utf8(bytes).map_err(|e| format!("INFO not UTF-8: {e}"))?;
                $crate::InfoSink::write(t, kind, s)
            });
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::status(r, "sz_is_write", err_out) }
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_flush(
            handle: *mut ::std::ffi::c_void,
            err_out: *mut *mut ::std::ffi::c_char,
        ) -> i32 {
            let r = ::std::panic::catch_unwind(|| {
                if handle.is_null() {
                    return Err("null handle".to_string());
                }
                // SAFETY: handle came from sz_is_create.
                $crate::InfoSink::flush(unsafe { &*(handle as *const $ty) })
            });
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::status(r, "sz_is_flush", err_out) }
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_close(
            handle: *mut ::std::ffi::c_void,
            err_out: *mut *mut ::std::ffi::c_char,
        ) -> i32 {
            let r = ::std::panic::catch_unwind(|| {
                if handle.is_null() {
                    return Err("null handle".to_string());
                }
                // SAFETY: handle came from sz_is_create.
                $crate::InfoSink::close(unsafe { &*(handle as *const $ty) })
            });
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::status(r, "sz_is_close", err_out) }
        }

        /// # Safety
        /// `p` must be null or a string returned by this plugin.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_free(p: *mut ::std::ffi::c_char) {
            // SAFETY: forwarded contract.
            unsafe { $crate::plugin::free(p) }
        }

        /// # Safety
        /// `handle` must be null or a handle from `sz_is_create`, destroyed once.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_is_destroy(handle: *mut ::std::ffi::c_void) {
            if !handle.is_null() {
                let _ = ::std::panic::catch_unwind(|| {
                    // SAFETY: handle came from Box::into_raw in sz_is_create.
                    drop(unsafe { Box::from_raw(handle as *mut $ty) });
                });
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString, c_char};
    use std::sync::Mutex;

    /// Records every call; `"fail"` config makes create fail, a `PANIC`
    /// payload panics inside write, `"ERR"` errors.
    struct Recorder {
        lines: Mutex<Vec<(InfoKind, String)>>,
    }

    impl Recorder {
        fn new(cfg: &str) -> Result<Self, String> {
            match cfg {
                "fail" => Err("bad config".to_string()),
                "panic" => panic!("create panicked"),
                _ => Ok(Self {
                    lines: Mutex::new(Vec::new()),
                }),
            }
        }
    }

    impl InfoSink for Recorder {
        fn write(&self, kind: InfoKind, info: &str) -> Result<(), String> {
            match info {
                "PANIC" => panic!("write panicked"),
                "ERR" => Err("write failed".to_string()),
                _ => {
                    self.lines.lock().unwrap().push((kind, info.to_string()));
                    Ok(())
                }
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
        fn close(&self) -> Result<(), String> {
            Err("already closed".to_string())
        }
    }

    export_info_sink!(Recorder, Recorder::new);

    fn take_err(p: *mut c_char) -> String {
        assert!(!p.is_null(), "expected an error message");
        // SAFETY: produced by set_err in this crate.
        let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
        // SAFETY: plugin-owned.
        unsafe { sz_is_free(p) };
        s
    }

    fn create(cfg: &str) -> (*mut std::ffi::c_void, *mut c_char) {
        let c = CString::new(cfg).unwrap();
        let mut err = std::ptr::null_mut();
        // SAFETY: valid string and out-pointer.
        let h = unsafe { sz_is_create(c.as_ptr(), &mut err) };
        (h, err)
    }

    fn write(h: *mut std::ffi::c_void, kind: u32, info: &str) -> (i32, *mut c_char) {
        let mut err = std::ptr::null_mut();
        // SAFETY: info valid for its length.
        let rc = unsafe { sz_is_write(h, kind, info.as_ptr().cast(), info.len(), &mut err) };
        (rc, err)
    }

    #[test]
    fn kind_codes_round_trip_and_match_the_contract() {
        for k in [InfoKind::Add, InfoKind::Delete, InfoKind::Redo] {
            assert_eq!(InfoKind::from_code(k.code()), Some(k));
        }
        assert_eq!(
            (SZ_IS_KIND_ADD, SZ_IS_KIND_DELETE, SZ_IS_KIND_REDO),
            (0, 1, 2)
        );
        assert_eq!(InfoKind::from_code(3), None);
        assert_eq!(sz_is_abi_version(), ABI_VERSION);
    }

    #[test]
    fn symbol_names_are_nul_terminated_and_prefixed() {
        for s in [
            SYM_ABI_VERSION,
            SYM_CREATE,
            SYM_WRITE,
            SYM_FLUSH,
            SYM_CLOSE,
            SYM_FREE,
            SYM_DESTROY,
        ] {
            assert_eq!(s.last(), Some(&0));
            assert!(s.starts_with(b"sz_is_"));
        }
    }

    #[test]
    fn create_write_flush_close_destroy_through_the_abi() {
        let (h, err) = create("");
        assert!(!h.is_null() && err.is_null());
        assert_eq!(write(h, SZ_IS_KIND_ADD, r#"{"A":1}"#).0, SZ_IS_OK);
        assert_eq!(write(h, SZ_IS_KIND_REDO, r#"{"B":2}"#).0, SZ_IS_OK);
        // SAFETY: h is a live Recorder handle.
        let rec = unsafe { &*(h as *const Recorder) };
        assert_eq!(
            *rec.lines.lock().unwrap(),
            vec![
                (InfoKind::Add, r#"{"A":1}"#.to_string()),
                (InfoKind::Redo, r#"{"B":2}"#.to_string())
            ]
        );
        let mut err = std::ptr::null_mut();
        // SAFETY: live handle.
        assert_eq!(unsafe { sz_is_flush(h, &mut err) }, SZ_IS_OK);
        // SAFETY: live handle.
        assert_eq!(unsafe { sz_is_close(h, &mut err) }, SZ_IS_ERROR);
        assert_eq!(take_err(err), "already closed");
        // SAFETY: destroyed once.
        unsafe { sz_is_destroy(h) };
        // SAFETY: null is a no-op.
        unsafe { sz_is_destroy(std::ptr::null_mut()) };
    }

    #[test]
    fn errors_and_panics_become_messages_never_unwind() {
        let (h, err) = create("fail");
        assert!(h.is_null());
        assert_eq!(take_err(err), "bad config");
        let (h, err) = create("panic");
        assert!(h.is_null());
        assert!(take_err(err).contains("panic in sz_is_create: create panicked"));

        let (h, _) = create("");
        let (rc, err) = write(h, SZ_IS_KIND_ADD, "ERR");
        assert_eq!(
            (rc, take_err(err)),
            (SZ_IS_ERROR, "write failed".to_string())
        );
        let (rc, err) = write(h, SZ_IS_KIND_ADD, "PANIC");
        assert_eq!(rc, SZ_IS_ERROR);
        assert!(take_err(err).contains("panic in sz_is_write: write panicked"));
        let (rc, err) = write(h, 9, "{}");
        assert_eq!(
            (rc, take_err(err)),
            (SZ_IS_ERROR, "unknown INFO kind 9".to_string())
        );
        let bad = [0xffu8];
        let mut err = std::ptr::null_mut();
        // SAFETY: valid buffer.
        let rc = unsafe { sz_is_write(h, 0, bad.as_ptr().cast(), 1, &mut err) };
        assert_eq!(rc, SZ_IS_ERROR);
        assert!(take_err(err).contains("not UTF-8"));
        // SAFETY: destroyed once.
        unsafe { sz_is_destroy(h) };
    }

    #[test]
    fn null_handles_are_errors() {
        let (rc, err) = write(std::ptr::null_mut(), 0, "{}");
        assert_eq!(
            (rc, take_err(err)),
            (SZ_IS_ERROR, "null handle or info".to_string())
        );
        let mut err = std::ptr::null_mut();
        // SAFETY: null handle is rejected before use.
        assert_eq!(
            unsafe { sz_is_flush(std::ptr::null_mut(), &mut err) },
            SZ_IS_ERROR
        );
        assert_eq!(take_err(err), "null handle");
        let mut err = std::ptr::null_mut();
        // SAFETY: null handle is rejected before use.
        assert_eq!(
            unsafe { sz_is_close(std::ptr::null_mut(), &mut err) },
            SZ_IS_ERROR
        );
        assert_eq!(take_err(err), "null handle");
    }
}
