//! Record-transform plugin contract.
//!
//! A plugin is a shared library (any language) that rewrites each load record
//! JSON before the consumer hands it to `add_record`. The consumer `dlopen`s it
//! once per process (`--record-transform-plugin`) and calls it concurrently
//! from every worker thread, so `sz_rt_transform` MUST be thread-safe on a
//! single handle.
//!
//! # C ABI (version [`ABI_VERSION`])
//!
//! ```c
//! uint32_t sz_rt_abi_version(void);
//! // NULL on failure; *err_out then holds a message freed with sz_rt_free.
//! void*    sz_rt_create(const char* config_utf8, char** err_out);
//! // SZ_RT_UNCHANGED: record used as-is (*out untouched).
//! // SZ_RT_REPLACED:  *out/*out_len hold the new UTF-8 record (free with sz_rt_free).
//! // SZ_RT_ERROR:     *err_out holds a message; the record is rejected (dead-letter).
//! int32_t  sz_rt_transform(void* handle, const char* in, size_t in_len,
//!                          char** out, size_t* out_len, char** err_out);
//! void     sz_rt_free(char* p);
//! void     sz_rt_destroy(void* handle);
//! ```
//!
//! Ownership: every `char*` the plugin returns is plugin-allocated and is
//! released only through `sz_rt_free`. `in` is consumer-owned, valid only for
//! the call, and not NUL-terminated. `config_utf8` is NUL-terminated (empty
//! string when no config is given). No panic/exception may cross the boundary.
//!
//! Rust plugins implement [`RecordTransform`] and invoke
//! [`export_record_transform!`], which generates all five symbols, with
//! `catch_unwind` around every entry point that runs plugin code
//! (`sz_rt_create`, `sz_rt_transform`, `sz_rt_destroy`).

use std::borrow::Cow;

/// Bumped on any incompatible change to the symbol set or semantics.
pub const ABI_VERSION: u32 = 1;

pub const SZ_RT_UNCHANGED: i32 = 0;
pub const SZ_RT_REPLACED: i32 = 1;
pub const SZ_RT_ERROR: i32 = -1;

pub const SYM_ABI_VERSION: &[u8] = b"sz_rt_abi_version\0";
pub const SYM_CREATE: &[u8] = b"sz_rt_create\0";
pub const SYM_TRANSFORM: &[u8] = b"sz_rt_transform\0";
pub const SYM_FREE: &[u8] = b"sz_rt_free\0";
pub const SYM_DESTROY: &[u8] = b"sz_rt_destroy\0";

pub type AbiVersionFn = unsafe extern "C" fn() -> u32;
pub type CreateFn = unsafe extern "C" fn(
    config: *const std::ffi::c_char,
    err_out: *mut *mut std::ffi::c_char,
) -> *mut std::ffi::c_void;
pub type TransformFn = unsafe extern "C" fn(
    handle: *mut std::ffi::c_void,
    input: *const std::ffi::c_char,
    in_len: usize,
    out: *mut *mut std::ffi::c_char,
    out_len: *mut usize,
    err_out: *mut *mut std::ffi::c_char,
) -> i32;
pub type FreeFn = unsafe extern "C" fn(p: *mut std::ffi::c_char);
pub type DestroyFn = unsafe extern "C" fn(handle: *mut std::ffi::c_void);

/// A record transform. `Cow::Borrowed` means "unchanged" (no copy crosses the
/// boundary); `Cow::Owned` replaces the record. `Err` rejects the record.
pub trait RecordTransform: Send + Sync {
    fn transform<'a>(&self, record: &'a str) -> Result<Cow<'a, str>, String>;
}

/// Plugin-side helpers used by [`export_record_transform!`]. Not part of the
/// consumer-facing API.
#[doc(hidden)]
pub mod plugin {
    use std::ffi::{CStr, CString, c_char};

    /// Writes `msg` to `err_out` as a plugin-owned C string (interior NULs are
    /// replaced so the message is never silently dropped).
    ///
    /// # Safety
    /// `err_out` must be null or valid for a pointer write.
    pub unsafe fn set_err(err_out: *mut *mut c_char, msg: &str) {
        if err_out.is_null() {
            return;
        }
        let c = CString::new(msg.replace('\0', "\\0")).unwrap_or_default();
        // SAFETY: caller guarantees err_out is valid for writes.
        unsafe { *err_out = c.into_raw() };
    }

    /// Returns a heap copy of `bytes` the consumer releases with `sz_rt_free`.
    pub fn into_raw_bytes(s: String) -> (*mut c_char, usize) {
        let len = s.len();
        // Records are JSON text: a NUL would be invalid input to the engine
        // anyway, and CString keeps free symmetric with into_raw.
        match CString::new(s) {
            Ok(c) => (c.into_raw(), len),
            Err(_) => (std::ptr::null_mut(), 0),
        }
    }

    /// # Safety
    /// `p` must be null or a pointer previously returned by this module.
    pub unsafe fn free(p: *mut c_char) {
        if !p.is_null() {
            // SAFETY: produced by CString::into_raw in this module.
            drop(unsafe { CString::from_raw(p) });
        }
    }

    /// # Safety
    /// `config` must be null or a valid NUL-terminated string.
    pub unsafe fn config_str<'a>(config: *const c_char) -> Result<&'a str, String> {
        if config.is_null() {
            return Ok("");
        }
        // SAFETY: caller guarantees a valid NUL-terminated string.
        unsafe { CStr::from_ptr(config) }
            .to_str()
            .map_err(|e| format!("plugin config is not UTF-8: {e}"))
    }

    pub fn panic_msg(p: &(dyn std::any::Any + Send)) -> String {
        p.downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string())
    }
}

/// Exports the five ABI symbols for a type implementing [`RecordTransform`]
/// plus a constructor `fn(&str) -> Result<T, String>` taking the plugin
/// config string.
#[macro_export]
macro_rules! export_record_transform {
    ($ty:ty, $ctor:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn sz_rt_abi_version() -> u32 {
            $crate::ABI_VERSION
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_rt_create(
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
                Err(p) => format!("panic in sz_rt_create: {}", $crate::plugin::panic_msg(&*p)),
            };
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::set_err(err_out, &msg) };
            ::std::ptr::null_mut()
        }

        /// # Safety
        /// See the crate-level ABI contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_rt_transform(
            handle: *mut ::std::ffi::c_void,
            input: *const ::std::ffi::c_char,
            in_len: usize,
            out: *mut *mut ::std::ffi::c_char,
            out_len: *mut usize,
            err_out: *mut *mut ::std::ffi::c_char,
        ) -> i32 {
            let r = ::std::panic::catch_unwind(|| {
                if handle.is_null() || input.is_null() {
                    return Err("null handle or input".to_string());
                }
                // SAFETY: handle came from sz_rt_create; input is valid for in_len bytes.
                let t = unsafe { &*(handle as *const $ty) };
                let bytes = unsafe { ::std::slice::from_raw_parts(input as *const u8, in_len) };
                let s =
                    ::std::str::from_utf8(bytes).map_err(|e| format!("input not UTF-8: {e}"))?;
                $crate::RecordTransform::transform(t, s).map(|c| match c {
                    // Only the input itself means "unchanged"; a borrowed
                    // sub-slice is a real change and must not be dropped.
                    ::std::borrow::Cow::Borrowed(b)
                        if b.as_ptr() == s.as_ptr() && b.len() == s.len() =>
                    {
                        None
                    }
                    ::std::borrow::Cow::Borrowed(b) => Some(b.to_owned()),
                    ::std::borrow::Cow::Owned(o) => Some(o),
                })
            });
            let msg = match r {
                Ok(Ok(None)) => return $crate::SZ_RT_UNCHANGED,
                Ok(Ok(Some(o))) => {
                    let (p, n) = $crate::plugin::into_raw_bytes(o);
                    if p.is_null() {
                        "transformed record contains a NUL byte".to_string()
                    } else {
                        // SAFETY: out/out_len are writable per the ABI.
                        unsafe {
                            *out = p;
                            *out_len = n;
                        }
                        return $crate::SZ_RT_REPLACED;
                    }
                }
                Ok(Err(e)) => e,
                Err(p) => format!(
                    "panic in sz_rt_transform: {}",
                    $crate::plugin::panic_msg(&*p)
                ),
            };
            // SAFETY: err_out is null or writable per the ABI.
            unsafe { $crate::plugin::set_err(err_out, &msg) };
            $crate::SZ_RT_ERROR
        }

        /// # Safety
        /// `p` must be null or a string returned by this plugin.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_rt_free(p: *mut ::std::ffi::c_char) {
            // SAFETY: forwarded contract.
            unsafe { $crate::plugin::free(p) }
        }

        /// # Safety
        /// `handle` must be null or a handle from `sz_rt_create`, destroyed once.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn sz_rt_destroy(handle: *mut ::std::ffi::c_void) {
            if !handle.is_null() {
                let _ = ::std::panic::catch_unwind(|| {
                    // SAFETY: handle came from Box::into_raw in sz_rt_create.
                    drop(unsafe { Box::from_raw(handle as *mut $ty) });
                });
            }
        }
    };
}
