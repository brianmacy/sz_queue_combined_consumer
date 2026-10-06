//! Record-transform hook: an optional plugin that rewrites each load record
//! before `add_record` (contract: the `sz-record-transform` crate).
//!
//! The plugin is `dlopen`ed once at startup (fail-fast, before `Sz_init`) and
//! shared by every worker thread. It runs in `worker::process_load`, the single
//! call site common to the RabbitMQ, SQS and file backends.

use std::borrow::Cow;
use std::ffi::{CStr, CString, c_char, c_void};
use std::fmt;
use std::sync::Arc;

pub use sz_record_transform::RecordTransform;
use sz_record_transform::{
    ABI_VERSION, AbiVersionFn, CreateFn, DestroyFn, FreeFn, SYM_ABI_VERSION, SYM_CREATE,
    SYM_DESTROY, SYM_FREE, SYM_TRANSFORM, SZ_RT_REPLACED, SZ_RT_UNCHANGED, TransformFn,
};

/// Shared, optional transform as stored in `Config` / `WorkerCtx`.
#[derive(Clone, Default)]
pub struct TransformHandle(pub Option<Arc<dyn RecordTransform>>);

impl fmt::Debug for TransformHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "TransformHandle(Some)"
        } else {
            "TransformHandle(None)"
        })
    }
}

impl TransformHandle {
    /// Loads `path` (if given) with `config` (empty when `None`).
    pub fn load(path: Option<&str>, config: Option<&str>) -> Result<Self, String> {
        match path.filter(|p| !p.is_empty()) {
            None => Ok(Self(None)),
            Some(p) => {
                let plugin = PluginTransform::open(p, config.unwrap_or(""))?;
                tracing::info!("record transform plugin loaded: {p}");
                Ok(Self(Some(Arc::new(plugin))))
            }
        }
    }
}

/// A transform backed by a `dlopen`ed shared library.
pub struct PluginTransform {
    lib: *mut c_void,
    handle: *mut c_void,
    transform: TransformFn,
    free: FreeFn,
    destroy: DestroyFn,
}

// SAFETY: the ABI requires sz_rt_transform to be thread-safe on one handle;
// the library and handle are immutable after construction.
unsafe impl Send for PluginTransform {}
// SAFETY: see above.
unsafe impl Sync for PluginTransform {}

fn dl_error() -> String {
    // SAFETY: dlerror returns null or a valid NUL-terminated thread-local string.
    let e = unsafe { libc::dlerror() };
    if e.is_null() {
        "unknown dlopen error".to_string()
    } else {
        // SAFETY: non-null from dlerror.
        unsafe { CStr::from_ptr(e) }.to_string_lossy().into_owned()
    }
}

/// # Safety
/// `lib` must be a live dlopen handle and `T` the correct fn-pointer type.
unsafe fn sym<T: Copy>(lib: *mut c_void, name: &[u8]) -> Result<T, String> {
    // SAFETY: name is a NUL-terminated constant from the ABI crate.
    let p = unsafe { libc::dlsym(lib, name.as_ptr().cast()) };
    if p.is_null() {
        return Err(format!(
            "missing symbol {}: {}",
            String::from_utf8_lossy(&name[..name.len() - 1]),
            dl_error()
        ));
    }
    // SAFETY: T is a fn pointer of pointer size; the plugin exports this symbol
    // with the matching signature per the ABI contract.
    Ok(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&p) })
}

impl PluginTransform {
    pub fn open(path: &str, config: &str) -> Result<Self, String> {
        let cpath = CString::new(path).map_err(|_| "plugin path contains NUL".to_string())?;
        let cconfig = CString::new(config).map_err(|_| "plugin config contains NUL".to_string())?;
        // SAFETY: valid C string; RTLD_LOCAL keeps plugin symbols private.
        let lib = unsafe { libc::dlopen(cpath.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if lib.is_null() {
            return Err(format!("dlopen {path}: {}", dl_error()));
        }
        // On any failure below the library stays loaded (process exits anyway);
        // dlclose of a half-initialized plugin is not worth the risk.
        let fail = |e: String| format!("plugin {path}: {e}");
        // SAFETY: lib is live; types match the ABI.
        let (abi, create, transform, free, destroy) = unsafe {
            (
                sym::<AbiVersionFn>(lib, SYM_ABI_VERSION).map_err(fail)?,
                sym::<CreateFn>(lib, SYM_CREATE).map_err(fail)?,
                sym::<TransformFn>(lib, SYM_TRANSFORM).map_err(fail)?,
                sym::<FreeFn>(lib, SYM_FREE).map_err(fail)?,
                sym::<DestroyFn>(lib, SYM_DESTROY).map_err(fail)?,
            )
        };
        // SAFETY: resolved above.
        let v = unsafe { abi() };
        if v != ABI_VERSION {
            return Err(fail(format!(
                "ABI version {v}, consumer requires {ABI_VERSION}"
            )));
        }
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: valid config string and err out-pointer.
        let handle = unsafe { create(cconfig.as_ptr(), &mut err) };
        if handle.is_null() {
            // SAFETY: err is null or plugin-owned.
            let msg = unsafe { take_string(free, err) }
                .unwrap_or_else(|| "sz_rt_create returned NULL".to_string());
            return Err(fail(format!("init failed: {msg}")));
        }
        Ok(Self {
            lib,
            handle,
            transform,
            free,
            destroy,
        })
    }
}

/// Copies and frees a plugin-owned C string.
///
/// # Safety
/// `p` must be null or a NUL-terminated string owned by the plugin.
unsafe fn take_string(free: FreeFn, p: *mut c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    // SAFETY: non-null plugin string.
    let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    // SAFETY: plugin-owned pointer released through the plugin's allocator.
    unsafe { free(p) };
    Some(s)
}

impl RecordTransform for PluginTransform {
    fn transform<'a>(&self, record: &'a str) -> Result<Cow<'a, str>, String> {
        let mut out: *mut c_char = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: handle is live; record is valid for its length for the call.
        let rc = unsafe {
            (self.transform)(
                self.handle,
                record.as_ptr().cast(),
                record.len(),
                &mut out,
                &mut out_len,
                &mut err,
            )
        };
        match rc {
            SZ_RT_UNCHANGED => Ok(Cow::Borrowed(record)),
            SZ_RT_REPLACED if !out.is_null() => {
                // SAFETY: plugin wrote out_len bytes at out.
                let bytes = unsafe { std::slice::from_raw_parts(out as *const u8, out_len) };
                let r = std::str::from_utf8(bytes)
                    .map(|s| Cow::Owned(s.to_string()))
                    .map_err(|e| format!("plugin output is not UTF-8: {e}"));
                // SAFETY: plugin-owned buffer.
                unsafe { (self.free)(out) };
                r
            }
            _ => {
                // ABI violation (REPLACED with null out, or ERROR that also
                // wrote out): release any buffer rather than leak it.
                if !out.is_null() {
                    // SAFETY: plugin-owned buffer.
                    unsafe { (self.free)(out) };
                }
                // SAFETY: err is null or plugin-owned.
                Err(unsafe { take_string(self.free, err) }
                    .unwrap_or_else(|| format!("plugin returned {rc} without a message")))
            }
        }
    }
}

impl Drop for PluginTransform {
    fn drop(&mut self) {
        // SAFETY: handle from sz_rt_create, destroyed exactly once. The library
        // is intentionally NOT dlclose'd: plugins (e.g. ONNX runtimes) may own
        // threads or TLS destructors that outlive the handle.
        unsafe { (self.destroy)(self.handle) };
        let _ = self.lib;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_path_is_none() {
        assert!(TransformHandle::load(None, None).unwrap().0.is_none());
        assert!(TransformHandle::load(Some(""), None).unwrap().0.is_none());
    }

    #[test]
    fn missing_library_fails_loudly() {
        let e =
            TransformHandle::load(Some("/nonexistent/libnope.so"), None).expect_err("must fail");
        assert!(e.contains("dlopen"), "{e}");
    }

    /// The example plugin cdylib, built as a dev-dependency next to this test.
    fn example_plugin() -> String {
        let deps = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let p = deps.join(format!(
            "{}sz_record_transform_example.{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_EXTENSION
        ));
        assert!(p.exists(), "example plugin not built at {p:?}");
        p.to_string_lossy().into_owned()
    }

    fn load_example(config: &str) -> Arc<dyn RecordTransform> {
        TransformHandle::load(Some(&example_plugin()), Some(config))
            .unwrap()
            .0
            .unwrap()
    }

    #[test]
    fn plugin_replaces_record() {
        let t = load_example(r#"{"ADDED":"Y","DATA_SOURCE":"TEST"}"#);
        let out = t
            .transform(r#"{"DATA_SOURCE":"OTHER","RECORD_ID":"1"}"#)
            .unwrap();
        assert!(matches!(out, Cow::Owned(_)), "expected a replaced record");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["ADDED"], "Y");
        assert_eq!(v["DATA_SOURCE"], "TEST");
        assert_eq!(v["RECORD_ID"], "1");
    }

    #[test]
    fn plugin_unchanged_is_borrowed() {
        let t = load_example("");
        let rec = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"1"}"#;
        assert!(matches!(t.transform(rec).unwrap(), Cow::Borrowed(b) if b == rec));
    }

    #[test]
    fn plugin_error_propagates_message() {
        let t = load_example("");
        let e = t
            .transform(r#"{"DATA_SOURCE":"TEST","RECORD_ID":"1","SZ_RT_FORCE_ERROR":1}"#)
            .unwrap_err();
        assert_eq!(e, "forced error");
        let e = t.transform("not json").unwrap_err();
        assert!(e.contains("not valid JSON"), "{e}");
    }

    #[test]
    fn plugin_borrowed_subslice_is_a_change() {
        let t = load_example("");
        let rec = "  {\"DATA_SOURCE\":\"TEST\",\"RECORD_ID\":\"1\",\"SZ_RT_BORROW_TRIMMED\":1}  ";
        let out = t.transform(rec).unwrap();
        assert!(
            matches!(out, Cow::Owned(_)),
            "borrowed sub-slice must not read as unchanged"
        );
        assert_eq!(out, rec.trim());
    }

    #[test]
    fn plugin_init_failure_is_loud() {
        let e = TransformHandle::load(Some(&example_plugin()), Some("[1]")).expect_err("must fail");
        assert!(
            e.contains("init failed: config must be a JSON object"),
            "{e}"
        );
    }

    #[test]
    fn plugin_is_thread_safe_on_one_handle() {
        let t = load_example(r#"{"ADDED":"Y"}"#);
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let t = t.clone();
                std::thread::spawn(move || {
                    for j in 0..500 {
                        let rec = format!(r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"{i}-{j}"}}"#);
                        let v: serde_json::Value =
                            serde_json::from_str(&t.transform(&rec).unwrap()).unwrap();
                        assert_eq!(v["RECORD_ID"], format!("{i}-{j}"));
                        assert_eq!(v["ADDED"], "Y");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }
    }
}
