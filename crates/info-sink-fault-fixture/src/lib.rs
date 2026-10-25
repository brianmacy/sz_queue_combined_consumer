//! Test fixture: a real INFO sink plugin (built with `export_info_sink!`, so
//! it goes through the same C ABI and loader as any production plugin) that
//! misbehaves on command, for the consumer's sink-failure and stuck-sink
//! tests.
//!
//! Config (JSON object): `{"fail":["flush"],"block":["close"],
//! "sleep_ms":{"write":3000}}` — `fail` / `block` list entry points
//! (`"write"`, `"flush"`, `"close"`): a `fail` entry returns an error, a
//! `block` entry never returns (a wedged plugin, e.g. a hung network volume);
//! `sleep_ms` makes an entry point take that long and then succeed (a slow
//! volume). Every other call succeeds; written lines are discarded. Unknown
//! keys or entry points are a create error.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use sz_info_sink::{InfoKind, InfoSink, export_info_sink};

/// Entry points a config may name.
const OPS: [&str; 3] = ["write", "flush", "close"];

/// The misbehaving sink.
pub struct FaultSink {
    fail: HashSet<String>,
    block: HashSet<String>,
    sleep: HashMap<String, Duration>,
}

impl FaultSink {
    /// Parses the config (see the crate docs).
    pub fn new(cfg: &str) -> Result<Self, String> {
        let v: serde_json::Value =
            serde_json::from_str(cfg).map_err(|e| format!("fault fixture config: {e}"))?;
        let obj = v.as_object().ok_or("fault fixture config: not an object")?;
        let mut fail = HashSet::new();
        let mut block = HashSet::new();
        let mut sleep = HashMap::new();
        for (key, value) in obj {
            if key == "sleep_ms" {
                let ops = value
                    .as_object()
                    .ok_or("fault fixture config: sleep_ms map")?;
                for (op, ms) in ops {
                    let ms = ms.as_u64().ok_or("fault fixture config: sleep_ms values")?;
                    sleep.insert(entry_point(op)?, Duration::from_millis(ms));
                }
                continue;
            }
            let set = match key.as_str() {
                "fail" => &mut fail,
                "block" => &mut block,
                other => return Err(format!("fault fixture config: unknown key {other}")),
            };
            for op in value.as_array().ok_or("fault fixture config: lists only")? {
                set.insert(entry_point(op.as_str().unwrap_or_default())?);
            }
        }
        Ok(Self { fail, block, sleep })
    }

    /// Applies the configured fault for `op`.
    fn call(&self, op: &str) -> Result<(), String> {
        if let Some(d) = self.sleep.get(op) {
            std::thread::sleep(*d);
        }
        if self.block.contains(op) {
            loop {
                std::thread::park();
            }
        }
        if self.fail.contains(op) {
            return Err(format!("fault fixture: {op} failed on command"));
        }
        Ok(())
    }
}

impl InfoSink for FaultSink {
    fn write(&self, _kind: InfoKind, _info: &str) -> Result<(), String> {
        self.call("write")
    }

    fn flush(&self) -> Result<(), String> {
        self.call("flush")
    }

    fn close(&self) -> Result<(), String> {
        self.call("close")
    }
}

/// Validates an entry-point name.
fn entry_point(op: &str) -> Result<String, String> {
    if OPS.contains(&op) {
        Ok(op.to_string())
    } else {
        Err(format!("fault fixture config: unknown entry point {op:?}"))
    }
}

export_info_sink!(FaultSink, FaultSink::new);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_faults_fail_and_the_rest_succeed() {
        let s = FaultSink::new(r#"{"fail":["flush","close"]}"#).unwrap();
        assert!(s.write(InfoKind::Add, "{}").is_ok());
        assert!(s.flush().unwrap_err().contains("flush failed on command"));
        assert!(s.close().unwrap_err().contains("close failed on command"));
        let s = FaultSink::new("{}").unwrap();
        assert!(s.flush().is_ok() && s.close().is_ok());
    }

    #[test]
    fn a_slow_entry_point_sleeps_then_succeeds() {
        let s = FaultSink::new(r#"{"sleep_ms":{"write":200}}"#).unwrap();
        let t0 = std::time::Instant::now();
        assert!(s.write(InfoKind::Add, "{}").is_ok());
        assert!(t0.elapsed() >= Duration::from_millis(200));
    }

    #[test]
    fn a_blocked_entry_point_never_returns() {
        let s = std::sync::Arc::new(FaultSink::new(r#"{"block":["close"],"fail":[]}"#).unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        let s2 = s.clone();
        std::thread::spawn(move || {
            let _ = tx.send(s2.close());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(300))
                .is_err(),
            "close returned"
        );
        assert!(s.write(InfoKind::Redo, "{}").is_ok(), "others still work");
    }

    #[test]
    fn bad_configs_are_create_errors() {
        for (cfg, want) in [
            ("nope", "fault fixture config"),
            ("[]", "not an object"),
            (r#"{"x":[]}"#, "unknown key x"),
            (r#"{"fail":"flush"}"#, "lists only"),
            (r#"{"fail":["seal"]}"#, "unknown entry point"),
            (r#"{"sleep_ms":[]}"#, "sleep_ms map"),
            (r#"{"sleep_ms":{"write":"x"}}"#, "sleep_ms values"),
            (r#"{"sleep_ms":{"seal":1}}"#, "unknown entry point"),
        ] {
            let e = FaultSink::new(cfg).err().expect(cfg);
            assert!(e.contains(want), "{cfg}: {e}");
        }
    }
}
