//! Example record-transform plugin: merges the top-level fields of its JSON
//! config object into every record. An empty config leaves records unchanged;
//! a record carrying `"SZ_RT_FORCE_ERROR"` is rejected (exercises the error
//! path); one carrying `"SZ_RT_BORROW_TRIMMED"` returns a borrowed, trimmed
//! sub-slice (exercises borrowed-but-changed). A `SLEEP_MS` config key (a
//! test hook, never merged into records) makes every transform sleep that long
//! first, simulating a worker stuck in a long call. Used by the consumer's
//! tests; also a template for real plugins.

use std::borrow::Cow;
use std::time::Duration;

use serde_json::{Map, Value};
use sz_record_transform::{RecordTransform, export_record_transform};

/// Config key holding the per-record sleep, in milliseconds (test hook).
const SLEEP_MS_KEY: &str = "SLEEP_MS";

pub struct MergeFields {
    fields: Map<String, Value>,
    /// Sleep before each transform; `None` (the default) = no sleep.
    sleep: Option<Duration>,
}

impl MergeFields {
    pub fn new(config: &str) -> Result<Self, String> {
        if config.trim().is_empty() {
            return Ok(Self {
                fields: Map::new(),
                sleep: None,
            });
        }
        let mut fields = match serde_json::from_str::<Value>(config) {
            Ok(Value::Object(fields)) => fields,
            Ok(_) => return Err("config must be a JSON object".to_string()),
            Err(e) => return Err(format!("config is not valid JSON: {e}")),
        };
        let sleep = match fields.remove(SLEEP_MS_KEY) {
            None => None,
            Some(v) => Some(Duration::from_millis(v.as_u64().ok_or_else(|| {
                format!("{SLEEP_MS_KEY} must be a non-negative integer, got {v}")
            })?)),
        };
        Ok(Self { fields, sleep })
    }
}

impl RecordTransform for MergeFields {
    fn transform<'a>(&self, record: &'a str) -> Result<Cow<'a, str>, String> {
        if let Some(sleep) = self.sleep {
            std::thread::sleep(sleep);
        }
        let mut v: Value =
            serde_json::from_str(record).map_err(|e| format!("record is not valid JSON: {e}"))?;
        let obj = v
            .as_object_mut()
            .ok_or_else(|| "record is not a JSON object".to_string())?;
        if obj.contains_key("SZ_RT_FORCE_ERROR") {
            return Err("forced error".to_string());
        }
        if obj.contains_key("SZ_RT_BORROW_TRIMMED") {
            // A borrowed view that differs from the input: must be loaded.
            return Ok(Cow::Borrowed(record.trim()));
        }
        if self.fields.is_empty() {
            return Ok(Cow::Borrowed(record));
        }
        for (k, val) in &self.fields {
            obj.insert(k.clone(), val.clone());
        }
        Ok(Cow::Owned(v.to_string()))
    }
}

export_record_transform!(MergeFields, MergeFields::new);

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    const RECORD: &str = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"1"}"#;

    #[test]
    fn default_config_does_not_sleep_and_leaves_record_unchanged() {
        let t = MergeFields::new("").unwrap();
        assert_eq!(t.sleep, None);
        assert!(matches!(t.transform(RECORD), Ok(Cow::Borrowed(RECORD))));
    }

    #[test]
    fn sleep_ms_sleeps_and_is_not_merged_into_the_record() {
        let t = MergeFields::new(r#"{"SLEEP_MS":50}"#).unwrap();
        assert_eq!(t.sleep, Some(Duration::from_millis(50)));
        let start = Instant::now();
        let out = t.transform(RECORD).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert!(matches!(out, Cow::Borrowed(RECORD)), "{out}");
    }

    #[test]
    fn sleep_ms_must_be_a_non_negative_integer() {
        assert!(MergeFields::new(r#"{"SLEEP_MS":"x"}"#).is_err());
        assert!(MergeFields::new(r#"{"SLEEP_MS":-1}"#).is_err());
    }
}
