//! Example record-transform plugin: merges the top-level fields of its JSON
//! config object into every record. An empty config leaves records unchanged;
//! a record carrying `"SZ_RT_FORCE_ERROR"` is rejected (exercises the error
//! path); one carrying `"SZ_RT_BORROW_TRIMMED"` returns a borrowed, trimmed
//! sub-slice (exercises borrowed-but-changed). Used by the consumer's tests;
//! also a template for real plugins.

use std::borrow::Cow;

use serde_json::{Map, Value};
use sz_record_transform::{RecordTransform, export_record_transform};

pub struct MergeFields {
    fields: Map<String, Value>,
}

impl MergeFields {
    pub fn new(config: &str) -> Result<Self, String> {
        if config.trim().is_empty() {
            return Ok(Self { fields: Map::new() });
        }
        match serde_json::from_str::<Value>(config) {
            Ok(Value::Object(fields)) => Ok(Self { fields }),
            Ok(_) => Err("config must be a JSON object".to_string()),
            Err(e) => Err(format!("config is not valid JSON: {e}")),
        }
    }
}

impl RecordTransform for MergeFields {
    fn transform<'a>(&self, record: &'a str) -> Result<Cow<'a, str>, String> {
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
