//! One model-visible history message and the native rows contributing to it.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub(super) struct HistoryMessage {
    value: Value,
    source_uuids: Vec<String>,
}

impl HistoryMessage {
    pub(super) fn new(value: Value, source_uuid: Option<String>) -> Self {
        Self {
            value,
            source_uuids: source_uuid.into_iter().collect(),
        }
    }
    pub(super) fn value(&self) -> &Value {
        &self.value
    }
    pub(super) fn update(&mut self, value: Value, source_uuid: Option<String>) {
        self.value = value;
        if let Some(source_uuid) = source_uuid {
            if !self.source_uuids.contains(&source_uuid) {
                self.source_uuids.push(source_uuid);
            }
        }
    }
    pub(super) fn is_preserved_by(&self, source_uuids: &[String]) -> bool {
        self.source_uuids
            .iter()
            .any(|uuid| source_uuids.contains(uuid))
    }
}
