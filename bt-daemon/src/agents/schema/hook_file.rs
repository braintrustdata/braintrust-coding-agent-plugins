use crate::json::Field;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HookFile<Group> {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub version: Field<u32>,
    pub hooks: BTreeMap<String, Vec<Group>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
