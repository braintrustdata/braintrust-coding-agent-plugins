use super::hook::Hook;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(super) struct HookEntry {
    pub(super) errors: Option<Vec<Value>>,
    pub(super) hooks: Vec<Hook>,
}
