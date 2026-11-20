use super::CursorHookEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Unrecognized native hook definitions belong to the user, and survive edits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum CursorHookEntries {
    Entries(Vec<CursorHookEntry>),
    Other(Value),
}
