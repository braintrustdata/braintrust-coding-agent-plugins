use super::{Notification, Request, Response};
use serde::{Deserialize, Serialize};

/// A single JSON-RPC frame. Untagged so one type round-trips a request, a
/// response, or a notification; disambiguated by which fields are present.
///
/// Note: `Response` must come before `Notification` in the enum. A response
/// carries `id` but no `method`; a notification carries `method` but no `id`;
/// a request carries both. Serde's untagged matching tries variants in order,
/// so ordering here plus `deny_unknown_fields`-free structs keeps them
/// unambiguous.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

impl Message {
    /// Parse one newline-delimited frame.
    pub fn from_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }

    /// Serialize to a single line (no trailing newline).
    pub fn to_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}
