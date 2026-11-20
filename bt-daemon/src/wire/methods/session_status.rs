use super::RecoveryInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStatus {
    pub session_id: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<crate::wire::SessionRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryInfo>,
    pub queued: u64,
    /// Canonical span operations not yet acknowledged for this route. Distinct
    /// from native events still awaiting translation and from unique spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_span_ops: Option<u64>,
    pub spans_emitted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permalink: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}
