use serde::{Deserialize, Serialize};

/// The daemon's own resolution of a route's credentials. Never carries the
/// credential itself. Resolution succeeded when `error` is absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthDiagnoseResult {
    /// The canonical selection the daemon resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<crate::wire::AuthSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
