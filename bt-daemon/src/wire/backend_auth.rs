use serde::{Deserialize, Serialize};

/// Backend credentials. `token` is an API key or an OAuth access token; the
/// daemon does not care which. Never serialized or persisted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackendAuth {
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    /// Optional org id. `bt` may or may not know it; the SDK's project
    /// registration works from `org_name` alone, so this is best-effort and
    /// only feeds the SDK's per-session credential/batch key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
}
