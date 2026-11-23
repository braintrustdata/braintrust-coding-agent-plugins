use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusParams {
    /// Omit for daemon-wide status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}
