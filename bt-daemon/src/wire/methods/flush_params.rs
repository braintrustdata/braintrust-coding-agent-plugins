use super::default_flush_timeout_ms;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushParams {
    pub session_id: String,
    #[serde(default = "default_flush_timeout_ms")]
    pub timeout_ms: u64,
}
