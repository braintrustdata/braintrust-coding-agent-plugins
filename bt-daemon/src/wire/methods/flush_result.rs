use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushResult {
    pub flushed: bool,
    pub pending: u64,
    #[serde(default)]
    pub accepted_sessions: u64,
}
