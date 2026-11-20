use super::SessionStatus;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    pub daemon_version: String,
    pub uptime_ms: u64,
    pub sessions: Vec<SessionStatus>,
}
