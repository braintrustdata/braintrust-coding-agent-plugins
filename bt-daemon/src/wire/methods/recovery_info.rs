use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryInfo {
    /// Retry attempts already made for this held delivery.
    #[serde(default)]
    pub attempt: u32,
    pub stage: String,
    pub position: u64,
    pub reason: String,
    pub retry: String,
    pub eligible: bool,
}
