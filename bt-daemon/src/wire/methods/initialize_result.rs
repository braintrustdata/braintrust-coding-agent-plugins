use super::Capabilities;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeResult {
    pub protocol_version: u32,
    pub daemon_version: String,
    #[serde(default)]
    pub capabilities: Capabilities,
}
