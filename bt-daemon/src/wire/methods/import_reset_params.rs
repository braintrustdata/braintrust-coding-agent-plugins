use serde::{Deserialize, Serialize};

/// Restart translation for a source session whose transcript was rewritten.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResetParams {
    pub source: String,
    pub session_id: String,
}
