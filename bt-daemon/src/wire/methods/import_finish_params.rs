use super::default_flush_timeout_ms;
use serde::{Deserialize, Serialize};

/// Deliver one imported source session's captured events through its route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportFinishParams {
    pub source: String,
    pub session_id: String,
    pub route: crate::wire::SessionRoute,
    /// Close the session's open spans, as a completed transcript does.
    /// Otherwise only flush, leaving it open for later capture.
    pub finalize: bool,
    #[serde(default = "default_flush_timeout_ms")]
    pub timeout_ms: u64,
}
