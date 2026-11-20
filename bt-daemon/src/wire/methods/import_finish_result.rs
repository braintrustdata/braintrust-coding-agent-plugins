use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportFinishResult {
    pub flushed: bool,
    pub pending: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_span_id: Option<String>,
    /// Distinct spans this route has delivered since its actor started.
    pub span_count: u64,
}
