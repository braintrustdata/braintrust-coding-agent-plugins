use serde::{Deserialize, Serialize};

/// Stable routing fields from one Claude transcript row. Message and tool
/// blocks intentionally remain raw because they are preserved in span I/O.
#[derive(Deserialize, Serialize)]
pub(super) struct TranscriptEnvelope {
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) subtype: Option<String>,
    #[serde(rename = "isCompactSummary", default)]
    pub(super) is_compact_summary: bool,
}
