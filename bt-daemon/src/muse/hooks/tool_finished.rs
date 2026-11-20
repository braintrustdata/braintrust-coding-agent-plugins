use super::ToolStarted;
use super::{NativeOutcome, ToolOutput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolFinished {
    #[serde(flatten)]
    pub tool: ToolStarted,
    pub tool_response: Option<ToolOutput>,
    pub duration_ms: Option<u64>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
