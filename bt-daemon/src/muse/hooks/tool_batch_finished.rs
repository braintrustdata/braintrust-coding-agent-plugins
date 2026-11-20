use super::ToolFinished;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolBatchFinished {
    #[serde(default)]
    pub tool_calls: Vec<ToolFinished>,
}
