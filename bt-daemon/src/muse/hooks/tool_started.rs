use super::{optional_id, ToolInput};
use crate::muse::ids::ToolCallId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolStarted {
    #[serde(default, deserialize_with = "optional_id")]
    pub tool_use_id: Option<ToolCallId>,
    pub tool_name: Option<String>,
    pub tool_input: Option<ToolInput>,
}
