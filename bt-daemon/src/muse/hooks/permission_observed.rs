use super::ToolInput;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PermissionObserved {
    pub tool_name: Option<String>,
    pub tool_input: Option<ToolInput>,
}
