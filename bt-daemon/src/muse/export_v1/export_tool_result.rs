use crate::muse::hooks::ToolOutput;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct ExportToolResult {
    pub tool_call_id: String,
    pub text: Option<ToolOutput>,
}
