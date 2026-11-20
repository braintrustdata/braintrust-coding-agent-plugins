use crate::muse::hooks::ToolArguments;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct ExportToolCall {
    pub call_id: String,
    pub name: String,
    pub args: Option<ToolArguments>,
}
