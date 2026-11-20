use super::ToolArguments;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FunctionCall {
    pub name: String,
    pub arguments: ToolArguments,
}
