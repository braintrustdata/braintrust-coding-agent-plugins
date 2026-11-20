use crate::json::Extensions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ToolCall {
    #[serde(default, alias = "tool_name", alias = "toolName")]
    pub name: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "arguments",
        alias = "input",
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    pub args: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "result",
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    pub output: Option<Value>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
