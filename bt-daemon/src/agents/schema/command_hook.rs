use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandHook {
    #[serde(rename = "type")]
    pub kind: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_windows: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(rename = "async", skip_serializing_if = "Option::is_none")]
    pub asynchronous: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
