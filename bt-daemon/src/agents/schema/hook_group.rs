use super::CommandHook;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HookGroup {
    pub hooks: Vec<CommandHook>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
