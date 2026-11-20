use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
pub(super) struct SubagentStartHook {
    pub(super) agent_id: String,
    #[serde(default)]
    pub(super) agent_type: Option<Value>,
}
