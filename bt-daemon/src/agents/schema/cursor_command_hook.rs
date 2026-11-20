use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CursorCommandHook {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_closed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
impl CursorCommandHook {
    pub fn capture(command: String, matcher: Option<String>) -> Self {
        Self {
            command,
            timeout: Some(10),
            fail_closed: Some(false),
            matcher,
            extra: Map::new(),
        }
    }
}
