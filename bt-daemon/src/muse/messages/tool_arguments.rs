use super::ToolInput;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ToolArguments {
    Encoded(String),
    Json(Value),
}

impl ToolArguments {
    pub fn input(&self) -> ToolInput {
        ToolInput(match self {
            Self::Encoded(s) => {
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
            }
            Self::Json(v) => v.clone(),
        })
    }
}
