use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ErrorDetail {
    Text(String),
    Structured(Value),
}

impl ErrorDetail {
    pub fn render(&self) -> Option<String> {
        match self {
            Self::Text(s) => (!s.is_empty()).then(|| s.clone()),
            Self::Structured(Value::Null) => None,
            Self::Structured(value) => Some(value.to_string()),
        }
    }
}
