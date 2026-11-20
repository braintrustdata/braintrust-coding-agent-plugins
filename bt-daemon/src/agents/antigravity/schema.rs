//! Antigravity native records shared by live translation and transcript import.
use crate::agents::usage::ProviderUsage;
use crate::json::Extensions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Native transcript records are decoded before entering translator state.
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct TranscriptRecord {
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub source: String,
    #[serde(default, alias = "stepIndex", deserialize_with = "optional_integer")]
    pub step_index: Option<i64>,
    #[serde(default, alias = "createdAt")]
    pub created_at: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    pub content: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    pub message: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    pub text: Option<Value>,
    #[serde(default, alias = "toolCalls")]
    pub tool_calls: Option<ToolCalls>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ProviderUsage>,
    /// Unknown native extensions are retained without inspecting their contents.
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}

pub(crate) fn optional_integer<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Integer {
        Number(i64),
        Text(String),
    }
    Option::<Integer>::deserialize(deserializer)?
        .map(|value| match value {
            Integer::Number(value) => Ok(value),
            Integer::Text(value) => value.parse().map_err(serde::de::Error::custom),
        })
        .transpose()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ToolCalls {
    Many(Vec<ToolCall>),
    One(ToolCall),
}
impl ToolCalls {
    pub fn iter(&self) -> std::slice::Iter<'_, ToolCall> {
        match self {
            Self::Many(calls) => calls.iter(),
            Self::One(call) => std::slice::from_ref(call).iter(),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn persisted_records_keep_missing_content_distinct_from_null() {
        for raw in [
            json!({"message":"fallback"}),
            json!({"content":null,"message":"fallback"}),
        ] {
            let record: TranscriptRecord = serde_json::from_value(raw.clone()).unwrap();
            let restored: TranscriptRecord =
                serde_json::from_value(serde_json::to_value(&record).unwrap()).unwrap();
            assert_eq!(restored.content, record.content);
            assert_eq!(restored.message, record.message);
            assert_eq!(
                restored.content.is_some(),
                raw.as_object().unwrap().contains_key("content")
            );
        }
    }
}
