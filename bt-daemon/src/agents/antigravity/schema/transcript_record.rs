use super::{optional_integer, ToolCalls};
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

impl TranscriptRecord {
    pub fn normalized_kind(&self) -> &str {
        self.kind
            .strip_prefix("CORTEX_STEP_TYPE_")
            .unwrap_or(&self.kind)
    }

    /// Native formats use three alternative content fields. Explicit null is
    /// still content and must not accidentally expose a lower-priority field.
    pub fn message_content(&self) -> Value {
        self.content
            .as_ref()
            .or(self.message.as_ref())
            .or(self.text.as_ref())
            .cloned()
            .unwrap_or(Value::Null)
    }

    pub fn created_at_ms(&self) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(self.created_at.as_deref()?)
            .ok()
            .map(|timestamp| timestamp.timestamp_millis())
    }
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
