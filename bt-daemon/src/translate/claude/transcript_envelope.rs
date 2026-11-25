use super::native_field::deserialize_optional_string;
use crate::json::Field;
use serde::Deserialize;
use serde_json::Value;

/// Stable routing fields from one Claude transcript row.
#[derive(Deserialize)]
pub(super) struct TranscriptEnvelope {
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) subtype: Option<String>,
    #[serde(rename = "isCompactSummary", default)]
    pub(super) is_compact_summary: bool,
    message: Option<TranscriptMessage>,
    // Claude stores tool-specific data here, such as a Bash or Agent result.
    #[serde(rename = "toolUseResult")]
    pub(super) tool_use_result: Option<Value>,
}

#[derive(Deserialize)]
struct TranscriptMessage {
    content: Option<TranscriptContent>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TranscriptContent {
    Blocks(Vec<TranscriptBlock>),
    Other(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct TranscriptBlock {
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(flatten)]
    result: TranscriptToolResult,
}

#[derive(Deserialize)]
pub(super) struct TranscriptToolResult {
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) tool_use_id: Option<String>,
    // The tool determines the shape of its result.
    #[serde(default)]
    content: Field<Value>,
    is_error: Option<bool>,
    #[serde(rename = "isError")]
    camel_case_is_error: Option<bool>,
}

impl TranscriptEnvelope {
    pub(super) fn tool_results(&self) -> impl Iterator<Item = &TranscriptToolResult> {
        let blocks = match self
            .message
            .as_ref()
            .and_then(|message| message.content.as_ref())
        {
            Some(TranscriptContent::Blocks(blocks)) => blocks.as_slice(),
            _ => &[],
        };
        blocks.iter().filter_map(|block| {
            (block.kind.as_deref() == Some("tool_result")).then_some(&block.result)
        })
    }
}

impl TranscriptToolResult {
    pub(super) fn content(&self) -> Option<&Value> {
        match &self.content {
            Field::Missing => None,
            Field::Null => Some(&Value::Null),
            Field::Value(value) => Some(value),
        }
    }

    pub(super) fn signals_error(&self) -> bool {
        self.is_error == Some(true) || self.camel_case_is_error == Some(true)
    }
}
