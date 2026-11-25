use super::native_field::deserialize_optional_string;
use super::parse_timestamp_ms;
use crate::json::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable routing fields from one Claude transcript row.
#[derive(Deserialize)]
pub(super) struct TranscriptEnvelope {
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) subtype: Option<String>,
    pub(super) operation: Option<QueueOperation>,
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

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum QueueOperation {
    Enqueue,
    #[serde(other)]
    Other,
}

pub(crate) enum CliForkHistory {
    Absent,
    AwaitingPrompt,
    EndsAt(usize),
}

/// CLI forks stamp copied records with their own session ID. Claude queues the
/// new prompt before writing that copied history, retaining its old timestamps.
pub(crate) fn cli_fork_history(records: &[Value]) -> CliForkHistory {
    let mut queued_at = None;
    let mut copied_assistant = false;
    for (index, record) in records.iter().enumerate() {
        let Ok(row) = TranscriptEnvelope::deserialize(record) else {
            continue;
        };
        match row.kind.as_deref() {
            Some("queue-operation") if matches!(row.operation, Some(QueueOperation::Enqueue)) => {
                if queued_at.is_none() {
                    queued_at = parse_timestamp_ms(record);
                }
            }
            Some("user") if matches!((queued_at, parse_timestamp_ms(record)), (Some(queue), Some(user)) if user >= queue) =>
            {
                return if copied_assistant {
                    CliForkHistory::EndsAt(index)
                } else {
                    CliForkHistory::Absent
                };
            }
            Some("user") if queued_at.is_none() || parse_timestamp_ms(record).is_none() => {
                return CliForkHistory::Absent;
            }
            Some("assistant") => {
                let (Some(queue), Some(assistant)) = (queued_at, parse_timestamp_ms(record)) else {
                    return CliForkHistory::Absent;
                };
                if assistant >= queue {
                    return CliForkHistory::Absent;
                }
                copied_assistant = true;
            }
            _ => {}
        }
    }
    if queued_at.is_some() {
        CliForkHistory::AwaitingPrompt
    } else {
        CliForkHistory::Absent
    }
}
