use super::parse_timestamp_ms;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable routing fields from one Claude transcript row. Message and tool
/// blocks intentionally remain raw because they are preserved in span I/O.
#[derive(Deserialize, Serialize)]
pub(super) struct TranscriptEnvelope {
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) subtype: Option<String>,
    pub(super) operation: Option<QueueOperation>,
    #[serde(rename = "isCompactSummary", default)]
    pub(super) is_compact_summary: bool,
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
