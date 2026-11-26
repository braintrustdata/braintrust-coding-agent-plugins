//! Interpret native tool completion without treating cancellation as failure.
use super::transcript_envelope::TranscriptToolResult;
use crate::translate::tool::nonempty_error_text;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub(super) enum TranscriptToolOutcome {
    Succeeded,
    Failed(String),
    Denied,
    Cancelled,
}

impl TranscriptToolOutcome {
    pub(super) fn from_result(
        result: &TranscriptToolResult,
        native_result: Option<&Value>,
    ) -> Self {
        let values = [result.content(), native_result];
        let values = || values.iter().copied().flatten();

        let error_signalled = result.signals_error() || values().any(has_structured_error);
        let structured_denial = values()
            .any(|value| has_structured_outcome(value, &["denied", "deny", "rejected", "reject"]));
        let denial_text = values().any(is_permission_denial);
        if structured_denial || (error_signalled && denial_text) {
            return TranscriptToolOutcome::Denied;
        }

        let structured_cancellation = values().any(|value| {
            has_structured_outcome(value, &["cancelled", "canceled", "aborted", "interrupted"])
                || has_structured_cancellation(value)
        });
        let cancellation_text = values().any(|value| {
            has_outcome_phrase(
                value,
                &[
                    "request interrupted by user",
                    "tool use was cancelled",
                    "tool use was canceled",
                    "operation was cancelled",
                    "operation was canceled",
                    "operation was aborted",
                    "user cancelled",
                    "user canceled",
                ],
            )
        });
        if structured_cancellation || (error_signalled && cancellation_text) {
            return TranscriptToolOutcome::Cancelled;
        }

        if error_signalled {
            let error = values()
                .find_map(nonempty_error_text)
                .unwrap_or_else(|| "Tool execution failed".into());
            TranscriptToolOutcome::Failed(error)
        } else {
            TranscriptToolOutcome::Succeeded
        }
    }
}

fn has_structured_outcome(value: &Value, outcomes: &[&str]) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    ["status", "outcome", "type"]
        .into_iter()
        .filter_map(|key| object.get(key).and_then(Value::as_str))
        .any(|status| outcomes.contains(&status.trim().to_ascii_lowercase().as_str()))
}

fn has_structured_error(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.get("is_error").and_then(Value::as_bool) == Some(true)
            || object.get("isError").and_then(Value::as_bool) == Some(true)
            || object.get("error").is_some_and(|error| {
                error.as_bool() == Some(true) || nonempty_error_text(error).is_some()
            })
            || has_structured_outcome(value, &["error", "failed", "failure"])
    })
}

fn has_structured_cancellation(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        ["interrupted", "cancelled", "canceled", "aborted"]
            .into_iter()
            .any(|key| object.get(key).and_then(Value::as_bool) == Some(true))
    })
}

fn has_outcome_phrase(value: &Value, phrases: &[&str]) -> bool {
    match value {
        Value::String(text) => {
            let text = text.to_ascii_lowercase();
            phrases.iter().any(|phrase| text.contains(phrase))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| has_outcome_phrase(value, phrases)),
        Value::Object(object) => object
            .values()
            .any(|value| has_outcome_phrase(value, phrases)),
        _ => false,
    }
}

pub(super) fn is_permission_denial(value: &Value) -> bool {
    (has_outcome_phrase(value, &["permission to use"])
        && has_outcome_phrase(value, &["has been denied"]))
        || has_outcome_phrase(
            value,
            &[
                "tool use was rejected",
                "tool use was denied",
                "user rejected",
                "user denied",
                "user doesn't want to proceed",
                "user does not want to proceed",
                "permission request was rejected",
                "permission request was denied",
                "but you haven't granted it yet",
                "denied by the claude code auto mode classifier",
                "denied by a built-in claude code safety check",
            ],
        )
}
