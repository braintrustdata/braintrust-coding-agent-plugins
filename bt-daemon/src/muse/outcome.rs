//! Native operation outcomes, including open future statuses.
use super::field::Field;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub(crate) enum OperationStatus {
    Success,
    Completed,
    ToolCalls,
    Stop,
    Failed,
    Failure,
    Error,
    Cancelled,
    Canceled,
    Interrupted,
    Timeout,
    TimedOut,
    Denied,
    Rejected,
    Other(String),
}
impl From<String> for OperationStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "success" => Self::Success,
            "completed" => Self::Completed,
            "tool_calls" => Self::ToolCalls,
            "stop" => Self::Stop,
            "failed" => Self::Failed,
            "failure" => Self::Failure,
            "error" => Self::Error,
            "cancelled" => Self::Cancelled,
            "canceled" => Self::Canceled,
            "interrupted" => Self::Interrupted,
            "timeout" => Self::Timeout,
            "timed_out" => Self::TimedOut,
            "denied" => Self::Denied,
            "rejected" => Self::Rejected,
            _ => Self::Other(s),
        }
    }
}
impl From<OperationStatus> for String {
    fn from(s: OperationStatus) -> Self {
        match s {
            OperationStatus::Success => "success",
            OperationStatus::Completed => "completed",
            OperationStatus::ToolCalls => "tool_calls",
            OperationStatus::Stop => "stop",
            OperationStatus::Failed => "failed",
            OperationStatus::Failure => "failure",
            OperationStatus::Error => "error",
            OperationStatus::Cancelled => "cancelled",
            OperationStatus::Canceled => "canceled",
            OperationStatus::Interrupted => "interrupted",
            OperationStatus::Timeout => "timeout",
            OperationStatus::TimedOut => "timed_out",
            OperationStatus::Denied => "denied",
            OperationStatus::Rejected => "rejected",
            OperationStatus::Other(s) => return s,
        }
        .into()
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct NativeOutcome {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub status: Field<OperationStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<ErrorDetail>,
}
impl NativeOutcome {
    pub fn error_message(&self) -> Option<String> {
        let error = self
            .error
            .as_ref()
            .and_then(ErrorDetail::render)
            .or_else(|| {
                self.status
                    .as_ref()
                    .filter(|s| {
                        matches!(
                            s,
                            OperationStatus::Failed
                                | OperationStatus::Failure
                                | OperationStatus::Error
                                | OperationStatus::Canceled
                                | OperationStatus::TimedOut
                                | OperationStatus::Cancelled
                                | OperationStatus::Interrupted
                                | OperationStatus::Timeout
                                | OperationStatus::Denied
                                | OperationStatus::Rejected
                        )
                    })
                    .map(|s| format!("Muse operation {}", String::from(s.clone())))
            });
        match (
            error,
            self.error_details.as_ref().and_then(ErrorDetail::render),
        ) {
            (Some(error), Some(details)) => Some(format!("{error}: {details}")),
            (Some(error), None) => Some(error),
            (None, details) => details,
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ToolOutcome {
    Success,
    Failure(String),
    Unknown,
}
