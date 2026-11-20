use serde::{Deserialize, Serialize};

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

impl OperationStatus {
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::Failed
                | Self::Failure
                | Self::Error
                | Self::Canceled
                | Self::TimedOut
                | Self::Cancelled
                | Self::Interrupted
                | Self::Timeout
                | Self::Denied
                | Self::Rejected
        )
    }
}
