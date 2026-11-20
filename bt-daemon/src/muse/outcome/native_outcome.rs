use super::{ErrorDetail, OperationStatus};
use crate::json::Field;
use serde::{Deserialize, Serialize};

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
                    .filter(|status| status.is_failure())
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
