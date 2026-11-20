use super::{ErrorDetail, NativeOutcome};
use crate::json::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SessionFinished {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub reason: Field<ErrorDetail>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
