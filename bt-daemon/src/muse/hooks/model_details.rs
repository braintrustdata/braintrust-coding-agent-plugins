use super::ProviderOptions;
use crate::json::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelDetails {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub step: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub attempt: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub message_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub tool_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub options: Field<ProviderOptions>,
}
