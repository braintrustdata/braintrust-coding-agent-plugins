use super::ModelDetails;
use super::{InputSummary, NativeMessage, ToolDescription};
use crate::json::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelStarted {
    #[serde(flatten)]
    pub details: ModelDetails,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub request_record_id: Field<String>,
    pub messages: Option<Vec<NativeMessage>>,
    pub tools: Option<Vec<ToolDescription>>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub input_summary: Field<InputSummary>,
}
