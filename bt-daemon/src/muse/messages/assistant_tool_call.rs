use super::{FunctionCall, FunctionCallType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AssistantToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: FunctionCallType,
    pub function: FunctionCall,
}
