use crate::json::Extensions;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct InputSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_message_count: Option<u64>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extensions: Extensions,
}
