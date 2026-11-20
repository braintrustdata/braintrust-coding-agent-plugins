use crate::json::Extensions;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Base64ImageSource {
    #[serde(rename = "type")]
    kind: Base64Type,
    media_type: String,
    data: String,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Base64Type {
    #[serde(rename = "base64")]
    Base64,
}
