use crate::json::Extensions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub(super) struct FunctionDefinition {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Tool-authored JSON Schema, intentionally unrestricted.
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
