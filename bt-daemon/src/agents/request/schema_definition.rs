use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub(super) struct SchemaDefinition {
    name: String,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    description: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    strict: Field<bool>,
    /// Provider/user-authored JSON Schema, intentionally unrestricted.
    schema: Value,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
