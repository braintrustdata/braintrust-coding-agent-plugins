use super::FunctionTool;
use crate::json::Extensions;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum ToolDefinition {
    Function(FunctionTool),
    BuiltIn(Extensions),
}

impl<'de> Deserialize<'de> for ToolDefinition {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if value.get("type").and_then(Value::as_str) == Some("function") {
            serde_json::from_value(value)
                .map(Self::Function)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_value(value)
                .map(Self::BuiltIn)
                .map_err(serde::de::Error::custom)
        }
    }
}
