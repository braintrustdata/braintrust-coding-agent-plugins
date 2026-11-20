use super::SchemaDefinition;
use crate::json::Extensions;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum ResponseFormat {
    Known(KnownResponseFormat),
    Other(Box<RawValue>),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum KnownResponseFormat {
    Text {
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    JsonObject {
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    JsonSchema {
        json_schema: SchemaDefinition,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}

impl<'de> Deserialize<'de> for ResponseFormat {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if matches!(
            value.get("type").and_then(Value::as_str),
            Some("text" | "json_object" | "json_schema")
        ) {
            serde_json::from_value(value)
                .map(Self::Known)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::value::to_raw_value(&value)
                .map(Self::Other)
                .map_err(serde::de::Error::custom)
        }
    }
}
