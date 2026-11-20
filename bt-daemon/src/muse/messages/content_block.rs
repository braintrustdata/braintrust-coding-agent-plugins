use super::KnownContentBlock;
use crate::json::Extensions;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Known content variants are validated before the unknown-content fallback.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ContentBlock {
    Known(KnownContentBlock),
    Other {
        #[serde(rename = "type")]
        kind: String,
        #[serde(flatten)]
        fields: Extensions,
    },
}

impl<'de> Deserialize<'de> for ContentBlock {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Tag {
            #[serde(rename = "type")]
            kind: String,
        }
        let value = Value::deserialize(d)?;
        let tag: Tag = serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
        if matches!(
            tag.kind.as_str(),
            "text" | "assistant_message" | "image" | "image_url"
        ) {
            serde_json::from_value(value)
                .map(Self::Known)
                .map_err(serde::de::Error::custom)
        } else {
            let mut fields: Extensions =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            fields.remove("type");
            Ok(Self::Other {
                kind: tag.kind,
                fields,
            })
        }
    }
}
