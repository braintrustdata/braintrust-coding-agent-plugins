use super::{Base64ImageSource, UrlImageSource};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ImageSource {
    Base64(Base64ImageSource),
    Url(UrlImageSource),
    /// Future source formats survive unchanged, without becoming known data.
    Other(Box<serde_json::value::RawValue>),
}

impl<'de> Deserialize<'de> for ImageSource {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("image source must be an object"));
        }
        match value.get("type").and_then(Value::as_str) {
            Some("base64") => serde_json::from_value(value)
                .map(Self::Base64)
                .map_err(serde::de::Error::custom),
            kind @ (None | Some("url")) if kind.is_some() || value.get("url").is_some() => {
                serde_json::from_value(value)
                    .map(Self::Url)
                    .map_err(serde::de::Error::custom)
            }
            _ => serde_json::value::to_raw_value(&value)
                .map(Self::Other)
                .map_err(serde::de::Error::custom),
        }
    }
}
