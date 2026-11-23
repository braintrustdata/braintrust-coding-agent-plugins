use serde::{Deserialize, Serialize};

/// A zero-sized marker that serializes to the string `"2.0"` and refuses any
/// other value, so the `jsonrpc` field is validated for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonRpcV2;

impl Serialize for JsonRpcV2 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for JsonRpcV2 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = String::deserialize(d)?;
        if v == "2.0" {
            Ok(JsonRpcV2)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported jsonrpc version {v:?}"
            )))
        }
    }
}
