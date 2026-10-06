//! Field deserializers for native hook payloads.
//!
//! Agents do not version their hook schemas, so a field that has an
//! unexpected JSON type is treated as absent instead of rejecting the whole
//! event. Use these with `#[serde(default, deserialize_with = "...")]`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// A string, or `None` for any other JSON type.
pub(crate) fn string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(match Value::deserialize(d)? {
        Value::String(s) => Some(s),
        _ => None,
    })
}

/// An unsigned integer, or `None` for any other JSON type.
pub(crate) fn u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    Ok(Value::deserialize(d)?.as_u64())
}

/// Any JSON number, or `None` for any other JSON type.
pub(crate) fn f64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    Ok(Value::deserialize(d)?.as_f64())
}

/// A boolean, or `None` for any other JSON type.
pub(crate) fn bool<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    Ok(Value::deserialize(d)?.as_bool())
}

/// The first element of an array when it is a string.
pub(crate) fn first_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(Value::deserialize(d)?
        .get(0)
        .and_then(Value::as_str)
        .map(str::to_owned))
}

/// A nested object, or `None` when it does not have the expected shape.
pub(crate) fn nested<'de, D: Deserializer<'de>, T: DeserializeOwned>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Ok(serde_json::from_value(Value::deserialize(d)?).ok())
}

/// An opaque native value that is forwarded verbatim. Unlike a plain
/// `Option<Value>`, an explicit `null` is preserved as `Some(Value::Null)`, so
/// "present but null" stays distinct from "absent".
pub(crate) fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

/// Parse a payload into its typed form. A payload that is not a JSON object
/// yields the default (all fields absent), matching field-by-field lookups.
pub(crate) fn parse<T: DeserializeOwned + Default>(payload: &Value) -> T {
    T::deserialize(payload).unwrap_or_default()
}
