//! Compatibility decoding for native Claude hook scalar fields.
use serde::Deserialize;
use serde_json::Value;

pub(super) fn deserialize_optional_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Value>::deserialize(deserializer)?
        .as_ref()
        .and_then(value_as_nonempty_string))
}

/// Claude hooks report effort as `{"level": "high"}`.
pub(super) fn deserialize_optional_effort<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Value>::deserialize(deserializer)?
        .as_ref()
        .and_then(|effort| effort.get("level"))
        .and_then(value_as_nonempty_string))
}

pub(super) fn deserialize_nonempty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    value_as_nonempty_string(&value)
        .ok_or_else(|| serde::de::Error::custom("expected a non-empty string or number"))
}

fn value_as_nonempty_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
