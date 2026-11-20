use super::decode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Timestamp decoding is deliberately separate from transcript routing. A
/// future change to an unrelated routing field must not cause bounded reads to
/// consume records from after a hook's capture point.
#[derive(Deserialize, Serialize)]
struct TimestampEnvelope {
    #[serde(
        rename = "timestamp",
        default,
        deserialize_with = "deserialize_optional_timestamp"
    )]
    timestamp_ms: Option<i64>,
}

fn deserialize_optional_timestamp<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Value>::deserialize(deserializer)?
        .as_ref()
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_timestamp))
}

pub(super) fn parse_rfc3339_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

pub(super) fn parse_timestamp_ms(value: &Value) -> Option<i64> {
    decode::<TimestampEnvelope>(value).and_then(|value| value.timestamp_ms)
}
