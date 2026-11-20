use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The stable outer shape of a rollout JSONL row. The payload deliberately
/// stays untyped here: Codex adds response-item fields regularly and several
/// handlers preserve those native objects in span input/output.
#[derive(Deserialize, Serialize)]
pub(super) struct RolloutRecord {
    #[serde(
        rename = "timestamp",
        default,
        deserialize_with = "deserialize_timestamp"
    )]
    pub(super) timestamp_ms: Option<i64>,
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) payload: Value,
}

impl RolloutRecord {
    /// Only metadata and turn-context rows establish this scope's directory.
    pub(super) fn working_directory(&self) -> Option<String> {
        if matches!(self.kind.as_deref(), Some("session_meta" | "turn_context")) {
            super::decode::<super::working_directory_payload::WorkingDirectoryPayload>(
                &self.payload,
            )
            .and_then(|payload| payload.cwd)
        } else {
            None
        }
    }

    /// Event and response rows carry a second native discriminator. A malformed
    /// or missing discriminator remains an unhandled event, as with future kinds.
    pub(super) fn payload_kind(&self) -> String {
        super::decode::<super::payload_kind::PayloadKind>(&self.payload)
            .and_then(|payload| payload.kind)
            .unwrap_or_default()
    }
}
pub(super) fn parse_ts(rec: &Value) -> Option<i64> {
    rec.get("timestamp").and_then(normalize_timestamp)
}

/// Deserialize Codex rollout timestamps into Unix milliseconds. Rollouts have
/// historically used RFC 3339 strings, but imported or newer producers may
/// supply numeric epoch seconds, milliseconds, microseconds, or nanoseconds.
/// Unknown values remain absent so the caller can use the hook timestamp.
fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Value>::deserialize(deserializer)?
        .as_ref()
        .and_then(normalize_timestamp))
}

fn normalize_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::String(value) => {
            parse_rfc3339_timestamp(value).or_else(|| normalize_numeric_timestamp(value))
        }
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                normalize_epoch_integer(value)
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value).ok().and_then(normalize_epoch_integer)
            } else {
                value.as_f64().and_then(normalize_epoch_float)
            }
        }
        _ => None,
    }
}

fn normalize_numeric_timestamp(value: &str) -> Option<i64> {
    value
        .parse::<i64>()
        .ok()
        .and_then(normalize_epoch_integer)
        .or_else(|| value.parse::<f64>().ok().and_then(normalize_epoch_float))
}

fn normalize_epoch_integer(value: i64) -> Option<i64> {
    let magnitude = value.unsigned_abs();
    if magnitude < 100_000_000_000 {
        value.checked_mul(1_000)
    } else if magnitude < 100_000_000_000_000 {
        Some(value)
    } else if magnitude < 100_000_000_000_000_000 {
        Some(value / 1_000)
    } else {
        Some(value / 1_000_000)
    }
}

fn normalize_epoch_float(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let magnitude = value.abs();
    let milliseconds = if magnitude < 100_000_000_000.0 {
        value * 1_000.0
    } else if magnitude < 100_000_000_000_000.0 {
        value
    } else if magnitude < 100_000_000_000_000_000.0 {
        value / 1_000.0
    } else {
        value / 1_000_000.0
    };
    if milliseconds < i64::MIN as f64 || milliseconds > i64::MAX as f64 {
        None
    } else {
        Some(milliseconds.trunc() as i64)
    }
}

fn parse_rfc3339_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}
#[cfg(test)]
mod tests {
    use super::{normalize_timestamp, RolloutRecord};
    use serde_json::json;
    #[test]
    fn rollout_timestamps_normalize_supported_formats() {
        let milliseconds = 1_704_067_202_000_i64;
        assert_eq!(
            normalize_timestamp(&json!(1_704_067_202_i64)),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!(milliseconds)),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!(1_704_067_202_000_000_i64)),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!(1_704_067_202_000_000_000_i64)),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!("2024-01-01T00:00:02Z")),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!("1704067202000")),
            Some(milliseconds)
        );
        assert_eq!(
            normalize_timestamp(&json!(1_704_067_202.25_f64)),
            Some(milliseconds + 250)
        );
        assert_eq!(normalize_timestamp(&json!({ "seconds": 1 })), None);
    }

    #[test]
    fn rollout_record_treats_unknown_timestamp_formats_as_missing() {
        let record: RolloutRecord = serde_json::from_value(json!({
            "timestamp": { "future": "format" },
            "type": "event_msg",
            "payload": { "type": "task_started" },
        }))
        .unwrap();
        assert_eq!(record.timestamp_ms, None);
    }
}
