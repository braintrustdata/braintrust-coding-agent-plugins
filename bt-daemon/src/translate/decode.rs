//! Decoding native payloads into typed structs.
//!
//! A field with an unexpected JSON type is an error, never silently dropped.
//! It usually means the agent changed its format and the translator needs an
//! update, so [`DecodeError`] keeps these failures distinguishable from other
//! translation errors.

use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fmt;

/// A native payload that does not have the shape its translator expects.
#[derive(Debug)]
pub(crate) struct DecodeError {
    /// The agent source whose payload failed to decode.
    pub source: &'static str,
    /// What was being decoded, such as an event name.
    pub what: String,
    pub error: serde_json::Error,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unexpected {} {} format: {}",
            self.source, self.what, self.error
        )
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Decode `value`, attributing a failure to `source` and `what`.
pub(crate) fn decode<T: DeserializeOwned>(
    source: &'static str,
    what: impl fmt::Display,
    value: &Value,
) -> Result<T, DecodeError> {
    T::deserialize(value).map_err(|error| DecodeError {
        source,
        what: what.to_string(),
        error,
    })
}
