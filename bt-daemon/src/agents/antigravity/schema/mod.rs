//! Antigravity native records shared by live translation and transcript import.
use serde::Deserialize;

mod tool_call;
mod tool_calls;
mod transcript_record;

pub(crate) use tool_call::ToolCall;
pub(crate) use tool_calls::ToolCalls;
pub(crate) use transcript_record::TranscriptRecord;

pub(crate) fn optional_integer<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Integer {
        Number(i64),
        Text(String),
    }
    Option::<Integer>::deserialize(deserializer)?
        .map(|value| match value {
            Integer::Number(value) => Ok(value),
            Integer::Text(value) => value.parse().map_err(serde::de::Error::custom),
        })
        .transpose()
}
