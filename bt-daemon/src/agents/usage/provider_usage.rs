use super::{CamelCaseUsage, SnakeCaseUsage, UsageMetrics};
use crate::json::Extensions;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ProviderUsage {
    SnakeCase(Box<SnakeCaseUsage>),
    CamelCase(Box<CamelCaseUsage>),
    Unknown(Extensions),
}

impl<'de> Deserialize<'de> for ProviderUsage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Inspect field names only at the decoding boundary. Optional-only
        // untagged structs would otherwise accept and swallow other variants.
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let fields: BTreeMap<String, serde::de::IgnoredAny> =
            serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        let snake = SNAKE_FIELDS.iter().any(|field| fields.contains_key(*field));
        let camel = CAMEL_FIELDS.iter().any(|field| fields.contains_key(*field));
        match (snake, camel) {
            (true, false) => serde_json::from_str(raw.get())
                .map(Self::SnakeCase)
                .map_err(serde::de::Error::custom),
            (false, true) => serde_json::from_str(raw.get())
                .map(Self::CamelCase)
                .map_err(serde::de::Error::custom),
            (false, false) => serde_json::from_str(raw.get())
                .map(Self::Unknown)
                .map_err(serde::de::Error::custom),
            (true, true) => Err(serde::de::Error::custom(
                "mixed snake_case and camelCase usage fields",
            )),
        }
    }
}
impl ProviderUsage {
    pub fn metrics(&self) -> UsageMetrics {
        match self {
            Self::SnakeCase(usage) => usage.metrics(),
            Self::CamelCase(usage) => usage.metrics(),
            Self::Unknown(_) => UsageMetrics::default(),
        }
    }
}

const SNAKE_FIELDS: &[&str] = &[
    "input_tokens",
    "prompt_tokens",
    "output_tokens",
    "completion_tokens",
    "total_tokens",
    "tokens",
    "cached_input_tokens",
    "prompt_cached_tokens",
    "cache_read_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "prompt_cache_creation_tokens",
    "reasoning_output_tokens",
    "completion_reasoning_tokens",
    "reasoning_tokens",
    "thinking_tokens",
    "input_tokens_details",
    "prompt_tokens_details",
    "output_tokens_details",
    "completion_tokens_details",
    "cache_creation",
];
const CAMEL_FIELDS: &[&str] = &[
    "inputTokens",
    "promptTokens",
    "outputTokens",
    "completionTokens",
    "totalTokens",
    "cachedReadTokens",
    "cacheReadTokens",
    "cacheCreationTokens",
    "reasoningTokens",
    "thinkingTokens",
    "apiDurationMs",
    "costUsdTicks",
    "modelCalls",
    "modelUsage",
];
