//! Native usage schemas shared by agent capture, transcript parsing, and reducers.
//!
//! Agents usually normalize providers themselves. The enum identifies the wire
//! format, not a guessed provider identity: OpenAI-compatible and Anthropic
//! payloads can share field names. Only explicitly modeled usage fields become
//! metrics. Unknown extensions round-trip as opaque JSON and are never scanned.
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

/// Snake-case input/output usage (Codex/OpenAI-compatible and Claude fields).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SnakeCaseUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cached_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_creation_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_details: Option<TokenDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<TokenDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens_details: Option<TokenDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<TokenDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation: Option<CacheCreation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TokenDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CacheCreation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_5m_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_1h_input_tokens: Option<u64>,
}
/// Camel-case counters emitted by Antigravity and Grok's ACP transcripts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CamelCaseUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_duration_ms: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_ticks: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_usage: Option<BTreeMap<String, CamelCaseUsage>>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
/// Typed normalized metrics for the readers that historically emit floats.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct UsageMetrics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cached_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_creation_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_reasoning_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_ticks: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_calls: Option<f64>,
}
impl UsageMetrics {
    pub fn is_empty(&self) -> bool {
        [
            self.prompt_tokens,
            self.completion_tokens,
            self.tokens,
            self.prompt_cached_tokens,
            self.prompt_cache_creation_tokens,
            self.completion_reasoning_tokens,
            self.thinking_tokens,
            self.cost,
            self.estimated_cost,
            self.api_duration_ms,
            self.cost_usd_ticks,
            self.model_calls,
        ]
        .iter()
        .all(Option::is_none)
    }
    pub fn with_total(mut self) -> Self {
        if self.tokens.is_none() {
            self.tokens = self
                .prompt_tokens
                .zip(self.completion_tokens)
                .map(|(p, c)| p + c);
        }
        self
    }
    pub fn observe(&mut self, other: Self) {
        if other.prompt_tokens.is_some() {
            self.prompt_tokens = other.prompt_tokens;
        }
        if other.completion_tokens.is_some() {
            self.completion_tokens = other.completion_tokens;
        }
        if other.tokens.is_some() {
            self.tokens = other.tokens;
        }
        if other.prompt_cached_tokens.is_some() {
            self.prompt_cached_tokens = other.prompt_cached_tokens;
        }
        if other.prompt_cache_creation_tokens.is_some() {
            self.prompt_cache_creation_tokens = other.prompt_cache_creation_tokens;
        }
        if other.completion_reasoning_tokens.is_some() {
            self.completion_reasoning_tokens = other.completion_reasoning_tokens;
        }
        if other.thinking_tokens.is_some() {
            self.thinking_tokens = other.thinking_tokens;
        }
        if other.cost.is_some() {
            self.cost = other.cost;
        }
        if other.estimated_cost.is_some() {
            self.estimated_cost = other.estimated_cost;
        }
        if other.api_duration_ms.is_some() {
            self.api_duration_ms = other.api_duration_ms;
        }
        if other.cost_usd_ticks.is_some() {
            self.cost_usd_ticks = other.cost_usd_ticks;
        }
        if other.model_calls.is_some() {
            self.model_calls = other.model_calls;
        }
    }
}
impl SnakeCaseUsage {
    pub fn metrics(&self) -> UsageMetrics {
        let input_details = self.input_tokens_details.as_ref();
        let prompt_details = self.prompt_tokens_details.as_ref();
        let output_details = self.output_tokens_details.as_ref();
        let completion_details = self.completion_tokens_details.as_ref();
        UsageMetrics {
            prompt_tokens: self.input_tokens.or(self.prompt_tokens).map(|n| n as f64),
            completion_tokens: self
                .output_tokens
                .or(self.completion_tokens)
                .map(|n| n as f64),
            tokens: self.total_tokens.or(self.tokens).map(|n| n as f64),
            prompt_cached_tokens: self
                .cached_input_tokens
                .or(self.prompt_cached_tokens)
                .or_else(|| input_details.and_then(|d| d.cached_tokens))
                .or_else(|| prompt_details.and_then(|d| d.cached_tokens))
                .or(self.cache_read_tokens)
                .or(self.cache_read_input_tokens)
                .map(|n| n as f64),
            prompt_cache_creation_tokens: self
                .prompt_cache_creation_tokens
                .or_else(|| {
                    input_details.and_then(|d| d.cache_creation_tokens.or(d.cache_write_tokens))
                })
                .or_else(|| {
                    prompt_details.and_then(|d| d.cache_creation_tokens.or(d.cache_write_tokens))
                })
                .or(self.cache_creation_input_tokens)
                .map(|n| n as f64),
            completion_reasoning_tokens: self
                .reasoning_output_tokens
                .or(self.completion_reasoning_tokens)
                .or(self.reasoning_tokens)
                .or_else(|| output_details.and_then(|d| d.reasoning_tokens))
                .or_else(|| completion_details.and_then(|d| d.reasoning_tokens))
                .map(|n| n as f64),
            thinking_tokens: self.thinking_tokens.map(|n| n as f64),
            cost: self.cost,
            estimated_cost: self
                .cost
                .or(self.estimated_cost)
                .or(self.total_cost)
                .or(self.cost_usd),
            ..UsageMetrics::default()
        }
    }
    pub fn is_empty(&self) -> bool {
        self.metrics().is_empty()
            && self.extra.is_empty()
            && self.cache_creation.is_none()
            && self.input_tokens_details.is_none()
            && self.prompt_tokens_details.is_none()
            && self.output_tokens_details.is_none()
            && self.completion_tokens_details.is_none()
    }
}
impl CamelCaseUsage {
    pub fn metrics(&self) -> UsageMetrics {
        UsageMetrics {
            prompt_tokens: self.input_tokens.or(self.prompt_tokens).map(|n| n as f64),
            completion_tokens: self
                .output_tokens
                .or(self.completion_tokens)
                .map(|n| n as f64),
            tokens: self.total_tokens.map(|n| n as f64),
            prompt_cached_tokens: self
                .cached_read_tokens
                .or(self.cache_read_tokens)
                .map(|n| n as f64),
            prompt_cache_creation_tokens: self.cache_creation_tokens.map(|n| n as f64),
            completion_reasoning_tokens: self.reasoning_tokens.map(|n| n as f64),
            thinking_tokens: self.thinking_tokens.map(|n| n as f64),
            api_duration_ms: self
                .api_duration_ms
                .as_ref()
                .and_then(serde_json::Number::as_f64),
            cost_usd_ticks: self
                .cost_usd_ticks
                .as_ref()
                .and_then(serde_json::Number::as_f64),
            model_calls: self.model_calls.map(|n| n as f64),
            ..UsageMetrics::default()
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct TokenMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cached_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_creation_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_reasoning_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
}

impl TokenMetrics {
    pub(crate) fn add(&mut self, other: &Self) {
        fn add(to: &mut Option<u64>, from: Option<u64>) {
            if let Some(from) = from {
                *to = Some(to.unwrap_or(0).saturating_add(from));
            }
        }
        add(&mut self.prompt_tokens, other.prompt_tokens);
        add(&mut self.completion_tokens, other.completion_tokens);
        add(&mut self.prompt_cached_tokens, other.prompt_cached_tokens);
        add(
            &mut self.prompt_cache_creation_tokens,
            other.prompt_cache_creation_tokens,
        );
        add(
            &mut self.completion_reasoning_tokens,
            other.completion_reasoning_tokens,
        );
        add(&mut self.tokens, other.tokens);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct OpenCodeTokens {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    #[serde(default)]
    pub reasoning: i64,
    #[serde(default)]
    pub cache: OpenCodeCacheTokens,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct OpenCodeCacheTokens {
    #[serde(default)]
    pub read: i64,
    #[serde(default)]
    pub write: i64,
}

/// Pi normalizes provider usage before forwarding events to the daemon.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(crate) struct PiUsage {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    // Pi reports `reasoning` as a subset of `output`, and only for providers
    // that expose the breakdown.
    #[serde(default)]
    pub reasoning: Option<i64>,
    #[serde(default)]
    pub cache_read: i64,
    // `cacheWrite1h` is the subset of `cacheWrite` written with 1h retention.
    #[serde(default)]
    pub cache_write: i64,
    #[serde(default)]
    pub cache_write1h: Option<i64>,
    #[serde(default)]
    pub total_tokens: Option<i64>,
    #[serde(default)]
    pub cost: Option<PiCost>,
}

#[derive(Default, Deserialize, Serialize)]
pub(crate) struct PiCost {
    #[serde(default)]
    pub total: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn provider_usage_selects_the_wire_format_and_round_trips_extensions() {
        for (raw, camel) in [
            (
                json!({"input_tokens":10,"output_tokens":2,"future":{"deep":[true,null]}}),
                false,
            ),
            (
                json!({"inputTokens":10,"outputTokens":2,"future":{"deep":[true,null]}}),
                true,
            ),
            (json!({"inputTokens":10,"outputTokens":2,"cost":0.25}), true),
            (
                json!({"prompt_tokens":10,"completion_tokens":2,"future":{"deep":[true,null]}}),
                false,
            ),
        ] {
            let usage: ProviderUsage = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(matches!(&usage, ProviderUsage::CamelCase(_)), camel);
            assert_eq!(usage.metrics().prompt_tokens, Some(10.0));
            assert_eq!(usage.metrics().completion_tokens, Some(2.0));
            assert_eq!(serde_json::to_value(usage).unwrap(), raw);
        }
    }

    #[test]
    fn unknown_usage_is_opaque_and_malformed_known_usage_is_an_error() {
        let raw = json!({"vendorExtension":{"inputTokens":999}});
        let usage: ProviderUsage = serde_json::from_value(raw.clone()).unwrap();
        assert!(matches!(&usage, ProviderUsage::Unknown(_)));
        assert!(usage.metrics().is_empty());
        assert_eq!(serde_json::to_value(usage).unwrap(), raw);
        for raw in [
            json!({"inputTokens":"10"}),
            json!({"output_tokens":{}}),
            json!({"input_tokens":1,"outputTokens":2}),
            json!({"input_tokens":-1}),
        ] {
            assert!(serde_json::from_value::<ProviderUsage>(raw).is_err());
        }
    }

    #[test]
    fn codex_alias_precedence_details_and_cost_remain_typed() {
        let usage: SnakeCaseUsage = serde_json::from_value(json!({
            "input_tokens":10,"prompt_tokens":999,"output_tokens":2,
            "cached_input_tokens":3,"input_tokens_details":{"cached_tokens":999,"cache_write_tokens":4},
            "output_tokens_details":{"reasoning_tokens":1},"cost":0.25,"total_cost":99
        })).unwrap();
        let metrics = usage.metrics().with_total();
        assert_eq!(metrics.prompt_tokens, Some(10.0));
        assert_eq!(metrics.tokens, Some(12.0));
        assert_eq!(metrics.prompt_cached_tokens, Some(3.0));
        assert_eq!(metrics.prompt_cache_creation_tokens, Some(4.0));
        assert_eq!(metrics.completion_reasoning_tokens, Some(1.0));
        assert_eq!(metrics.estimated_cost, Some(0.25));
    }

    #[test]
    fn grok_per_model_usage_is_typed_and_preserves_partial_counts() {
        let raw =
            json!({"inputTokens":10,"modelUsage":{"model-a":{"outputTokens":2,"future":null}}});
        let usage: CamelCaseUsage = serde_json::from_value(raw.clone()).unwrap();
        let model = &usage.model_usage.as_ref().unwrap()["model-a"];
        assert_eq!(model.input_tokens, None);
        assert_eq!(model.output_tokens, Some(2));
        assert_eq!(serde_json::to_value(usage).unwrap(), raw);
    }

    #[test]
    fn both_opencode_protocols_keep_the_same_cache_counter_shape() {
        let tokens: OpenCodeTokens = serde_json::from_value(json!({
            "input":5,"output":2,"reasoning":1,"cache":{"read":3,"write":4}
        }))
        .unwrap();
        assert_eq!(tokens.input + tokens.cache.read + tokens.cache.write, 12);
        assert_eq!(tokens.output, 2);
        assert_eq!(tokens.reasoning, 1);
    }
}
