use super::UsageMetrics;
use crate::json::Extensions;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
