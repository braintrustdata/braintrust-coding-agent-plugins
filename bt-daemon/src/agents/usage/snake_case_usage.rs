use super::UsageMetrics;
use crate::json::Extensions;
use serde::{Deserialize, Serialize};

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
