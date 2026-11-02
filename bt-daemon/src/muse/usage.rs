//! Hook/export counters are snake_case, optional, and may be turn-cumulative.
//! They are not the SDK's camelCase, required-counter MSP view types.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub(crate) struct TokenUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UsageScope {
    #[default]
    Turn,
    Model,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
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

impl TokenUsage {
    pub fn delta_since(&self, previous: &Self) -> Self {
        fn delta(current: Option<u64>, previous: Option<u64>) -> Option<u64> {
            current.map(|current| {
                previous
                    .and_then(|p| current.checked_sub(p))
                    .unwrap_or(current)
            })
        }
        Self {
            input_tokens: delta(self.input_tokens, previous.input_tokens),
            output_tokens: delta(self.output_tokens, previous.output_tokens),
            cached_tokens: delta(self.cached_tokens, previous.cached_tokens),
            cache_read_tokens: delta(self.cache_read_tokens, previous.cache_read_tokens),
            cache_write_tokens: delta(self.cache_write_tokens, previous.cache_write_tokens),
            reasoning_tokens: delta(self.reasoning_tokens, previous.reasoning_tokens),
        }
    }
    pub fn metrics(&self) -> TokenMetrics {
        TokenMetrics {
            prompt_tokens: self.input_tokens,
            completion_tokens: self.output_tokens,
            // Preserve the native alias precedence used before this refactor.
            prompt_cached_tokens: self.cached_tokens.or(self.cache_read_tokens),
            prompt_cache_creation_tokens: self.cache_write_tokens,
            completion_reasoning_tokens: self.reasoning_tokens,
            tokens: self
                .input_tokens
                .zip(self.output_tokens)
                .map(|(i, o)| i.saturating_add(o)),
        }
    }
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
