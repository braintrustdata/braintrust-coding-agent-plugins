use serde::{Deserialize, Serialize};

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
