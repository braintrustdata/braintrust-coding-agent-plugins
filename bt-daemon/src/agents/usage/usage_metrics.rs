use serde::Serialize;

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
