//! Bounded, counted-once Muse reducer state. Native counter DTOs and arithmetic
//! are independent of the translator in crate::muse::usage.
use super::super::recent::RecentMap;
use crate::muse::usage::{TokenMetrics, TokenUsage, UsageScope};

#[derive(Default)]
pub(super) struct UsageTracker {
    turn_usage: RecentMap<String, TokenUsage>,
    model_metrics: RecentMap<String, TokenMetrics>,
    pub totals: TokenMetrics,
}
impl UsageTracker {
    pub fn record(
        &mut self,
        turn: Option<&str>,
        request: &str,
        usage: &TokenUsage,
        scope: UsageScope,
        request_was_open: bool,
    ) -> TokenMetrics {
        let mut counters = usage.clone();
        if scope == UsageScope::Turn && request_was_open {
            if let Some(turn) = turn {
                if let Some(previous) = self.turn_usage.get(turn) {
                    counters = usage.delta_since(previous);
                }
                self.turn_usage.insert(turn.to_owned(), usage.clone());
            }
        }
        let metrics = counters.metrics();
        if request_was_open && self.model_metrics.get(request).is_none() {
            self.totals.add(&metrics);
            self.model_metrics
                .insert(request.to_owned(), metrics.clone());
        }
        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn cumulative_usage_resets_missing_counters_and_replay() {
        let mut tracker = UsageTracker::default();
        let first: TokenUsage =
            serde_json::from_value(json!({"input_tokens":10,"output_tokens":5})).unwrap();
        let second: TokenUsage =
            serde_json::from_value(json!({"input_tokens":14,"output_tokens":8})).unwrap();
        tracker.record(Some("turn"), "a", &first, UsageScope::Turn, true);
        let delta = tracker.record(Some("turn"), "b", &second, UsageScope::Turn, true);
        assert_eq!(delta.tokens, Some(7));
        tracker.record(Some("turn"), "b", &second, UsageScope::Turn, false);
        assert_eq!(tracker.totals.tokens, Some(22));
        let reset = tracker.record(Some("turn"), "c", &first, UsageScope::Turn, true);
        assert_eq!(reset.tokens, Some(15));
        assert_eq!(reset.prompt_cached_tokens, None);
    }
    #[test]
    fn per_model_alias_precedence_and_saturation() {
        let usage: TokenUsage = serde_json::from_value(json!({"input_tokens":u64::MAX,"output_tokens":3,"cached_tokens":0,"cache_read_tokens":4})).unwrap();
        let metrics = usage.metrics();
        assert_eq!(metrics.tokens, Some(u64::MAX));
        assert_eq!(metrics.prompt_cached_tokens, Some(0));
        let mut tracker = UsageTracker::default();
        tracker.record(Some("turn"), "a", &usage, UsageScope::Model, true);
        tracker.record(Some("turn"), "b", &usage, UsageScope::Model, true);
        assert_eq!(tracker.totals.tokens, Some(u64::MAX));
    }
}
