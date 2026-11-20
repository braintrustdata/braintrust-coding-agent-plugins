//! Native usage schemas shared by agent capture, transcript parsing, and reducers.
//!
//! Agents usually normalize providers themselves. The enum identifies the wire
//! format, not a guessed provider identity: OpenAI-compatible and Anthropic
//! payloads can share field names. Only explicitly modeled usage fields become
//! metrics. Unknown extensions round-trip as opaque JSON and are never scanned.

mod camel_case_usage;
mod opencode_tokens;
mod pi_usage;
mod provider_usage;
mod snake_case_usage;
mod token_metrics;
mod usage_metrics;

pub(crate) use camel_case_usage::CamelCaseUsage;
pub(crate) use opencode_tokens::OpenCodeTokens;
pub(crate) use pi_usage::PiUsage;
pub(crate) use provider_usage::ProviderUsage;
pub(crate) use snake_case_usage::SnakeCaseUsage;
pub(crate) use token_metrics::TokenMetrics;
pub(crate) use usage_metrics::UsageMetrics;

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
