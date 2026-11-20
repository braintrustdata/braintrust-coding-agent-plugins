//! Dotted option keys observed in native Muse hooks. These options coexist:
//! selecting one provider variant for the entire object would discard others.

mod reasoning_effort;
mod thinking_mode;

pub(crate) use reasoning_effort::ReasoningEffort;
pub(crate) use thinking_mode::ThinkingMode;

use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ProviderOptions {
    #[serde(
        default,
        rename = "anthropic.output_config.effort",
        skip_serializing_if = "Field::is_missing"
    )]
    pub anthropic_effort: Field<ReasoningEffort>,
    #[serde(
        default,
        rename = "anthropic.thinking",
        skip_serializing_if = "Field::is_missing"
    )]
    pub anthropic_thinking: Field<ThinkingMode>,
    #[serde(
        default,
        rename = "anthropic.thinking.budget_tokens",
        skip_serializing_if = "Field::is_missing"
    )]
    pub anthropic_thinking_budget: Field<u64>,
    #[serde(
        default,
        rename = "local.reasoning.effort",
        skip_serializing_if = "Field::is_missing"
    )]
    pub local_effort: Field<ReasoningEffort>,
    #[serde(
        default,
        rename = "meta.reasoning.effort",
        skip_serializing_if = "Field::is_missing"
    )]
    pub reasoning_effort: Field<ReasoningEffort>,
    #[serde(
        default,
        rename = "openai.reasoning.effort",
        skip_serializing_if = "Field::is_missing"
    )]
    pub openai_effort: Field<ReasoningEffort>,
    #[serde(
        default,
        rename = "openrouter.reasoning.effort",
        skip_serializing_if = "Field::is_missing"
    )]
    pub openrouter_effort: Field<ReasoningEffort>,
    #[serde(
        default,
        rename = "meta.session_id",
        skip_serializing_if = "Field::is_missing"
    )]
    pub session_id: Field<String>,
    #[serde(
        default,
        rename = "meta.traceparent",
        skip_serializing_if = "Field::is_missing"
    )]
    pub traceparent: Field<String>,
    /// Provider-defined keys outside the recorded contract are never inspected.
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extensions: Extensions,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn provider_options_coexist_and_preserve_null_unknown_values_and_new_modes() {
        let raw = json!({
            "anthropic.output_config.effort":"low", "anthropic.thinking":"enabled",
            "anthropic.thinking.budget_tokens":4096, "openai.reasoning.effort":"high",
            "meta.reasoning.effort":null, "local.reasoning.effort":"future",
            "vendor.future":{"nested":[null,42]}
        });
        let options: ProviderOptions = serde_json::from_value(raw.clone()).unwrap();
        assert!(matches!(
            options.anthropic_effort,
            Field::Value(ReasoningEffort::Low)
        ));
        assert!(matches!(
            options.openai_effort,
            Field::Value(ReasoningEffort::High)
        ));
        assert!(matches!(options.reasoning_effort, Field::Null));
        assert!(matches!(
            options.local_effort,
            Field::Value(ReasoningEffort::Other(_))
        ));
        assert_eq!(serde_json::to_value(options).unwrap(), raw);
        for raw in [
            json!({"anthropic.thinking.budget_tokens":"4096"}),
            json!({"openai.reasoning.effort":42}),
        ] {
            assert!(serde_json::from_value::<ProviderOptions>(raw).is_err());
        }
    }
}
