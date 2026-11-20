use super::{ModelOutput, NativeOutcome, ProviderOptions};
use crate::json::Field;
use crate::muse::usage::{TokenUsage, UsageScope};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelFinished {
    pub usage: Option<TokenUsage>,
    #[serde(default)]
    pub usage_scope: UsageScope,
    pub output_text_preview: Option<ModelOutput>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub response_id: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub finish_reason: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub tool_call_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub options: Field<ProviderOptions>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
