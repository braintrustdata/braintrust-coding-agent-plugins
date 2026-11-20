use super::{ExportToolCall, ExportToolResult};
use crate::muse::hooks::{ErrorDetail, InputSummary};
use crate::muse::usage::TokenUsage;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum RunEvent {
    Started {
        prompt: Option<String>,
    },
    ModelInputTraceRecorded {
        request_record_id: String,
        model_step: Option<u64>,
        bounded: Option<InputSummary>,
    },
    ModelResponseCreated {
        response_id: Option<String>,
    },
    ModelCompleted {
        usage: Option<TokenUsage>,
    },
    AssistantToolCallsCommitted {
        tool_calls: Vec<ExportToolCall>,
        response_id: Option<String>,
    },
    ToolResultBatchCommitted {
        results: Vec<ExportToolResult>,
    },
    ModelFailureTerminalIntent {
        reason: Option<String>,
    },
    // Resource samples are not provider token usage. Their unused body is skipped.
    ResourceUsageSampled,
    AssistantMessageCommitted {
        text: String,
        response_id: Option<String>,
    },
    Terminal {
        terminal: String,
        reason: Option<ErrorDetail>,
    },
    #[serde(other)]
    Ignored,
}
