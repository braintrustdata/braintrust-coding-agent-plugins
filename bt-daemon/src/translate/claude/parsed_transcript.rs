use super::{llm_call::LlmCall, message_history::MessageHistory, transcript_tool::TranscriptTool};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct ParsedTranscript {
    pub(super) calls: Vec<LlmCall>,
    pub(super) tools: Vec<TranscriptTool>,
    pub(super) history: MessageHistory,
}
