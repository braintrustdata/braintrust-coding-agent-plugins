//! Typed Cursor hook payloads and transcript records.
//!
//! The journal keeps every native payload verbatim. The translator decodes it
//! here once per event and works with these types from then on. A field with
//! an unexpected type fails the event with a [`DecodeError`]; unknown fields
//! and events are ignored. Arbitrary native values that are forwarded
//! verbatim into spans, such as tool inputs and outputs, remain `Value`.

use crate::translate::decode::{decode, DecodeError};
use serde::Deserialize;
use serde_json::Value;

/// Fields read from every Cursor hook, regardless of its event.
#[derive(Default, Deserialize)]
pub(super) struct HookCommon {
    pub cwd: Option<String>,
    pub workspace_roots: Option<Vec<String>>,
    pub model_id: Option<String>,
    pub model: Option<String>,
    pub model_params: Option<Value>,
    /// Set by transcript import when the session start time is inferred.
    pub start_time_estimated: Option<bool>,
    pub generation_id: Option<String>,
    pub tool_use_id: Option<String>,
    /// Added by the daemon when it mirrors the session transcript. Empty when
    /// no transcript was available at this hook.
    #[serde(rename = "_bt_transcript_mirror")]
    pub transcript_mirror: Option<TranscriptMirror>,
}

impl HookCommon {
    pub fn decode(event: &str, payload: &Value) -> Result<Self, DecodeError> {
        decode("cursor", format!("{event} hook"), payload)
    }

    pub fn workspace_root(&self) -> Option<&str> {
        self.workspace_roots.as_ref()?.first().map(String::as_str)
    }
}

#[derive(Default, Deserialize)]
pub(super) struct TranscriptMirror {
    pub mirror: Option<String>,
    pub through: Option<u64>,
}

/// One Cursor hook, decoded by event name.
pub(super) enum CursorHook {
    SessionStart,
    BeforeSubmitPrompt(PromptHook),
    Tool(ToolPhase, ToolHook),
    AfterAgentThought(TextHook),
    AfterAgentResponse(ResponseHook),
    Stop(StopHook),
    SessionEnd(SessionEndHook),
    Subagent(SubagentPhase, SubagentHook),
    PreCompact,
    Specialized(SpecializedKind, SpecializedHook),
    /// An event this translator does not reduce.
    Other,
}

impl CursorHook {
    pub fn decode(event: &str, payload: &Value) -> Result<Self, DecodeError> {
        let what = format!("{event} hook");
        Ok(match event {
            "sessionStart" => Self::SessionStart,
            "beforeSubmitPrompt" => Self::BeforeSubmitPrompt(decode("cursor", &what, payload)?),
            "preToolUse" => Self::Tool(ToolPhase::Pre, decode("cursor", &what, payload)?),
            "postToolUse" => Self::Tool(ToolPhase::Post, decode("cursor", &what, payload)?),
            "postToolUseFailure" => {
                Self::Tool(ToolPhase::Failure, decode("cursor", &what, payload)?)
            }
            "afterAgentThought" => Self::AfterAgentThought(decode("cursor", &what, payload)?),
            "afterAgentResponse" => Self::AfterAgentResponse(decode("cursor", &what, payload)?),
            "stop" => Self::Stop(decode("cursor", &what, payload)?),
            "sessionEnd" => Self::SessionEnd(decode("cursor", &what, payload)?),
            "subagentStart" => {
                Self::Subagent(SubagentPhase::Start, decode("cursor", &what, payload)?)
            }
            "subagentStop" => {
                Self::Subagent(SubagentPhase::Stop, decode("cursor", &what, payload)?)
            }
            "preCompact" => Self::PreCompact,
            "afterShellExecution" => {
                Self::Specialized(SpecializedKind::Shell, decode("cursor", &what, payload)?)
            }
            "afterMCPExecution" => {
                Self::Specialized(SpecializedKind::Mcp, decode("cursor", &what, payload)?)
            }
            "afterFileEdit" => {
                Self::Specialized(SpecializedKind::FileEdit, decode("cursor", &what, payload)?)
            }
            "beforeReadFile" => {
                Self::Specialized(SpecializedKind::ReadFile, decode("cursor", &what, payload)?)
            }
            _ => Self::Other,
        })
    }
}

#[derive(Default, Deserialize)]
pub(super) struct PromptHook {
    pub prompt: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ToolPhase {
    Pre,
    Post,
    Failure,
}

#[derive(Default, Deserialize)]
pub(super) struct ToolHook {
    pub tool_name: Option<String>,
    pub tool_input: Option<Value>,
    pub tool_output: Option<Value>,
    /// Native execution time in milliseconds, reported on terminal hooks.
    pub duration: Option<f64>,
    pub parent_tool_call_id: Option<String>,
    pub error_message: Option<String>,
    pub failure_type: Option<String>,
    pub is_interrupt: Option<bool>,
}

#[derive(Default, Deserialize)]
pub(super) struct TextHook {
    pub text: Option<String>,
}

#[derive(Default, Deserialize)]
pub(super) struct ResponseHook {
    pub text: Option<String>,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

#[derive(Default, Deserialize)]
pub(super) struct StopHook {
    pub status: Option<String>,
    pub error_message: Option<String>,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

/// Token totals for one user generation, repeated on response and stop.
#[derive(Default, Deserialize)]
pub(super) struct TurnUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
pub(super) struct SessionEndHook {
    pub reason: Option<String>,
    pub final_status: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SubagentPhase {
    Start,
    Stop,
}

#[derive(Default, Deserialize)]
pub(super) struct SubagentHook {
    pub subagent_id: Option<String>,
    /// The tool call that spawned the subagent.
    pub tool_call_id: Option<String>,
    pub task: Option<String>,
    pub subagent_model: Option<String>,
    pub subagent_type: Option<String>,
    pub summary: Option<String>,
    pub status: Option<String>,
}

/// Hooks that carry a richer result for a tool already reported by the
/// generic tool hooks.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SpecializedKind {
    Shell,
    Mcp,
    FileEdit,
    ReadFile,
}

impl SpecializedKind {
    /// The generic tool name this hook can enrich.
    pub fn accepts(self, tool_name: &str, hook: &SpecializedHook) -> bool {
        match self {
            Self::Shell => tool_name == "Shell",
            Self::Mcp => {
                tool_name.strip_prefix("MCP:").unwrap_or(tool_name)
                    == hook.tool_name.as_deref().unwrap_or("")
            }
            Self::ReadFile => tool_name == "Read",
            Self::FileEdit => tool_name == "Write",
        }
    }
}

#[derive(Default, Deserialize)]
pub(super) struct SpecializedHook {
    pub command: Option<String>,
    pub tool_name: Option<String>,
    /// MCP arguments, which Cursor may send as a JSON-encoded string.
    pub tool_input: Option<Value>,
    pub file_path: Option<String>,
    /// File contents observed before a read.
    pub content: Option<String>,
    pub output: Option<Value>,
    pub result_json: Option<Value>,
    pub edits: Option<Value>,
}

impl SpecializedHook {
    /// The native result, whichever field this hook reports it in.
    pub fn result(&self) -> Option<&Value> {
        self.output
            .as_ref()
            .or(self.result_json.as_ref())
            .or(self.edits.as_ref())
    }
}

/// One line of Cursor's saved transcript.
#[derive(Default, Deserialize)]
pub(super) struct TranscriptRecord {
    pub role: Option<String>,
    pub message: Option<TranscriptMessage>,
}

#[derive(Default, Deserialize)]
pub(super) struct TranscriptMessage {
    pub content: Option<MessageContent>,
}

/// Message content: plain text, or content blocks of which only text blocks
/// carry observable text.
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Deserialize)]
pub(super) struct ContentBlock {
    pub text: Option<String>,
}

impl MessageContent {
    /// The concatenated text of the content.
    pub fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Blocks(blocks) => blocks
                .iter()
                .filter_map(|block| block.text.as_deref())
                .collect(),
        }
    }
}

impl TranscriptRecord {
    pub fn decode(record: &Value) -> Result<Self, DecodeError> {
        decode("cursor", "transcript record", record)
    }
}
