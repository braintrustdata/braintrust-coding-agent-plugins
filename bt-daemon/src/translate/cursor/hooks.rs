//! Typed Cursor hook payloads and transcript records.
//!
//! The journal keeps every native payload verbatim. The translator parses it
//! here once per event and works with these types from then on. Scalar fields
//! with an unexpected JSON type are treated as absent (see [`lenient`]).
//! Arbitrary native values that are forwarded verbatim into spans, such as
//! tool inputs and outputs, remain `Value`.

use crate::translate::lenient::{self, parse};
use serde::Deserialize;
use serde_json::Value;

/// Fields read from every Cursor hook, regardless of its event.
#[derive(Default, Deserialize)]
pub(super) struct HookCommon {
    #[serde(default, deserialize_with = "lenient::string")]
    pub cwd: Option<String>,
    #[serde(
        default,
        rename = "workspace_roots",
        deserialize_with = "lenient::first_string"
    )]
    pub workspace_root: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub model_id: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "lenient::present")]
    pub model_params: Option<Value>,
    /// Set by transcript import when the session start time is inferred.
    #[serde(default, deserialize_with = "lenient::bool")]
    pub start_time_estimated: Option<bool>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub generation_id: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub tool_use_id: Option<String>,
    /// Added by the daemon when it mirrors the session transcript.
    #[serde(
        default,
        rename = "_bt_transcript_mirror",
        deserialize_with = "lenient::nested"
    )]
    pub transcript_mirror: Option<TranscriptMirror>,
}

#[derive(Default, Deserialize)]
pub(super) struct TranscriptMirror {
    #[serde(default, deserialize_with = "lenient::string")]
    pub mirror: Option<String>,
    #[serde(default, deserialize_with = "lenient::u64")]
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
    pub fn parse(event: &str, payload: &Value) -> Self {
        match event {
            "sessionStart" => Self::SessionStart,
            "beforeSubmitPrompt" => Self::BeforeSubmitPrompt(parse(payload)),
            "preToolUse" => Self::Tool(ToolPhase::Pre, parse(payload)),
            "postToolUse" => Self::Tool(ToolPhase::Post, parse(payload)),
            "postToolUseFailure" => Self::Tool(ToolPhase::Failure, parse(payload)),
            "afterAgentThought" => Self::AfterAgentThought(parse(payload)),
            "afterAgentResponse" => Self::AfterAgentResponse(parse(payload)),
            "stop" => Self::Stop(parse(payload)),
            "sessionEnd" => Self::SessionEnd(parse(payload)),
            "subagentStart" => Self::Subagent(SubagentPhase::Start, parse(payload)),
            "subagentStop" => Self::Subagent(SubagentPhase::Stop, parse(payload)),
            "preCompact" => Self::PreCompact,
            "afterShellExecution" => Self::Specialized(SpecializedKind::Shell, parse(payload)),
            "afterMCPExecution" => Self::Specialized(SpecializedKind::Mcp, parse(payload)),
            "afterFileEdit" => Self::Specialized(SpecializedKind::FileEdit, parse(payload)),
            "beforeReadFile" => Self::Specialized(SpecializedKind::ReadFile, parse(payload)),
            _ => Self::Other,
        }
    }
}

#[derive(Default, Deserialize)]
pub(super) struct PromptHook {
    #[serde(default, deserialize_with = "lenient::string")]
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
    #[serde(default, deserialize_with = "lenient::string")]
    pub tool_name: Option<String>,
    #[serde(default, deserialize_with = "lenient::present")]
    pub tool_input: Option<Value>,
    #[serde(default, deserialize_with = "lenient::present")]
    pub tool_output: Option<Value>,
    /// Native execution time in milliseconds, reported on terminal hooks.
    #[serde(default, deserialize_with = "lenient::f64")]
    pub duration: Option<f64>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub parent_tool_call_id: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub error_message: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub failure_type: Option<String>,
    #[serde(default, deserialize_with = "lenient::bool")]
    pub is_interrupt: Option<bool>,
}

#[derive(Default, Deserialize)]
pub(super) struct TextHook {
    #[serde(default, deserialize_with = "lenient::string")]
    pub text: Option<String>,
}

#[derive(Default, Deserialize)]
pub(super) struct ResponseHook {
    #[serde(default, deserialize_with = "lenient::string")]
    pub text: Option<String>,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

#[derive(Default, Deserialize)]
pub(super) struct StopHook {
    #[serde(default, deserialize_with = "lenient::string")]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub error_message: Option<String>,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

/// Token totals for one user generation, repeated on response and stop.
#[derive(Default, Deserialize)]
pub(super) struct TurnUsage {
    #[serde(default, deserialize_with = "lenient::u64")]
    pub input_tokens: Option<u64>,
    #[serde(default, deserialize_with = "lenient::u64")]
    pub output_tokens: Option<u64>,
    #[serde(default, deserialize_with = "lenient::u64")]
    pub cache_read_tokens: Option<u64>,
    #[serde(default, deserialize_with = "lenient::u64")]
    pub cache_write_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
pub(super) struct SessionEndHook {
    #[serde(default, deserialize_with = "lenient::string")]
    pub reason: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub final_status: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub error_message: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SubagentPhase {
    Start,
    Stop,
}

#[derive(Default, Deserialize)]
pub(super) struct SubagentHook {
    #[serde(default, deserialize_with = "lenient::string")]
    pub subagent_id: Option<String>,
    /// The tool call that spawned the subagent.
    #[serde(default, deserialize_with = "lenient::string")]
    pub tool_call_id: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub task: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub subagent_model: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub subagent_type: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub summary: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
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
    #[serde(default, deserialize_with = "lenient::string")]
    pub command: Option<String>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub tool_name: Option<String>,
    /// MCP arguments, which Cursor may send as a JSON-encoded string.
    #[serde(default, deserialize_with = "lenient::present")]
    pub tool_input: Option<Value>,
    #[serde(default, deserialize_with = "lenient::string")]
    pub file_path: Option<String>,
    /// File contents observed before a read.
    #[serde(default, deserialize_with = "lenient::string")]
    pub content: Option<String>,
    #[serde(default, deserialize_with = "lenient::present")]
    pub output: Option<Value>,
    #[serde(default, deserialize_with = "lenient::present")]
    pub result_json: Option<Value>,
    #[serde(default, deserialize_with = "lenient::present")]
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
    #[serde(default, deserialize_with = "lenient::string")]
    pub role: Option<String>,
    #[serde(default, deserialize_with = "lenient::nested")]
    pub message: Option<TranscriptMessage>,
}

#[derive(Default, Deserialize)]
pub(super) struct TranscriptMessage {
    /// The concatenated text of the message content, which is either a
    /// string or an array of content blocks. Other block types are ignored.
    #[serde(default, rename = "content", deserialize_with = "content_text")]
    pub text: Option<String>,
}

fn content_text<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let content = Value::deserialize(d)?;
    if let Some(text) = content.as_str() {
        return Ok(Some(text.into()));
    }
    Ok(Some(
        content
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect(),
    ))
}
