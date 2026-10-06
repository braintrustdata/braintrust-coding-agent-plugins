//! Typed Cursor hook payloads and transcript records.
//!
//! The journal keeps every native payload verbatim. The translator decodes it
//! here once per event and works with these types from then on. The shapes
//! follow the payloads the Cursor CLI builds for each hook: a field it always
//! sends is required, and a missing field or one with an unexpected type
//! fails the event with a [`DecodeError`]. Only fields Cursor omits in some
//! cases are optional. Fields the translator does not read are not modelled.
//!
//! Transcript import synthesizes its own `importStart`, `importCheckpoint`
//! and `importStop` events, which carry only what a saved transcript records.

use crate::translate::decode::{decode, DecodeError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One decoded Cursor hook.
pub(super) struct Hook {
    pub kind: CursorHook,
    /// Present on every live agent hook; absent on import events.
    pub common: Option<HookCommon>,
    /// Added by the daemon when it mirrors the session transcript.
    pub transcript_mirror: Option<TranscriptMirror>,
}

impl Hook {
    pub fn decode(event: &str, payload: &Value) -> Result<Self, DecodeError> {
        let what = format!("{event} hook");
        let kind = CursorHook::decode(event, &what, payload)?;
        let common = match kind {
            CursorHook::ImportStart(_)
            | CursorHook::ImportCheckpoint
            | CursorHook::ImportStop(_)
            | CursorHook::Other => None,
            _ => Some(decode("cursor", &what, payload)?),
        };
        let daemon: DaemonFields = decode("cursor", &what, payload)?;
        Ok(Self {
            kind,
            common,
            transcript_mirror: daemon.transcript_mirror,
        })
    }

    pub fn generation_id(&self) -> Option<&str> {
        self.common
            .as_ref()
            .map(|common| common.generation_id.as_str())
    }

    /// The model the agent reported, preferring its precise id.
    pub fn model(&self) -> Option<&str> {
        let common = self.common.as_ref()?;
        Some(common.model_id.as_deref().unwrap_or(&common.model))
    }

    pub fn model_params(&self) -> Option<&Value> {
        self.common.as_ref()?.model_params.as_ref()
    }

    /// The working directory, falling back to the first workspace root.
    pub fn cwd(&self) -> Option<&str> {
        let cwd = match &self.kind {
            CursorHook::Tool(_, tool) => tool.cwd.as_deref().filter(|cwd| !cwd.is_empty()),
            _ => None,
        };
        cwd.or_else(|| {
            self.common
                .as_ref()?
                .workspace_roots
                .first()
                .map(String::as_str)
        })
    }

    /// The native tool call this hook reports, if any.
    pub fn tool_use_id(&self) -> Option<&str> {
        match &self.kind {
            CursorHook::Tool(_, tool) => Some(&tool.tool_use_id),
            CursorHook::Specialized(specialized) => specialized.tool_use_id(),
            _ => None,
        }
    }
}

/// Fields the Cursor CLI sends with every agent hook.
#[derive(Deserialize)]
pub(super) struct HookCommon {
    /// Empty for background agents.
    pub generation_id: String,
    /// `unknown` when the CLI cannot name the model.
    pub model: String,
    pub model_id: Option<String>,
    pub model_params: Option<Value>,
    pub workspace_roots: Vec<String>,
}

#[derive(Deserialize)]
struct DaemonFields {
    #[serde(rename = "_bt_transcript_mirror")]
    transcript_mirror: Option<TranscriptMirror>,
}

/// The daemon's reference to its copy of the transcript at this hook.
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum TranscriptMirror {
    Captured {
        mirror: String,
        through: u64,
    },
    /// No transcript was available at this hook.
    Unavailable(EmptyObject),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EmptyObject {}

/// One Cursor hook, decoded by event name.
pub(super) enum CursorHook {
    SessionStart,
    BeforeSubmitPrompt(PromptHook),
    Tool(ToolPhase, ToolHook),
    AfterAgentThought(ThoughtHook),
    AfterAgentResponse(ResponseHook),
    Stop(StopHook),
    SessionEnd(SessionEndHook),
    SubagentStart(SubagentStartHook),
    SubagentStop(SubagentStopHook),
    PreCompact,
    Specialized(Specialized),
    ImportStart(ImportBoundary),
    ImportCheckpoint,
    ImportStop(ImportStopHook),
    /// An event this translator does not reduce.
    Other,
}

impl CursorHook {
    fn decode(event: &str, what: &str, payload: &Value) -> Result<Self, DecodeError> {
        Ok(match event {
            "sessionStart" => Self::SessionStart,
            "beforeSubmitPrompt" => Self::BeforeSubmitPrompt(decode("cursor", what, payload)?),
            "preToolUse" => Self::Tool(ToolPhase::Pre, decode("cursor", what, payload)?),
            "postToolUse" => {
                let hook: PostToolHook = decode("cursor", what, payload)?;
                Self::Tool(ToolPhase::Post(hook.result), hook.tool)
            }
            "postToolUseFailure" => {
                let hook: ToolFailureHook = decode("cursor", what, payload)?;
                Self::Tool(ToolPhase::Failure(hook.failure), hook.tool)
            }
            "afterAgentThought" => Self::AfterAgentThought(decode("cursor", what, payload)?),
            "afterAgentResponse" => Self::AfterAgentResponse(decode("cursor", what, payload)?),
            "stop" => Self::Stop(decode("cursor", what, payload)?),
            "sessionEnd" => Self::SessionEnd(decode("cursor", what, payload)?),
            "subagentStart" => Self::SubagentStart(decode("cursor", what, payload)?),
            "subagentStop" => Self::SubagentStop(decode("cursor", what, payload)?),
            "preCompact" => Self::PreCompact,
            "afterShellExecution" => {
                Self::Specialized(Specialized::Shell(decode("cursor", what, payload)?))
            }
            "afterMCPExecution" => {
                Self::Specialized(Specialized::Mcp(decode("cursor", what, payload)?))
            }
            "afterFileEdit" => {
                Self::Specialized(Specialized::FileEdit(decode("cursor", what, payload)?))
            }
            "beforeReadFile" => {
                Self::Specialized(Specialized::ReadFile(decode("cursor", what, payload)?))
            }
            IMPORT_START => Self::ImportStart(decode("cursor", what, payload)?),
            IMPORT_CHECKPOINT => Self::ImportCheckpoint,
            IMPORT_STOP => Self::ImportStop(decode("cursor", what, payload)?),
            _ => Self::Other,
        })
    }
}

pub(crate) const IMPORT_START: &str = "importStart";
pub(crate) const IMPORT_CHECKPOINT: &str = "importCheckpoint";
pub(crate) const IMPORT_STOP: &str = "importStop";

#[derive(Deserialize)]
pub(super) struct PromptHook {
    pub prompt: String,
}

pub(super) enum ToolPhase {
    Pre,
    Post(ToolResult),
    Failure(ToolFailure),
}

/// Fields every tool hook carries.
#[derive(Deserialize)]
pub(super) struct ToolHook {
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: Map<String, Value>,
    /// Sent by pre- and post-use hooks when the tool has a working directory.
    pub cwd: Option<String>,
    /// Set when the tool runs inside another tool call, such as a subagent.
    pub parent_tool_call_id: Option<String>,
}

#[derive(Deserialize)]
struct PostToolHook {
    #[serde(flatten)]
    tool: ToolHook,
    #[serde(flatten)]
    result: ToolResult,
}

#[derive(Deserialize)]
pub(super) struct ToolResult {
    pub tool_output: String,
    /// Native execution time in milliseconds.
    pub duration: f64,
}

#[derive(Deserialize)]
struct ToolFailureHook {
    #[serde(flatten)]
    tool: ToolHook,
    #[serde(flatten)]
    failure: ToolFailure,
}

#[derive(Deserialize)]
pub(super) struct ToolFailure {
    pub error_message: String,
    pub failure_type: String,
    pub is_interrupt: bool,
    /// Native execution time in milliseconds.
    pub duration: f64,
}

impl ToolPhase {
    pub fn duration(&self) -> Option<f64> {
        match self {
            Self::Pre => None,
            Self::Post(result) => Some(result.duration),
            Self::Failure(failure) => Some(failure.duration),
        }
    }
}

#[derive(Deserialize)]
pub(super) struct ThoughtHook {
    pub text: String,
}

#[derive(Deserialize)]
pub(super) struct ResponseHook {
    pub text: String,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

#[derive(Deserialize)]
pub(super) struct StopHook {
    pub status: String,
    #[serde(flatten)]
    pub usage: TurnUsage,
}

/// Token totals for one user generation, repeated on response and stop. The
/// CLI omits counts it does not know.
#[derive(Deserialize)]
pub(super) struct TurnUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}

#[derive(Deserialize)]
pub(super) struct SessionEndHook {
    pub reason: String,
    pub final_status: String,
    /// Documented as optional; the CLI does not send it.
    pub error_message: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct SubagentStartHook {
    pub subagent_id: String,
    pub subagent_type: String,
    pub task: String,
    /// The tool call that spawned the subagent.
    pub tool_call_id: Option<String>,
    pub subagent_model: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct SubagentStopHook {
    pub subagent_id: String,
    pub subagent_type: String,
    pub status: String,
    pub summary: Option<String>,
}

/// Hooks that carry a richer result for a tool already reported by the
/// generic tool hooks. None of them names the tool call; the CLI does not
/// send `tool_use_id` here, so it is matched only when present.
pub(super) enum Specialized {
    Shell(ShellHook),
    Mcp(McpHook),
    FileEdit(FileEditHook),
    ReadFile(ReadFileHook),
}

#[derive(Deserialize)]
pub(super) struct ShellHook {
    pub tool_use_id: Option<String>,
    pub command: String,
    pub output: String,
}

#[derive(Deserialize)]
pub(super) struct McpHook {
    pub tool_use_id: Option<String>,
    pub tool_name: String,
    /// The tool arguments, JSON-encoded.
    pub tool_input: String,
    /// The tool result, JSON-encoded.
    pub result_json: String,
}

#[derive(Deserialize)]
pub(super) struct FileEditHook {
    pub tool_use_id: Option<String>,
    pub file_path: String,
    pub edits: Vec<FileEdit>,
}

#[derive(Deserialize, Serialize)]
pub(super) struct FileEdit {
    pub old_string: String,
    pub new_string: String,
}

#[derive(Deserialize)]
pub(super) struct ReadFileHook {
    pub tool_use_id: Option<String>,
    pub file_path: String,
    /// File contents observed before the read.
    pub content: String,
}

impl Specialized {
    pub fn tool_use_id(&self) -> Option<&str> {
        match self {
            Self::Shell(hook) => hook.tool_use_id.as_deref(),
            Self::Mcp(hook) => hook.tool_use_id.as_deref(),
            Self::FileEdit(hook) => hook.tool_use_id.as_deref(),
            Self::ReadFile(hook) => hook.tool_use_id.as_deref(),
        }
    }

    /// Whether this hook can enrich a generic tool span with this name.
    pub fn accepts(&self, tool_name: &str) -> bool {
        match self {
            Self::Shell(_) => tool_name == "Shell",
            Self::Mcp(hook) => {
                tool_name.strip_prefix("MCP:").unwrap_or(tool_name) == hook.tool_name
            }
            Self::ReadFile(_) => tool_name == "Read",
            Self::FileEdit(_) => tool_name == "Write",
        }
    }

    /// Whether this hook describes the call with these generic tool inputs.
    pub fn matches_input(&self, input: &Value) -> bool {
        let file_path = || {
            input
                .get("file_path")
                .or_else(|| input.get("path"))
                .and_then(Value::as_str)
        };
        match self {
            Self::Shell(hook) => {
                input.get("command").and_then(Value::as_str) == Some(hook.command.as_str())
            }
            Self::Mcp(hook) => {
                let arguments = serde_json::from_str::<Value>(&hook.tool_input)
                    .unwrap_or_else(|_| Value::String(hook.tool_input.clone()));
                input == &arguments
            }
            Self::FileEdit(hook) => file_path() == Some(hook.file_path.as_str()),
            Self::ReadFile(hook) => file_path() == Some(hook.file_path.as_str()),
        }
    }

    /// The native result to record on the tool span. Reads report contents
    /// observed before the tool ran instead.
    pub fn result(&self) -> Option<Value> {
        match self {
            Self::Shell(hook) => Some(Value::String(hook.output.clone())),
            Self::Mcp(hook) => Some(Value::String(hook.result_json.clone())),
            Self::FileEdit(hook) => {
                Some(serde_json::to_value(&hook.edits).expect("file edits serialize"))
            }
            Self::ReadFile(_) => None,
        }
    }
}

/// `sessionStart` synthesized by transcript import.
#[derive(Deserialize)]
pub(super) struct ImportBoundary {
    /// Always true: a saved transcript has no session start time.
    pub start_time_estimated: bool,
}

/// `stop` synthesized by transcript import from the transcript's final
/// `turn_ended` marker, when it has one.
#[derive(Deserialize)]
pub(super) struct ImportStopHook {
    pub status: Option<String>,
    pub error_message: Option<String>,
}

/// One line of Cursor's saved transcript.
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum TranscriptRecord {
    Message {
        role: Role,
        message: TranscriptMessage,
    },
    /// Marks the end of a turn. The translator reads turn status from hooks,
    /// so only the marker's shape is checked.
    TurnEnded {
        #[serde(rename = "type")]
        _kind: TurnEndedKind,
        #[serde(rename = "status")]
        _status: String,
        #[serde(rename = "error", default)]
        _error: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Role {
    User,
    Assistant,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Deserialize)]
pub(super) struct TranscriptMessage {
    pub content: MessageContent,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TurnEndedKind {
    TurnEnded,
}

/// Message content: plain text, or content blocks.
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ContentBlock {
    Text {
        text: String,
    },
    /// Tool requests are reported by hooks; transcripts add nothing to them.
    ToolUse,
}

impl MessageContent {
    /// The concatenated text of the content.
    pub fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Blocks(blocks) => blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    ContentBlock::ToolUse => None,
                })
                .collect(),
        }
    }
}

impl TranscriptRecord {
    pub fn decode(record: &Value) -> Result<Self, DecodeError> {
        decode("cursor", "transcript record", record)
    }

    pub fn role(&self) -> Option<Role> {
        match self {
            Self::Message { role, .. } => Some(*role),
            Self::TurnEnded { .. } => None,
        }
    }

    pub fn text(&self) -> Option<String> {
        match self {
            Self::Message { message, .. } => Some(message.content.text()),
            Self::TurnEnded { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn specialized_hooks_require_their_kind_s_fields() {
        let common = json!({"generation_id":"g","model":"m","workspace_roots":[]});
        let mut shell = common.clone();
        shell["command"] = json!("ls");
        let error = Hook::decode("afterShellExecution", &shell)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("missing field `output`"), "{error}");
        shell["output"] = json!("a.txt");
        assert!(Hook::decode("afterShellExecution", &shell).is_ok());
    }
}
