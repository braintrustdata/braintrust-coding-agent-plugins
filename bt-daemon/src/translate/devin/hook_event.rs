use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct HookEvent {
    pub prompt_id: Option<String>,
    // Added at capture from Devin's DEVIN_PROJECT_DIR environment variable.
    pub cwd: Option<String>,
    #[serde(flatten)]
    pub kind: HookKind,
}

#[derive(Deserialize)]
#[serde(tag = "hook_event_name")]
pub(super) enum HookKind {
    SessionStart,
    UserPromptSubmit {
        prompt: String,
    },
    PreToolUse(ToolCall),
    PostToolUse {
        #[serde(flatten)]
        tool: ToolCall,
        tool_response: ToolResponse,
    },
    PermissionRequest(ToolCall),
    Stop {
        last_assistant_message: Option<String>,
    },
    PostCompaction {
        summary: Option<String>,
    },
    SessionEnd,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
pub(super) struct ToolCall {
    pub tool_name: String,
    pub tool_use_id: Option<String>,
    // Each built-in or MCP tool controls its own argument schema.
    pub tool_input: Option<Value>,
}

#[derive(Deserialize)]
pub(super) struct ToolResponse {
    pub success: Option<bool>,
    pub output: Option<String>,
    pub error: Option<String>,
}
