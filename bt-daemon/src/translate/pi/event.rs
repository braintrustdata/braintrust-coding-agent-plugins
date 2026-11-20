//! Partial Pi callback payloads. Correlation remains in the parent reducer.
use serde::{Deserialize, Serialize};
use serde_json::Value;

// These are deliberately partial native event types. Serde ignores fields we
// do not model, while `Value` remains at the user-controlled JSON boundaries
// that we forward without interpreting (messages, tool arguments, results).
// Keeping this vocabulary local lets the raw journal remain forward-compatible
// and makes each translator reducer explicit about the fields it consumes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct BeforeAgentStart {
    #[serde(default)]
    pub(super) prompt: Option<Value>,
    #[serde(default)]
    pub(super) message: Option<Value>,
    #[serde(default)]
    pub(super) images: Option<Vec<PromptImage>>,
}

/// Pi's before_agent_start images contain base64 data, separate from the prompt.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PromptImage {
    pub(super) data: String,
    pub(super) mime_type: String,
}

/// `ctx.model`: the selected model, which may be a Pi 1.0 virtual model that
/// routes each request to a physical model.
#[derive(Deserialize, Serialize)]
pub(super) struct SelectedModel {
    pub(super) provider: String,
    pub(super) id: String,
}

#[derive(Deserialize, Serialize)]
pub(super) struct ContextEvent {
    #[serde(default)]
    pub(super) messages: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct MessageUpdate {
    #[serde(default)]
    pub(super) assistant_message_event: Option<MessageDelta>,
    #[serde(default, rename = "type")]
    pub(super) kind: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(super) struct MessageDelta {
    #[serde(default, rename = "type")]
    pub(super) kind: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(super) struct ThinkingLevel {
    #[serde(default)]
    pub(super) level: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct ToolExecutionStart {
    pub(super) tool_call_id: String,
    #[serde(default)]
    pub(super) tool_name: Option<String>,
    #[serde(default)]
    pub(super) args: Value,
    // Pi 1.0 sets this on calls another tool made through
    // `ctx.executeTool()`, such as codemode's `tools.<name>()`.
    #[serde(default)]
    pub(super) parent_tool_call_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct ToolExecutionEnd {
    #[serde(default)]
    pub(super) tool_call_id: Option<String>,
    #[serde(default)]
    pub(super) tool_name: Option<String>,
    #[serde(default)]
    pub(super) result: Option<Value>,
    #[serde(default)]
    pub(super) is_error: bool,
    #[serde(default)]
    pub(super) parent_tool_call_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct AgentEnd {
    #[serde(default)]
    pub(super) will_retry: bool,
}
