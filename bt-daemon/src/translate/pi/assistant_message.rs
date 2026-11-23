//! Pi assistant messages own physical-model selection and span-message projection.
use crate::agents::usage::PiUsage as Usage;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct AssistantMessage {
    #[serde(default)]
    pub(super) role: Option<String>,
    #[serde(default)]
    pub(super) provider: Option<String>,
    #[serde(default)]
    pub(super) api: Option<String>,
    #[serde(default)]
    response_model: Option<String>,
    #[serde(default)]
    routed_model: Option<String>,
    #[serde(default)]
    resolved_model: Option<String>,
    #[serde(default)]
    actual_model: Option<String>,
    #[serde(default)]
    concrete_model: Option<String>,
    #[serde(default)]
    output_model: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    pub(super) usage: Usage,
    #[serde(default)]
    content: Vec<AssistantContent>,
    #[serde(default)]
    pub(super) error_message: Option<String>,
    #[serde(default)]
    pub(super) stop_reason: Option<String>,
    #[serde(default)]
    pub(super) response_id: Option<String>,
    // Pi 1.0 records the level dispatched for this response, which a virtual
    // model's router may choose independently of the selected level.
    #[serde(default)]
    pub(super) thinking_level: Option<String>,
    #[serde(default)]
    pub(super) provider_thinking_level: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[derive(Serialize)]
enum AssistantContent {
    #[serde(rename = "text")]
    Text {
        #[serde(default)]
        text: Option<String>,
    },
    #[serde(rename = "thinking")]
    Thinking {
        #[serde(default)]
        thinking: Option<String>,
    },
    #[serde(rename = "toolCall")]
    ToolCall {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        arguments: Option<Value>,
    },
    #[serde(other)]
    Other,
}

impl AssistantMessage {
    /// Compare the native provider/model identity, independently of any routed
    /// response-model aliases used for display metadata.
    pub(super) fn matches_model(&self, provider: &str, id: &str) -> bool {
        self.provider.as_deref() == Some(provider) && self.model.as_deref() == Some(id)
    }

    pub(super) fn response_model(&self) -> Option<String> {
        [
            &self.response_model,
            &self.routed_model,
            &self.resolved_model,
            &self.actual_model,
            &self.concrete_model,
            &self.output_model,
            &self.model,
        ]
        .into_iter()
        .flatten()
        .next()
        .cloned()
    }
    pub(super) fn normalized(&self) -> Value {
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        for part in &self.content {
            match part {
                AssistantContent::Text {
                    text: Some(content),
                } => text.push_str(content),
                AssistantContent::Thinking {
                    thinking: Some(content),
                } => reasoning.push_str(content),
                AssistantContent::ToolCall {
                    id,
                    name,
                    arguments,
                } => calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(
                            arguments.as_ref().unwrap_or(&Value::Null),
                        )
                        .unwrap_or_default(),
                    },
                })),
                _ => {}
            }
        }
        let mut out = json!({"role":"assistant","content":text});
        if !reasoning.is_empty() {
            out["reasoning"] = json!([{"id":"reasoning","content":reasoning}])
        }
        if !calls.is_empty() {
            out["tool_calls"] = Value::Array(calls)
        }
        out
    }
}
