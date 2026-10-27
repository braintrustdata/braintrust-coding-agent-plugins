//! Native hook message and provider payload contracts.
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// These values have schemas owned by the tool or provider, not by Muse tracing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolInput(pub Value);
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolOutput(pub Value);
pub(crate) type ProviderOptions = BTreeMap<String, Value>;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ModelOutput {
    Text(String),
    Messages(Vec<NativeMessage>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NativeMessage {
    pub role: MessageRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<AssistantToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub(crate) enum MessageRole {
    System,
    Developer,
    User,
    Assistant,
    Tool,
    Other(String),
}
impl From<String> for MessageRole {
    fn from(s: String) -> Self {
        match s.as_str() {
            "system" => Self::System,
            "developer" => Self::Developer,
            "user" => Self::User,
            "assistant" => Self::Assistant,
            "tool" => Self::Tool,
            _ => Self::Other(s),
        }
    }
}
impl From<MessageRole> for String {
    fn from(role: MessageRole) -> Self {
        match role {
            MessageRole::System => "system",
            MessageRole::Developer => "developer",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
            MessageRole::Other(s) => return s,
        }
        .into()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub(crate) enum ContentBlock {
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(flatten)]
        extensions: BTreeMap<String, Value>,
    },
    #[serde(rename = "assistant_message")]
    AssistantMessage {
        text: String,
        #[serde(flatten)]
        extensions: BTreeMap<String, Value>,
    },
    #[serde(untagged)]
    Other {
        #[serde(rename = "type")]
        kind: String,
        #[serde(flatten)]
        fields: BTreeMap<String, Value>,
    },
}
impl<'de> Deserialize<'de> for ContentBlock {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Dispatch first so a malformed known block cannot hide in the fallback.
        #[derive(Deserialize)]
        struct Tag {
            #[serde(rename = "type")]
            kind: String,
        }
        #[derive(Deserialize)]
        struct Text {
            text: String,
            #[serde(rename = "type")]
            _kind: String,
            #[serde(flatten)]
            extensions: BTreeMap<String, Value>,
        }
        let value = Value::deserialize(d)?;
        let tag: Tag = serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
        match tag.kind.as_str() {
            "text" | "assistant_message" => {
                let body: Text = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(if tag.kind == "text" {
                    Self::Text {
                        text: body.text,
                        extensions: body.extensions,
                    }
                } else {
                    Self::AssistantMessage {
                        text: body.text,
                        extensions: body.extensions,
                    }
                })
            }
            _ => {
                let mut fields: BTreeMap<String, Value> =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                fields.remove("type");
                Ok(Self::Other {
                    kind: tag.kind,
                    fields,
                })
            }
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AssistantToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: FunctionCallType,
    pub function: FunctionCall,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FunctionCallType {
    Function,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FunctionCall {
    pub name: String,
    pub arguments: ToolArguments,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ToolArguments {
    Encoded(String),
    Json(Value),
}
impl ToolArguments {
    pub fn input(&self) -> ToolInput {
        ToolInput(match self {
            Self::Encoded(s) => {
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
            }
            Self::Json(v) => v.clone(),
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ToolDescription {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_parameters: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct InputSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_message_count: Option<u64>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
