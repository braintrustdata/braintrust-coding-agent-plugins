//! Shared concrete OpenCode context and provider content types.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct NativeCompaction {
    pub messages: Option<NativeMessages>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum NativeMessages {
    Messages(Vec<NativeContextMessage>),
    Opaque(Value),
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeContextMessage {
    pub role: String,
    pub content: MessageContent,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ContextMessage {
    pub role: String,
    pub content: MessageContent,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum MessageContent {
    Text(String),
    Parts(Vec<ContextPart>),
    Other(Value),
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum ContextPart {
    Known(KnownContextPart),
    Other(Value),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(crate) enum KnownContextPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "media")]
    Media {
        #[serde(rename = "mediaType")]
        media_type: Option<String>,
        data: Option<String>,
        media: Option<Media>,
        #[serde(flatten)]
        extra: serde_json::Map<String, Value>,
    },
    #[serde(rename = "tool-call")]
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(rename = "tool-result")]
    ToolResult { id: String, result: ToolResult },
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Media {
    pub source: Option<MediaSource>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct MediaSource {
    #[serde(rename = "mediaType")]
    pub media_type: Option<String>,
    pub data: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ToolResult {
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct NativeError {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: String,
    pub status: Option<u16>,
    pub response: Option<ErrorResponse>,
    #[serde(rename = "retryAfterMs")]
    pub retry_after_ms: Option<u64>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ErrorResponse {
    pub body: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum ToolContent {
    Known(KnownToolContent),
    Other(Value),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(crate) enum KnownToolContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "file")]
    File {
        mime: String,
        uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Prompt {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub files: Vec<PromptFile>,
    #[serde(default)]
    pub skills: Vec<PromptSkill>,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct PromptSkill {
    pub name: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct PromptFile {
    pub mime: String,
    pub data: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Model {
    pub id: Option<String>,
    #[serde(rename = "providerID")]
    pub provider_id: Option<String>,
    pub variant: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct Tokens {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    #[serde(default)]
    pub reasoning: i64,
    #[serde(default)]
    pub cache: CacheTokens,
}
#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CacheTokens {
    #[serde(default)]
    pub read: i64,
    #[serde(default)]
    pub write: i64,
}
