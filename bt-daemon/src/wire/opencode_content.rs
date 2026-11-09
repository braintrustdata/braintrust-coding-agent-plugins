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
