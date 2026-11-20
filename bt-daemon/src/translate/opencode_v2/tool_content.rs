//! Native tool result blocks own their span-content conversion.
use super::span::ContentPart;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(super) enum ToolContent {
    Known(KnownToolContent),
    Other(Value),
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(super) enum KnownToolContent {
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
impl ToolContent {
    pub(super) fn into_span_part(self) -> ContentPart {
        match self {
            Self::Known(KnownToolContent::File { mime, uri, .. }) if mime.starts_with("image/") => {
                ContentPart::image(uri)
            }
            other => ContentPart::Tool(other),
        }
    }
}
