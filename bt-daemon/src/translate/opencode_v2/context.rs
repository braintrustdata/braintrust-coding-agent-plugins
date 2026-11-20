//! Native OpenCode context and its conversion to Braintrust messages.
use super::event::ToolContent;
use super::span::{Content, ContentPart, Message, ToolCall};
use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
pub(super) struct Context {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(default)]
    pub system: Vec<String>,
    #[serde(default)]
    pub messages: Vec<ContextMessage>,
}
#[derive(Deserialize, Serialize)]
pub(super) struct ContextMessage {
    pub role: String,
    pub content: MessageContent,
}
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum MessageContent {
    Text(String),
    Parts(Vec<ContextPart>),
    Other(Value),
}
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum ContextPart {
    Known(KnownContextPart),
    Other(Value),
}
// Select the known schema before deserializing it. An untagged fallback
// must not turn a malformed known media or tool part into arbitrary JSON.
impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(text) => Ok(Self::Text(text)),
            Value::Array(parts) => serde_json::from_value(Value::Array(parts))
                .map(Self::Parts)
                .map_err(serde::de::Error::custom),
            other => Ok(Self::Other(other)),
        }
    }
}
impl<'de> Deserialize<'de> for ContextPart {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if matches!(
            value.get("type").and_then(Value::as_str),
            Some("text" | "media" | "tool-call" | "tool-result")
        ) {
            serde_json::from_value(value)
                .map(Self::Known)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Self::Other(value))
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(super) enum KnownContextPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "media")]
    Media {
        #[serde(
            default,
            rename = "mediaType",
            skip_serializing_if = "Field::is_missing"
        )]
        media_type: Field<String>,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        data: Field<String>,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        media: Field<Media>,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extra: Extensions,
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
#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Media {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub source: Field<MediaSource>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
#[derive(Clone, Deserialize, Serialize)]
pub(super) struct MediaSource {
    #[serde(
        default,
        rename = "mediaType",
        skip_serializing_if = "Field::is_missing"
    )]
    pub media_type: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub data: Field<String>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extra: Extensions,
}
#[derive(Clone, Deserialize, Serialize)]
pub(super) struct ToolResult {
    pub value: Value,
}

impl Context {
    pub(super) fn into_messages(self) -> Vec<Message> {
        let mut messages = Vec::new();
        let system = self.system.join("\n\n");
        if !system.is_empty() {
            messages.push(Message::new("system", Content::Text(system)));
        }
        for message in self.messages {
            match message.content {
                MessageContent::Text(text) => {
                    messages.push(Message::new(message.role, Content::Text(text)))
                }
                MessageContent::Other(content) => {
                    messages.push(Message::new(message.role, Content::Dynamic(content)))
                }
                MessageContent::Parts(parts) => {
                    let mut content = Vec::new();
                    let mut calls = Vec::new();
                    let mut single_text = None;
                    for part in parts {
                        match part {
                            ContextPart::Known(KnownContextPart::Text { text }) => {
                                single_text = Some(text.clone());
                                content.push(ContentPart::text(text));
                            }
                            ContextPart::Known(KnownContextPart::Media {
                                media_type,
                                data,
                                media,
                                extra,
                            }) => {
                                let source = media.as_ref().and_then(|media| media.source.as_ref());
                                let mime = source
                                    .and_then(|source| source.media_type.as_deref())
                                    .or(media_type.as_deref());
                                let bytes = source
                                    .and_then(|source| source.data.as_deref())
                                    .or(data.as_deref());
                                if let Some((mime, bytes)) = mime
                                    .zip(bytes)
                                    .filter(|(mime, _)| mime.starts_with("image/"))
                                {
                                    content.push(ContentPart::image(format!(
                                        "data:{mime};base64,{bytes}"
                                    )));
                                } else {
                                    content.push(ContentPart::Native(KnownContextPart::Media {
                                        media_type,
                                        data,
                                        media,
                                        extra,
                                    }));
                                }
                            }
                            ContextPart::Known(KnownContextPart::ToolCall { id, name, input }) => {
                                calls.push(ToolCall::new(id, name, &input))
                            }
                            ContextPart::Known(KnownContextPart::ToolResult { id, result }) => {
                                let value = match serde_json::from_value::<Vec<ToolContent>>(
                                    result.value.clone(),
                                ) {
                                    Ok(parts) => Content::Parts(
                                        parts
                                            .into_iter()
                                            .map(ToolContent::into_span_part)
                                            .collect(),
                                    ),
                                    Err(_) => Content::Dynamic(result.value),
                                };
                                messages.push(Message::tool(id, value));
                            }
                            ContextPart::Other(value) => content.push(ContentPart::Dynamic(value)),
                        }
                    }
                    if message.role == "tool" && content.is_empty() && calls.is_empty() {
                        continue;
                    }
                    let value = if content.is_empty() {
                        Content::Text(String::new())
                    } else if content.len() == 1 && single_text.is_some() {
                        Content::Text(single_text.unwrap_or_default())
                    } else {
                        Content::Parts(content)
                    };
                    let mut normalized = Message::new(message.role, value);
                    normalized.tool_calls = calls;
                    messages.push(normalized);
                }
            }
        }
        messages
    }
}

#[cfg(test)]
mod extension_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn media_extensions_round_trip_but_known_fields_remain_typed() {
        let raw = json!([{"type":"media","media":{"source":{"mediaType":"application/pdf","data":"YWJj","future":true},"future":null},"future":[42]}]);
        let parsed: MessageContent = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
        assert!(serde_json::from_value::<MessageContent>(json!([
            {"type":"media","media":{"source":{"data":42}}}
        ]))
        .is_err());
        let future = json!([{"type":"future","payload":{"data":42}}]);
        let parsed: MessageContent = serde_json::from_value(future.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), future);
    }
}
