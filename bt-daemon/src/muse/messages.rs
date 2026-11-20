//! Native hook message and provider payload contracts.
pub(crate) use super::provider_options::ProviderOptions;
use crate::json::{Extensions, Field};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// These values have schemas owned by the tool or provider, not by Muse tracing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolInput(pub Value);
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolOutput(pub Value);
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
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extensions: Extensions,
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
/// Known content variants are validated before the unknown-content fallback.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ContentBlock {
    Known(KnownContentBlock),
    Other {
        #[serde(rename = "type")]
        kind: String,
        #[serde(flatten)]
        fields: Extensions,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum KnownContentBlock {
    Text {
        text: String,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    AssistantMessage {
        text: String,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        provider_item_id: Field<String>,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        phase: Field<MessagePhase>,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    Image {
        source: ImageSource,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    ImageUrl {
        image_url: ImageUrl,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MessagePhase {
    Analysis,
    Commentary,
    FinalAnswer,
    #[serde(untagged)]
    Other(String),
}
impl<'de> Deserialize<'de> for ContentBlock {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Tag {
            #[serde(rename = "type")]
            kind: String,
        }
        let value = Value::deserialize(d)?;
        let tag: Tag = serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
        if matches!(
            tag.kind.as_str(),
            "text" | "assistant_message" | "image" | "image_url"
        ) {
            serde_json::from_value(value)
                .map(Self::Known)
                .map_err(serde::de::Error::custom)
        } else {
            let mut fields: Extensions =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            fields.remove("type");
            Ok(Self::Other {
                kind: tag.kind,
                fields,
            })
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ImageSource {
    Base64(Base64ImageSource),
    Url(UrlImageSource),
    /// Future source formats survive unchanged, without becoming known data.
    Other(Box<serde_json::value::RawValue>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Base64ImageSource {
    #[serde(rename = "type")]
    kind: Base64Type,
    media_type: String,
    data: String,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Base64Type {
    #[serde(rename = "base64")]
    Base64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UrlImageSource {
    url: String,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
impl<'de> Deserialize<'de> for ImageSource {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom("image source must be an object"));
        }
        match value.get("type").and_then(Value::as_str) {
            Some("base64") => serde_json::from_value(value)
                .map(Self::Base64)
                .map_err(serde::de::Error::custom),
            kind @ (None | Some("url")) if kind.is_some() || value.get("url").is_some() => {
                serde_json::from_value(value)
                    .map(Self::Url)
                    .map_err(serde::de::Error::custom)
            }
            _ => serde_json::value::to_raw_value(&value)
                .map(Self::Other)
                .map_err(serde::de::Error::custom),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ImageUrl {
    Url(String),
    Detailed {
        url: String,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        detail: Field<ImageDetail>,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ImageDetail {
    Auto,
    Low,
    High,
    #[serde(untagged)]
    Other(String),
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
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extensions: Extensions,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct InputSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_message_count: Option<u64>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    pub extensions: Extensions,
}

#[cfg(test)]
mod extension_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn known_assistant_fields_and_image_variants_round_trip() {
        for raw in [
            json!({"type":"assistant_message","text":"done","provider_item_id":null,"phase":"final_answer","future":true}),
            json!({"type":"assistant_message","text":"done","phase":"future-phase"}),
            json!({"type":"image","source":{"url":"https://example.com/image","future":null},"future":true}),
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"YWJj","future":true}}),
            json!({"type":"image_url","image_url":{"url":"https://example.com/image","detail":"high","future":true}}),
            json!({"type":"image","source":{"type":"future","opaque":[null,42]}}),
            json!({"type":"future","opaque":[null,42]}),
        ] {
            let parsed: ContentBlock = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
        }
        let parsed: ContentBlock = serde_json::from_value(
            json!({"type":"assistant_message","text":"done","phase":"final_answer"}),
        )
        .unwrap();
        assert!(matches!(
            parsed,
            ContentBlock::Known(KnownContentBlock::AssistantMessage {
                phase: Field::Value(MessagePhase::FinalAnswer),
                ..
            })
        ));
    }

    #[test]
    fn malformed_known_fields_cannot_hide_as_unknown_content() {
        for raw in [
            json!({"type":"image","source":42}),
            json!({"type":"assistant_message","text":"done","provider_item_id":42}),
            json!({"type":"assistant_message","text":"done","phase":42}),
            json!({"type":"image","source":{"url":42}}),
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":42}}),
            json!({"type":"image_url","image_url":{"url":"example","detail":42}}),
        ] {
            assert!(serde_json::from_value::<ContentBlock>(raw).is_err());
        }
    }
}
