//! Concrete span data produced from OpenCode 2 events.
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Serialize, serde::Deserialize)]
#[serde(untagged)]
pub(super) enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
    Dynamic(Value),
}
#[derive(Clone, Serialize)]
#[serde(untagged)]
pub(super) enum ContentPart {
    Text(TextPart),
    Image(ImagePart),
    Native(super::event::KnownContextPart),
    Tool(super::event::ToolContent),
    Dynamic(Value),
}
/// Parts are only ever serialized after construction, so saved translator
/// state restores each one exactly as it was written. Matching the untagged
/// variants instead would turn, for example, a reasoning part with provider
/// options into a plain text part and drop its other fields.
impl<'de> serde::Deserialize<'de> for ContentPart {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Value::deserialize(deserializer).map(Self::Dynamic)
    }
}
#[derive(Clone, Serialize)]
pub(super) struct TextPart {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}
#[derive(Clone, Serialize)]
pub(super) struct ImagePart {
    #[serde(rename = "type")]
    pub kind: String,
    pub image_url: ImageUrl,
}
#[derive(Clone, Serialize)]
pub(super) struct ImageUrl {
    pub url: String,
}
impl ContentPart {
    pub fn text(text: String) -> Self {
        Self::Text(TextPart {
            kind: "text".into(),
            text,
        })
    }
    pub fn image(url: String) -> Self {
        Self::Image(ImagePart {
            kind: "image_url".into(),
            image_url: ImageUrl { url },
        })
    }
}
#[derive(Clone, Serialize, serde::Deserialize)]
pub(super) struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionCall,
}
#[derive(Clone, Serialize, serde::Deserialize)]
pub(super) struct FunctionCall {
    pub name: String,
    pub arguments: String,
}
impl ToolCall {
    pub fn new(id: String, name: String, input: &Value) -> Self {
        Self {
            id,
            kind: "function".into(),
            function: FunctionCall {
                name,
                arguments: input.to_string(),
            },
        }
    }
}
#[derive(Clone, Serialize, serde::Deserialize)]
pub(super) struct Reasoning {
    pub id: String,
    pub content: String,
}
#[derive(Clone, Serialize, serde::Deserialize)]
pub(super) struct Message {
    pub role: String,
    pub content: Content,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Vec<Reasoning>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
}
impl Message {
    pub fn new(role: impl Into<String>, content: Content) -> Self {
        Self {
            role: role.into(),
            content,
            tool_calls: vec![],
            reasoning: None,
            tool_call_id: None,
            message_type: None,
        }
    }
    pub fn tool(id: String, content: Content) -> Self {
        let mut message = Self::new("tool", content);
        message.tool_call_id = Some(id);
        message
    }
    pub fn summary(text: String) -> Self {
        let mut message = Self::new("assistant", Content::Text(text));
        message.message_type = Some("compaction_summary".into());
        message
    }
}
#[derive(Serialize)]
pub(super) struct RootMetadata<'a> {
    pub session_id: &'a str,
    pub source: &'static str,
    #[serde(rename = "_bt_defer_root")]
    pub defer_root: bool,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_subagent: Option<bool>,
}
#[derive(Serialize)]
pub(super) struct TurnMetadata {
    pub turn_number: u32,
    pub model: Option<String>,
    pub loaded_skill_names: Vec<String>,
}
#[derive(Serialize)]
pub(super) struct RootTotals {
    pub total_turns: u32,
    pub total_tool_calls: u32,
}
#[derive(Serialize)]
pub(super) struct ToolMetadata<'a> {
    pub tool_name: &'a str,
    pub call_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_approval: Option<super::super::tool::ToolApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_name: Option<Option<&'a str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_path: Option<&'a str>,
    /// Set only when the user requested the skill; absent means model-chosen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_load_trigger: Option<&'static str>,
}
impl<'a> ToolMetadata<'a> {
    pub fn new(tool_name: &'a str, call_id: &'a str) -> Self {
        Self {
            tool_name,
            call_id,
            tool_approval: None,
            permission_id: None,
            permission_type: None,
            tool_kind: None,
            skill_name: None,
            skill_path: None,
            skill_load_trigger: None,
        }
    }
}
#[derive(Serialize)]
pub(super) struct LlmMetadata<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub message_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<&'a str>,
}
#[derive(Serialize)]
pub(super) struct Metrics {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub tokens: i64,
    pub prompt_cached_tokens: i64,
    pub prompt_cache_creation_tokens: i64,
    pub reasoning_tokens: i64,
}
impl Metrics {
    pub fn from_tokens(t: super::event::Tokens) -> Self {
        let prompt = t.input + t.cache.read + t.cache.write;
        Self {
            prompt_tokens: prompt,
            completion_tokens: t.output,
            tokens: prompt + t.output + t.reasoning,
            prompt_cached_tokens: t.cache.read,
            prompt_cache_creation_tokens: t.cache.write,
            reasoning_tokens: t.reasoning,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saved_content_parts_restore_every_field() {
        let parts = vec![
            ContentPart::text("prompt".into()),
            ContentPart::Dynamic(serde_json::json!({
                "type": "reasoning", "text": "thinking", "providerOptions": {"effort": "high"}
            })),
        ];
        let saved = serde_json::to_value(&parts).unwrap();
        let restored: Vec<ContentPart> = serde_json::from_value(saved.clone()).unwrap();
        assert_eq!(serde_json::to_value(&restored).unwrap(), saved);
    }
}
