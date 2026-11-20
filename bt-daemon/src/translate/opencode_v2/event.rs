//! Partial OpenCode 2 wire types. Unknown fields are accepted; arbitrary tool
//! inputs, outputs and provider-specific content remain JSON values.
pub(super) use crate::agents::usage::OpenCodeTokens as Tokens;
use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct NativeEnvelope {
    pub id: Option<String>,
    pub location: Option<Location>,
    pub metadata: Option<EventMetadata>,
    #[serde(flatten)]
    pub event: NativeEvent,
}
#[derive(Deserialize)]
pub(super) struct Location {
    pub directory: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct EventMetadata {
    #[serde(rename = "keepID")]
    pub keep_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "data")]
pub(super) enum NativeEvent {
    #[serde(rename = "session.created")]
    Created(Created),
    #[serde(rename = "session.inbox.enqueued")]
    Enqueued(Enqueued),
    #[serde(rename = "session.inbox.delivered")]
    Delivered(Inbox),
    #[serde(rename = "session.inbox.cancelled")]
    Cancelled(Inbox),
    #[serde(rename = "model.context")]
    Context(Context),
    #[serde(rename = "session.step.started")]
    StepStarted(StepStarted),
    #[serde(rename = "session.step.ended")]
    StepEnded(StepEnded),
    #[serde(rename = "session.step.failed")]
    StepFailed(StepEnded),
    #[serde(rename = "session.text.ended")]
    TextEnded(TextEnded),
    #[serde(rename = "session.reasoning.ended")]
    ReasoningEnded(TextEnded),
    #[serde(rename = "session.tool.input.started")]
    ToolInputStarted(ToolInputStarted),
    #[serde(rename = "session.tool.called")]
    ToolCalled(ToolCalled),
    #[serde(rename = "session.tool.success")]
    ToolSuccess(ToolSuccess),
    #[serde(rename = "session.tool.failed")]
    ToolFailed(ToolFailed),
    #[serde(rename = "permission.asked")]
    PermissionAsked(PermissionAsked),
    #[serde(rename = "permission.replied")]
    PermissionReplied(PermissionReplied),
    #[serde(rename = "session.compaction.started")]
    CompactionStarted(CompactionStarted),
    #[serde(rename = "session.compaction.ended")]
    CompactionEnded(CompactionEnded),
    #[serde(rename = "session.compaction.failed")]
    CompactionFailed(CompactionFailed),
    #[serde(rename = "session.execution.succeeded")]
    Succeeded(Session),
    #[serde(rename = "session.execution.failed")]
    Failed(ExecutionFailed),
    #[serde(rename = "session.execution.interrupted")]
    Interrupted(Interrupted),
    #[serde(rename = "session.deleted")]
    Deleted(Session),
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
pub(super) struct Session {
    #[serde(rename = "sessionID")]
    pub session_id: String,
}
#[derive(Deserialize)]
pub(super) struct Created {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "parentID")]
    pub parent_id: Option<String>,
    pub title: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct Inbox {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "inboxID")]
    pub inbox_id: String,
}
#[derive(Deserialize)]
pub(super) struct Enqueued {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "inboxID")]
    pub inbox_id: String,
    pub item: InboxItem,
}
#[derive(Deserialize)]
#[serde(tag = "type")]
pub(super) enum InboxItem {
    #[serde(rename = "user")]
    User { payload: Prompt },
    #[serde(other)]
    Other,
}
#[derive(Deserialize, Serialize)]
pub(super) struct Prompt {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub files: Vec<PromptFile>,
    #[serde(default)]
    pub skills: Vec<PromptSkill>,
}
#[derive(Deserialize, Serialize)]
pub(super) struct PromptSkill {
    pub name: String,
}
#[derive(Deserialize, Serialize)]
pub(super) struct PromptFile {
    pub mime: String,
    pub data: String,
}
#[derive(Clone, Default, Deserialize, Serialize)]
pub(super) struct Model {
    pub id: Option<String>,
    #[serde(rename = "providerID")]
    pub provider_id: Option<String>,
    pub variant: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct StepStarted {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub started: Option<i64>,
    pub model: Option<Model>,
}
#[derive(Deserialize)]
pub(super) struct StepEnded {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub tokens: Option<Tokens>,
    pub error: Option<NativeError>,
}
#[derive(Deserialize)]
pub(super) struct TextEnded {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub ordinal: Option<u64>,
    pub text: String,
}
#[derive(Deserialize)]
pub(super) struct ToolInputStarted {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: Option<String>,
    pub id: String,
    pub name: String,
}
#[derive(Deserialize)]
pub(super) struct ToolCalled {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub id: String,
    pub input: Value,
}
#[derive(Deserialize)]
pub(super) struct ToolSuccess {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub id: String,
    pub content: Option<Vec<ToolContent>>,
}
#[derive(Deserialize)]
pub(super) struct ToolFailed {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "assistantMessageID")]
    pub message_id: String,
    pub id: String,
    pub error: NativeError,
}
#[derive(Deserialize, Serialize)]
pub(super) struct NativeError {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: String,
    pub status: Option<u16>,
    pub response: Option<ErrorResponse>,
    #[serde(rename = "retryAfterMs")]
    pub retry_after_ms: Option<u64>,
}
#[derive(Deserialize, Serialize)]
pub(super) struct ErrorResponse {
    pub body: String,
}
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
#[derive(Deserialize)]
pub(super) struct PermissionAsked {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub id: String,
    pub action: String,
    pub source: Option<PermissionSource>,
}
#[derive(Deserialize)]
pub(super) struct PermissionSource {
    pub id: String,
    #[serde(rename = "messageID")]
    pub message_id: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct PermissionReplied {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "requestID")]
    pub request_id: String,
    pub reply: String,
}
#[derive(Deserialize)]
pub(super) struct CompactionStarted {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "inputID")]
    pub input_id: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct CompactionEnded {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub text: String,
    pub model: Option<Model>,
    pub tokens: Option<Tokens>,
    pub native: Option<NativeCompaction>,
    #[serde(rename = "providerContext")]
    pub provider_context: Option<NativeCompaction>,
}
#[derive(Deserialize)]
pub(super) struct NativeCompaction {
    pub messages: Option<NativeMessages>,
}
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum NativeMessages {
    Messages(Vec<NativeContextMessage>),
    Opaque(Value),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeContextMessage {
    pub role: String,
    pub content: MessageContent,
}
#[derive(Deserialize)]
pub(super) struct CompactionFailed {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub error: NativeError,
    pub tokens: Option<Tokens>,
}
#[derive(Deserialize)]
pub(super) struct ExecutionFailed {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub error: NativeError,
}
#[derive(Deserialize)]
pub(super) struct Interrupted {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub reason: Option<String>,
}

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
