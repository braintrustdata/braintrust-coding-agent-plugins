//! Partial OpenCode 2 wire types. Unknown fields are accepted; arbitrary tool
//! inputs, outputs and provider-specific content remain JSON values.
pub(super) use crate::wire::opencode_content::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct NativeEnvelope {
    pub id: Option<String>,
    pub location: Option<Location>,
    pub metadata: Option<EventMetadata>,
    pub durable: Option<DurableEvent>,
    #[serde(flatten)]
    pub event: NativeEvent,
}
#[derive(Deserialize)]
pub(super) struct DurableEvent {
    pub seq: u64,
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
    #[serde(rename = "session.snapshot")]
    Snapshot(crate::wire::opencode_snapshot::Snapshot),
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
#[derive(Deserialize)]
pub(super) struct Prompt {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub files: Vec<PromptFile>,
    #[serde(default)]
    pub skills: Vec<PromptSkill>,
}
#[derive(Deserialize)]
pub(super) struct PromptSkill {
    pub name: String,
}
#[derive(Deserialize)]
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
#[derive(Default, Deserialize, Serialize)]
pub(super) struct Tokens {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    #[serde(default)]
    pub reasoning: i64,
    #[serde(default)]
    pub cache: CacheTokens,
}
#[derive(Default, Deserialize, Serialize)]
pub(super) struct CacheTokens {
    #[serde(default)]
    pub read: i64,
    #[serde(default)]
    pub write: i64,
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

#[derive(Deserialize)]
pub(super) struct Context {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(default)]
    pub system: Vec<String>,
    #[serde(default)]
    pub messages: Vec<ContextMessage>,
}
