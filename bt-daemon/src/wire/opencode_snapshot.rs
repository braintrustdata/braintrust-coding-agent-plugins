//! Typed OpenCode session projection used to recover events lost at shutdown.
use super::opencode_content::{Model, NativeError, Prompt, Tokens, ToolContent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Snapshot {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub messages: Vec<SnapshotMessage>,
    pub parent_id: Option<String>,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub idle_ms: Option<i64>,
    pub terminal_error: Option<NativeError>,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotMessage {
    pub id: String,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub updated_ms: i64,
    pub kind: SnapshotMessageKind,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
pub(crate) enum SnapshotMessageKind {
    #[serde(rename = "user")]
    User(SnapshotUser),
    #[serde(rename = "assistant")]
    Assistant(SnapshotAssistant),
    #[serde(rename = "compaction")]
    Compaction(SnapshotCompaction),
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotUser {
    pub time: SnapshotTime,
    #[serde(flatten)]
    pub prompt: Prompt,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotCompaction {
    pub time: SnapshotTime,
    pub status: SnapshotCompactionStatus,
    #[serde(default)]
    pub summary: String,
    pub model: Option<Model>,
    pub tokens: Option<Tokens>,
    pub error: Option<NativeError>,
    #[serde(rename = "providerContext")]
    pub provider_context: Option<crate::wire::opencode_content::NativeCompaction>,
    pub metadata: Option<SnapshotCompactionMetadata>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SnapshotCompactionStatus {
    Running,
    Completed,
    Failed,
    #[serde(other)]
    Other,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotCompactionMetadata {
    #[serde(rename = "keepID")]
    pub keep_id: Option<String>,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotAssistant {
    pub time: SnapshotTime,
    pub model: Model,
    #[serde(default)]
    pub content: Vec<SnapshotPart>,
    pub tokens: Option<Tokens>,
    pub error: Option<NativeError>,
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotTime {
    pub created: i64,
    pub completed: Option<i64>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(crate) enum SnapshotPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "reasoning")]
    Reasoning { text: String },
    #[serde(rename = "tool")]
    Tool {
        id: String,
        name: String,
        state: SnapshotToolState,
    },
    #[serde(other)]
    Other,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status")]
pub(crate) enum SnapshotToolState {
    #[serde(rename = "streaming")]
    Streaming,
    #[serde(rename = "running")]
    Running { input: Value },
    #[serde(rename = "completed")]
    Completed {
        input: Value,
        content: Vec<ToolContent>,
    },
    #[serde(rename = "error")]
    Error { input: Value, error: NativeError },
    #[serde(other)]
    Other,
}
