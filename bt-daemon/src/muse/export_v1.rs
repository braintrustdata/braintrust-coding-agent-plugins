//! Durable export-v1 DTOs. This is a separate native schema from MSP views.
use super::hooks::{ErrorDetail, InputSummary, ToolArguments, ToolOutput};
use super::usage::TokenUsage;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Header {
    pub export_schema_version: u64,
    pub exporter_version: Option<Version>,
    pub sessions: Vec<Session>,
    #[serde(default)]
    pub session_terminated_abnormally: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Version {
    String(String),
    Object { semver: String },
}

impl Version {
    pub(crate) fn into_string(self) -> String {
        match self {
            Self::String(value) | Self::Object { semver: value } => value,
        }
    }
}

#[derive(Clone, Deserialize)]
pub(crate) struct Session {
    pub session_id: String,
    pub trajectory_id: Option<String>,
    pub root_session_id: Option<String>,
    pub session_end: Option<SessionEnd>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct SessionEnd {
    pub exit_reason: Option<String>,
}

/// Export timestamps are microseconds; daemon envelopes use milliseconds.
#[derive(Clone, Copy, Deserialize)]
#[serde(transparent)]
pub(crate) struct UnixMicros(i64);
impl UnixMicros {
    pub fn millis(self) -> i64 {
        self.0 / 1_000
    }
}
#[derive(Deserialize)]
pub(crate) struct ExportRecord {
    pub recorded_at: Option<UnixMicros>,
    pub envelope: Option<NativeEnvelope>,
}
#[derive(Deserialize)]
pub(crate) struct NativeEnvelope {
    pub recorded_at: Option<UnixMicros>,
    pub payload: Option<NativePayload>,
}
pub(crate) enum NativePayload {
    Metadata { record: Option<ExportMetadata> },
    RunModel { record: Option<RunModelRecord> },
    Run { run_id: String, event: RunEvent },
    Task { event: Option<TaskEvent> },
    SessionEnd,
    Ignored,
}
// Some retained envelopes contain non-event payloads without a kind. Select
// the schema before decoding: unknown bodies stay opaque, malformed *known*
// bodies still fail. RawValue is confined to this codec and never enters state.
impl<'de> Deserialize<'de> for NativePayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde_json::value::RawValue;
        #[derive(Default, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Kind {
            Metadata,
            RunModel,
            Run,
            Task,
            SessionEnd,
            #[default]
            #[serde(other)]
            Ignored,
        }
        #[derive(Deserialize)]
        struct Payload {
            #[serde(default)]
            kind: Option<Kind>,
            run_id: Option<String>,
            event: Option<Box<RawValue>>,
            record: Option<Box<RawValue>>,
        }
        fn decode<T: serde::de::DeserializeOwned, E: serde::de::Error>(
            value: Option<Box<RawValue>>,
        ) -> Result<Option<T>, E> {
            value
                .map(|raw| serde_json::from_str(raw.get()).map_err(E::custom))
                .transpose()
        }
        let raw = Payload::deserialize(d)?;
        Ok(match raw.kind.unwrap_or_default() {
            Kind::Ignored => Self::Ignored,
            Kind::SessionEnd => Self::SessionEnd,
            Kind::Metadata => Self::Metadata {
                record: decode(raw.record)?,
            },
            Kind::RunModel => Self::RunModel {
                record: decode(raw.record)?,
            },
            Kind::Task => Self::Task {
                event: decode(raw.event)?,
            },
            Kind::Run => match decode(raw.event)? {
                None | Some(RunEvent::Ignored) => Self::Ignored,
                Some(event) => Self::Run {
                    run_id: raw
                        .run_id
                        .ok_or_else(|| serde::de::Error::missing_field("run_id"))?,
                    event,
                },
            },
        })
    }
}
#[derive(Clone, Default, Deserialize)]
pub(crate) struct ExportMetadata {
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub workspace_root: Option<String>,
}
#[derive(Clone, Deserialize)]
pub(crate) struct RunModelRecord {
    pub run_stream: RunStream,
    #[serde(flatten)]
    pub metadata: ExportMetadata,
}
#[derive(Clone, Deserialize)]
pub(crate) struct RunStream {
    pub id: String,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum RunEvent {
    Started {
        prompt: Option<String>,
    },
    ModelInputTraceRecorded {
        request_record_id: String,
        model_step: Option<u64>,
        bounded: Option<InputSummary>,
    },
    ModelResponseCreated {
        response_id: Option<String>,
    },
    ModelCompleted {
        usage: Option<TokenUsage>,
    },
    AssistantToolCallsCommitted {
        tool_calls: Vec<ExportToolCall>,
        response_id: Option<String>,
    },
    ToolResultBatchCommitted {
        results: Vec<ExportToolResult>,
    },
    ModelFailureTerminalIntent {
        reason: Option<String>,
    },
    // Resource samples are not provider token usage. Their unused body is skipped.
    ResourceUsageSampled,
    AssistantMessageCommitted {
        text: String,
        response_id: Option<String>,
    },
    Terminal {
        terminal: String,
        reason: Option<ErrorDetail>,
    },
    #[serde(other)]
    Ignored,
}
#[derive(Deserialize)]
pub(crate) struct ExportToolCall {
    pub call_id: String,
    pub name: String,
    pub args: Option<ToolArguments>,
}
#[derive(Deserialize)]
pub(crate) struct ExportToolResult {
    pub tool_call_id: String,
    pub text: Option<ToolOutput>,
}
#[derive(Deserialize)]
pub(crate) struct TaskEvent {
    #[serde(default)]
    pub kind: TaskEventKind,
    pub task_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub origin: Option<TaskOrigin>,
    pub reason: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskEventKind {
    Scheduled,
    Started,
    Completed,
    Failed,
    #[serde(alias = "canceled")]
    Cancelled,
    Rejected,
    #[default]
    #[serde(other)]
    Ignored,
}
#[derive(Deserialize)]
pub(crate) struct TaskOrigin {
    pub tool_call_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn non_event_payloads_and_future_records_are_ignored_but_known_records_are_checked() {
        for payload in [
            json!({"record":{"future":true}}),
            json!({"kind":null,"record":{"future":true}}),
            json!({"kind":"future_payload","event":{"future":true}}),
            json!({"kind":"run","event":{"kind":"future_event"}}),
            json!({"kind":"run"}),
        ] {
            assert!(matches!(
                serde_json::from_str::<NativePayload>(&payload.to_string()).unwrap(),
                NativePayload::Ignored
            ));
        }
        assert!(serde_json::from_str::<NativePayload>(r#"{"kind":"run","run_id":"r","event":{"kind":"model_completed","usage":{"input_tokens":"wrong"}}}"#).is_err());
        assert!(serde_json::from_str::<NativePayload>(
            r#"{"kind":"run","event":{"kind":"started"}}"#
        )
        .is_err());
    }
}
