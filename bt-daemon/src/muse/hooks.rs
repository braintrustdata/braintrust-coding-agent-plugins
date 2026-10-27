//! Hook wire DTOs shared by live decoding and export synthesis. Unknown events
//! are ignored; known events decode once before any tracing policy executes.
use super::field::Field;
use super::ids::{RequestId, ToolCallId, TurnId};
pub(crate) use super::messages::*;
pub(crate) use super::outcome::*;
use super::usage::{TokenUsage, UsageScope};
use crate::wire::Envelope;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

macro_rules! hook_events {
    ($($name:ident($body:ty)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub(crate) enum HookName { $($name),+ }
        impl HookName {
            pub const ALL: &'static [Self] = &[$(Self::$name),+];
            pub fn as_str(self) -> &'static str { match self { $(Self::$name => stringify!($name)),+ } }
            pub fn parse(name: &str) -> Option<Self> { match name {
                $(stringify!($name) => Some(Self::$name),)+
                "ToolUseStart" => Some(Self::PreToolUse),
                _ => None,
            }}
        }
        #[derive(Debug, Clone)]
        pub(crate) enum MuseEvent { $($name(Hook<$body>)),+ }
        impl MuseEvent {
            pub fn name(&self) -> HookName { match self { $(Self::$name(_) => HookName::$name),+ } }
            pub fn context(&self) -> &HookContext { match self { $(Self::$name(hook) => &hook.context),+ } }
            pub fn payload(&self) -> serde_json::Result<Value> { match self { $(Self::$name(hook) => serde_json::to_value(hook)),+ } }
            pub fn decode(envelope: &Envelope) -> serde_json::Result<Option<Self>> {
                Ok(match HookName::parse(&envelope.event) {
                    $(Some(HookName::$name) => Some(Self::$name(serde_json::from_value(envelope.payload.clone())?)),)+
                    None => None,
                })
            }
        }
    }
}
hook_events! {
    SessionStart(Empty), UserPromptSubmit(PromptSubmitted),
    PreToolUse(ToolStarted), PermissionRequest(PermissionObserved),
    PostToolUse(ToolFinished), PostToolUseFailure(ToolFinished),
    PostToolBatch(ToolBatchFinished), PreLLMCall(ModelStarted), PostLLMCall(ModelFinished),
    PreCompact(Compaction), PostCompact(Compaction),
    SubagentStart(SubagentStarted), SubagentStop(SubagentFinished),
    Stop(TurnFinished), StopFailure(TurnFinished), SessionEnd(SessionFinished),
    Notification(NotificationObserved),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Empty {}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Hook<B> {
    #[serde(flatten)]
    pub context: HookContext,
    #[serde(flatten)]
    pub body: B,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HookContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "optional_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub turn_id: Option<TurnId>,
    #[serde(
        default,
        deserialize_with = "optional_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_id: Option<RequestId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript_path: Option<String>,
    #[serde(flatten)]
    pub session: SessionMetadata,
}
impl HookContext {
    pub fn turn(&self) -> Option<&str> {
        self.turn_id.as_deref().filter(|s| !s.is_empty())
    }
    pub fn request(&self) -> Option<&str> {
        self.request_id.as_deref().filter(|s| !s.is_empty())
    }
}
// The native optional identifiers have historically tolerated non-string values.
// Keep this compatibility at decoding, never inside correlation logic.
fn optional_id<'de, D: Deserializer<'de>, T: From<String>>(d: D) -> Result<Option<T>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Id {
        Text(String),
        Other(serde::de::IgnoredAny),
    }
    Ok(match Id::deserialize(d)? {
        Id::Text(s) if !s.is_empty() => Some(T::from(s)),
        _ => None,
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SessionMetadata {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub cwd: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub model: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub provider: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub permission_mode: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub trajectory_id: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub root_session_id: Field<String>,
}
impl SessionMetadata {
    pub fn update(&mut self, incoming: &Self) {
        macro_rules! update { ($($field:ident),+) => { $(if !incoming.$field.is_missing() { self.$field.clone_from(&incoming.$field); })+ }; }
        update!(
            cwd,
            model,
            provider,
            permission_mode,
            trajectory_id,
            root_session_id
        );
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PromptSubmitted {
    pub prompt: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelDetails {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub step: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub attempt: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub message_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub tool_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub options: Field<ProviderOptions>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelStarted {
    #[serde(flatten)]
    pub details: ModelDetails,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub request_record_id: Field<String>,
    pub messages: Option<Vec<NativeMessage>>,
    pub tools: Option<Vec<ToolDescription>>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub input_summary: Field<InputSummary>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ModelFinished {
    pub usage: Option<TokenUsage>,
    #[serde(default)]
    pub usage_scope: UsageScope,
    pub output_text_preview: Option<ModelOutput>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub response_id: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub finish_reason: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub tool_call_count: Field<u64>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub options: Field<ProviderOptions>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolStarted {
    #[serde(default, deserialize_with = "optional_id")]
    pub tool_use_id: Option<ToolCallId>,
    pub tool_name: Option<String>,
    pub tool_input: Option<ToolInput>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolFinished {
    #[serde(flatten)]
    pub tool: ToolStarted,
    pub tool_response: Option<ToolOutput>,
    pub duration_ms: Option<u64>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ToolBatchFinished {
    #[serde(default)]
    pub tool_calls: Vec<ToolFinished>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PermissionObserved {
    pub tool_name: Option<String>,
    pub tool_input: Option<ToolInput>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct NotificationObserved {
    pub notification_type: Option<String>,
    pub title: Option<String>,
    pub message: Option<String>,
    #[serde(flatten)]
    pub tool: PermissionObserved,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubagentStarted {
    pub child_session_id: Option<String>,
    pub subagent_id: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubagentFinished {
    #[serde(flatten)]
    pub child: SubagentStarted,
    pub last_assistant_message: Option<ModelOutput>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Compaction {
    pub trigger: Option<String>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TurnFinished {
    pub last_assistant_message: Option<ModelOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal: Option<String>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SessionFinished {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub reason: Field<ErrorDetail>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn envelope(name: &str, payload: Value) -> Envelope {
        Envelope {
            source: "muse".into(),
            session_id: "session".into(),
            event: name.into(),
            ts_ms: 100,
            source_version: None,
            plugin_version: None,
            managed_run_id: None,
            capture: None,
            route: None,
            config: None,
            payload,
        }
    }
    #[test]
    fn all_hook_names_decode_and_optional_ids_remain_tolerant() {
        for name in HookName::ALL {
            let e = envelope(
                name.as_str(),
                json!({"turn_id":{},"request_id":4,"future_field":true}),
            );
            let decoded = MuseEvent::decode(&e).unwrap().unwrap();
            assert_eq!(decoded.name(), *name);
            assert_eq!(decoded.context().turn(), None);
            assert_eq!(decoded.context().request(), None);
        }
        assert!(matches!(
            MuseEvent::decode(&envelope("ToolUseStart", json!({}))).unwrap(),
            Some(MuseEvent::PreToolUse(_))
        ));
        assert!(MuseEvent::decode(&envelope("FutureEvent", json!({})))
            .unwrap()
            .is_none());
        assert!(MuseEvent::decode(&envelope("PreLLMCall", json!({"step":"wrong"}))).is_err());
    }
    #[test]
    fn known_content_is_validated_and_unknown_content_is_lossless() {
        let block = json!({"type":"image","source":{"url":"example"},"future":true});
        let decoded: ContentBlock = serde_json::from_value(block.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), block);
        assert!(serde_json::from_value::<ContentBlock>(json!({"type":"text","text":42})).is_err());
        let args = ToolArguments::Encoded("{\"x\":1}".into());
        assert_eq!(args.input().0, json!({"x":1}));
        assert_eq!(
            ToolArguments::Encoded("invalid json".into()).input().0,
            json!("invalid json")
        );
    }
}
