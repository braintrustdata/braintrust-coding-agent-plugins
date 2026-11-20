use super::*;
use crate::wire::Envelope;
use serde::{Deserialize, Serialize};
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
