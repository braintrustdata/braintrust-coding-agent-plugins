//! Hook wire DTOs shared by live decoding and export synthesis. Unknown events
//! are ignored; known events decode once before any tracing policy executes.

use serde::{Deserialize, Deserializer};

mod compaction;
mod empty;
mod hook;
mod hook_context;
mod model_details;
mod model_finished;
mod model_started;
mod notification_observed;
mod permission_observed;
mod prompt_submitted;
mod session_finished;
mod session_metadata;
mod subagent_finished;
mod subagent_started;
mod tool_batch_finished;
mod tool_finished;
mod tool_started;
mod turn_finished;

pub(crate) use compaction::Compaction;
pub(crate) use empty::Empty;
pub(crate) use hook::Hook;
pub(crate) use hook_context::HookContext;
pub(crate) use model_details::ModelDetails;
pub(crate) use model_finished::ModelFinished;
pub(crate) use model_started::ModelStarted;
pub(crate) use notification_observed::NotificationObserved;
pub(crate) use permission_observed::PermissionObserved;
pub(crate) use prompt_submitted::PromptSubmitted;
pub(crate) use session_finished::SessionFinished;
pub(crate) use session_metadata::SessionMetadata;
pub(crate) use subagent_finished::SubagentFinished;
pub(crate) use subagent_started::SubagentStarted;
pub(crate) use tool_batch_finished::ToolBatchFinished;
pub(crate) use tool_finished::ToolFinished;
pub(crate) use tool_started::ToolStarted;
pub(crate) use turn_finished::TurnFinished;

pub(crate) use super::messages::*;
pub(crate) use super::outcome::*;
mod events;
pub(crate) use events::{HookName, MuseEvent};

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

#[cfg(test)]
mod tests;
