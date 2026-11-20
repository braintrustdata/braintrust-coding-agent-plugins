use super::{deserialize_present, CursorCommandHook, CursorHookEntries, CursorHookEntry};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct CursorHooks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub hooks: Option<BTreeMap<String, CursorHookEntries>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl CursorHooks {
    /// Compare the complete known registration, including timeout and matcher.
    pub fn contains_registration(&self, event: &str, expected: &CursorCommandHook) -> bool {
        matches!(self.hooks.as_ref().and_then(|hooks| hooks.get(event)), Some(CursorHookEntries::Entries(entries))
            if entries.iter().any(|entry| matches!(entry, CursorHookEntry::Command(hook) if hook == expected)))
    }

    /// Filter known commands without changing unknown event or entry formats.
    pub fn retain_commands(&mut self, mut keep: impl FnMut(&str) -> bool) {
        if let Some(hooks) = &mut self.hooks {
            for entries in hooks.values_mut() {
                if let CursorHookEntries::Entries(entries) = entries {
                    entries.retain(|entry| entry.command().is_none_or(&mut keep));
                }
            }
        }
    }

    /// Replace one command's registrations, or remove them without creating a
    /// missing hook map/event. Unknown event formats cannot be safely edited.
    pub fn replace_registration(
        &mut self,
        event: &str,
        command: &str,
        replacement: Option<CursorCommandHook>,
    ) -> anyhow::Result<()> {
        if replacement.is_none()
            && self
                .hooks
                .as_ref()
                .is_none_or(|hooks| !hooks.contains_key(event))
        {
            return Ok(());
        }
        let entries = self
            .hooks
            .get_or_insert_with(Default::default)
            .entry(event.to_owned())
            .or_insert_with(|| CursorHookEntries::Entries(Vec::new()));
        let CursorHookEntries::Entries(entries) = entries else {
            anyhow::bail!("Cursor hook event `{event}` must be an array");
        };
        entries.retain(|entry| entry.command() != Some(command));
        if let Some(replacement) = replacement {
            entries.push(CursorHookEntry::Command(replacement));
        }
        Ok(())
    }
}
