//! Authored user-hook schema. Foreign settings are edited losslessly in setup;
//! they cannot be decoded as this narrower schema without losing extensions.
use super::hooks::HookName;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Serialize)]
pub(crate) struct OwnedHookFile {
    pub schema_version: u32,
    pub hooks: BTreeMap<HookName, Vec<OwnedHookGroup>>,
}
#[derive(Serialize)]
pub(crate) struct OwnedHookGroup {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<&'static str>,
    pub hooks: Vec<OwnedCommandHook>,
}
#[derive(Serialize)]
pub(crate) struct OwnedCommandHook {
    #[serde(rename = "type")]
    pub hook_type: CommandHookType,
    pub command: String,
}
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CommandHookType {
    Command,
}
impl OwnedHookFile {
    pub fn current(command: &str) -> Self {
        Self::for_events(HookName::ALL.iter().copied(), command)
    }
    pub fn for_events(events: impl IntoIterator<Item = HookName>, command: &str) -> Self {
        Self {
            schema_version: 1,
            hooks: events
                .into_iter()
                .map(|name| {
                    (
                        name,
                        vec![OwnedHookGroup {
                            matcher: None,
                            hooks: vec![OwnedCommandHook {
                                hook_type: CommandHookType::Command,
                                command: command.into(),
                            }],
                        }],
                    )
                })
                .collect(),
        }
    }
}

pub(crate) const HOOK_COMMAND: &str = "bt trace hook --source muse --flush-on-turn-end";

/// Lossless preservation boundary for user hooks, including unknown groups,
/// handler attributes, and future schemas. Only exact owned handlers are edited.
#[derive(Clone, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct HookOverlay(serde_json::Map<String, serde_json::Value>);
impl HookOverlay {
    pub fn from_value(value: serde_json::Value) -> anyhow::Result<Self> {
        Ok(Self(serde_json::from_value(value)?))
    }
    pub fn into_value(self) -> serde_json::Value {
        serde_json::Value::Object(self.0)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn contains_braintrust_hook(&self) -> bool {
        self.0.values().any(contains_braintrust_hook)
    }
    pub fn remove_owned(&mut self) -> anyhow::Result<()> {
        use anyhow::Context;
        use serde_json::Value;
        let mut emptied = Vec::new();
        for (event, groups) in &mut self.0 {
            let groups = groups
                .as_array_mut()
                .context("Muse hook event must be an array")?;
            let original_len = groups.len();
            for index in (0..groups.len()).rev() {
                let handlers = groups[index]
                    .get_mut("hooks")
                    .and_then(Value::as_array_mut)
                    .context("Muse hook group must contain a hooks array")?;
                let original_handlers = handlers.len();
                handlers.retain(|handler| !owned_command(handler));
                if original_handlers != handlers.len() && handlers.is_empty() {
                    groups.remove(index);
                }
            }
            if original_len != groups.len() && groups.is_empty() {
                emptied.push(event.clone());
            }
        }
        for event in emptied {
            self.0.remove(&event);
        }
        Ok(())
    }
    pub fn append_owned(&mut self, command: &str) -> anyhow::Result<()> {
        use anyhow::Context;
        use serde_json::Value;
        for (event, groups) in OwnedHookFile::current(command).hooks {
            let target = self
                .0
                .entry(event.as_str().to_owned())
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .context("Muse hook event must be an array")?;
            for group in groups {
                target.push(serde_json::to_value(group)?);
            }
        }
        Ok(())
    }
}
fn owned_command(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    value.get("type").and_then(Value::as_str) == Some("command")
        && matches!(
            value.get("command").and_then(Value::as_str),
            Some(HOOK_COMMAND) | Some("bt trace hook --source muse --flush-on-turn-end || true")
        )
        && value.as_object().is_some_and(|object| object.len() == 2)
}
/// Inspect arbitrary enterprise/project schemas without rewriting them.
pub(crate) fn contains_braintrust_hook(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "command"
                && value.as_str().is_some_and(|command| {
                    command.contains("--source")
                        && command.contains("muse")
                        && command.contains("hook")
                }))
                || contains_braintrust_hook(value)
        }),
        Value::Array(items) => items.iter().any(contains_braintrust_hook),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn attributes_prevent_ownership_and_foreign_groups_survive() {
        let foreign = json!({"matcher":"x","extension":true,"hooks":[{"type":"command","command":HOOK_COMMAND,"timeout":42}]});
        let mut hooks = HookOverlay::from_value(json!({"Stop":[foreign.clone()]})).unwrap();
        hooks.append_owned(HOOK_COMMAND).unwrap();
        hooks.remove_owned().unwrap();
        assert_eq!(hooks.into_value(), json!({"Stop":[foreign]}));
    }
}
