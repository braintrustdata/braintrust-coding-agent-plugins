//! Known hook structure is typed; extension data and future handler formats are
//! preserved as opaque JSON. No settings orchestration navigates these values.
use super::{HOOK_COMMAND, LEGACY_HOOK_COMMAND};
use crate::muse::field::Field;
use crate::muse::hooks::HookName;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

type Extensions = BTreeMap<String, Value>;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct HookOverlay(BTreeMap<String, Vec<HookGroup>>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct HookGroup {
    hooks: Vec<HookHandler>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    matcher: Field<String>,
    #[serde(flatten)]
    extra: Extensions,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
enum HookHandler {
    User(UserCommand),
    Enterprise(EnterpriseCommand),
    Unknown(Value),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum CommandType {
    #[serde(rename = "command")]
    Command,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserCommand {
    #[serde(rename = "type")]
    kind: CommandType,
    command: String,
    #[serde(flatten)]
    extra: Extensions,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EnterpriseCommand {
    command: String,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    name: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    timeout_ms: Field<u64>,
    #[serde(flatten)]
    extra: Extensions,
}
impl<'de> Deserialize<'de> for HookHandler {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // The opaque handler boundary selects a native schema. A malformed
        // recognized command must not silently pass as a future handler kind.
        let value = Value::deserialize(d)?;
        if value.get("type").and_then(Value::as_str) == Some("command") {
            serde_json::from_value(value)
                .map(Self::User)
                .map_err(serde::de::Error::custom)
        } else if value.get("type").is_none() && value.get("command").is_some_and(Value::is_string)
        {
            serde_json::from_value(value)
                .map(Self::Enterprise)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Self::Unknown(value))
        }
    }
}
impl HookHandler {
    fn is_owned(&self) -> bool {
        matches!(self, Self::User(handler) if handler.extra.is_empty()
            && matches!(handler.command.as_str(), HOOK_COMMAND | LEGACY_HOOK_COMMAND))
    }
    fn contains_braintrust(&self) -> bool {
        match self {
            Self::User(h) => is_forwarder(&h.command) || extensions_contain(&h.extra),
            Self::Enterprise(h) => is_forwarder(&h.command) || extensions_contain(&h.extra),
            Self::Unknown(raw) => opaque_contains_braintrust(raw),
        }
    }
}
impl HookOverlay {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn contains_braintrust_hook(&self) -> bool {
        self.0.values().flatten().any(|g| {
            extensions_contain(&g.extra) || g.hooks.iter().any(HookHandler::contains_braintrust)
        })
    }
    pub fn remove_owned(&mut self) {
        self.0.retain(|_, groups| {
            let original_len = groups.len();
            groups.retain_mut(|group| {
                let original = group.hooks.len();
                group.hooks.retain(|h| !h.is_owned());
                // Foreign group attributes remain even if its only handler was ours.
                original == group.hooks.len()
                    || !group.hooks.is_empty()
                    || !group.extra.is_empty()
                    || match &group.matcher {
                        Field::Missing => false,
                        Field::Value(value) => value != "*",
                        Field::Null => true,
                    }
            });
            original_len == groups.len() || !groups.is_empty()
        });
    }
    pub fn append_owned(&mut self, command: &str) {
        for event in HookName::ALL {
            self.0
                .entry(event.as_str().into())
                .or_default()
                .push(HookGroup {
                    matcher: Field::Missing,
                    extra: BTreeMap::new(),
                    hooks: vec![HookHandler::User(UserCommand {
                        kind: CommandType::Command,
                        command: command.into(),
                        extra: BTreeMap::new(),
                    })],
                });
        }
    }
}
fn is_forwarder(command: &str) -> bool {
    command.contains("--source") && command.contains("muse") && command.contains("hook")
}
fn extensions_contain(extra: &Extensions) -> bool {
    extra.iter().any(|(key, value)| {
        (key == "command" && value.as_str().is_some_and(is_forwarder))
            || opaque_contains_braintrust(value)
    })
}
/// Conservative compatibility scan restricted to unknown extension/handler data.
pub(super) fn opaque_contains_braintrust(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "command" && value.as_str().is_some_and(is_forwarder))
                || opaque_contains_braintrust(value)
        }),
        Value::Array(items) => items.iter().any(opaque_contains_braintrust),
        _ => false,
    }
}

/// Read-only view of known project/enterprise files. Other root schemas are
/// retained by the inspection codec rather than being rewritten or normalized.
#[derive(Deserialize)]
pub(super) struct HookDocument {
    #[serde(default)]
    hooks: HookOverlay,
    #[serde(flatten)]
    extra: Extensions,
}
impl HookDocument {
    pub fn contains_braintrust(&self) -> bool {
        self.hooks.contains_braintrust_hook() || extensions_contain(&self.extra)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn foreign_handlers_and_groups_survive_install_remove_cycle() {
        let original = json!({"FutureEvent":[{"matcher":null,"extension":{"nested":true},"hooks":[
            {"type":"future","payload":{"x":1}},
            {"name":"enterprise","timeout_ms":1000,"command":"echo policy","extension":true},
            {"type":"command","command":HOOK_COMMAND,"timeout":42}
        ]}],"FutureEmpty":[]});
        let mut hooks: HookOverlay = serde_json::from_value(original.clone()).unwrap();
        hooks.append_owned(HOOK_COMMAND);
        hooks.remove_owned();
        assert_eq!(serde_json::to_value(hooks).unwrap(), original);
    }
    #[test]
    fn removing_our_handler_keeps_foreign_group_attributes() {
        for group in [
            json!({"hooks":[{"type":"command","command":HOOK_COMMAND}],"future":true}),
            json!({"hooks":[{"type":"command","command":HOOK_COMMAND}],"matcher":null}),
            json!({"hooks":[{"type":"command","command":HOOK_COMMAND}],"matcher":"custom"}),
        ] {
            let mut hooks: HookOverlay =
                serde_json::from_value(json!({"Stop":[group.clone()]})).unwrap();
            hooks.remove_owned();
            let mut expected = group;
            expected["hooks"] = json!([]);
            assert_eq!(
                serde_json::to_value(hooks).unwrap(),
                json!({"Stop":[expected]})
            );
        }
    }
    #[test]
    fn conservative_forwarder_checks_cover_unknown_extensions() {
        for handler in [
            json!({"type":"future","nested":{"command":HOOK_COMMAND}}),
            json!({"type":"command","command":"echo unrelated","extension":{"command":HOOK_COMMAND}}),
        ] {
            let mut hooks: HookOverlay =
                serde_json::from_value(json!({"Stop":[{"hooks":[handler]}]})).unwrap();
            hooks.remove_owned();
            assert!(hooks.contains_braintrust_hook());
        }
        assert!(serde_json::from_value::<HookOverlay>(
            json!({"Stop":[{"hooks":[{"type":"command","command":null}]}]})
        )
        .is_err());
    }
}
