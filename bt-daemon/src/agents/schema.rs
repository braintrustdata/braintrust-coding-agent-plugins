//! Schemas shared by capture, setup, managed runs, and transcript import.
//!
//! Unknown extension fields are preserved when editing an agent-owned file.
//! Known fields are decoded here and used as concrete Rust values internally.
use crate::json::Field;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HookFile<Group> {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub version: Field<u32>,
    pub hooks: BTreeMap<String, Vec<Group>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HookGroup {
    pub hooks: Vec<CommandHook>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandHook {
    #[serde(rename = "type")]
    pub kind: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_windows: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(rename = "async", skip_serializing_if = "Option::is_none")]
    pub asynchronous: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenCodeConfig {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub model: Field<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub plugin: Option<Vec<OpenCodePlugin>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// OpenCode also accepts plugin tuples with provider-specific options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum OpenCodePlugin {
    Package(String),
    Configured((String, Value)),
}
impl OpenCodePlugin {
    pub fn package(&self) -> Option<&str> {
        match self {
            Self::Package(package) => Some(package),
            Self::Configured(_) => None,
        }
    }
}

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

/// Unrecognized native hook definitions belong to the user, and survive edits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum CursorHookEntries {
    Entries(Vec<CursorHookEntry>),
    Other(Value),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum CursorHookEntry {
    Command(CursorCommandHook),
    Other(Value),
}
impl CursorHookEntry {
    pub fn command(&self) -> Option<&str> {
        match self {
            Self::Command(hook) => Some(&hook.command),
            Self::Other(_) => None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CursorCommandHook {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_closed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
impl CursorCommandHook {
    pub fn capture(command: String, matcher: Option<String>) -> Self {
        Self {
            command,
            timeout: Some(10),
            fail_closed: Some(false),
            matcher,
            extra: Map::new(),
        }
    }
}

/// The manifest identity fields used by setup and capture version detection.
#[derive(Debug, Deserialize)]
pub(crate) struct PluginManifest {
    pub name: Option<String>,
    pub version: Option<String>,
    pub repository: Option<Repository>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Repository {
    Url(String),
    Package {
        #[serde(rename = "type")]
        _kind: Option<String>,
        #[serde(rename = "url")]
        _url: String,
    },
}
impl Repository {
    pub fn url(&self) -> Option<&str> {
        match self {
            Self::Url(url) => Some(url),
            Self::Package { .. } => None,
        }
    }
}

/// Preserve explicit null for opaque JSON fields while a missing field remains
/// None. Used only when native null has different semantics from absence.
pub(crate) fn deserialize_present<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hook_file_round_trips_extensions_and_rejects_malformed_commands() {
        let raw = json!({"future": true, "hooks": {"Stop": [{
            "matcher": "*", "futureGroup": [1], "hooks": [{
                "type": "command", "command": "bt trace hook", "commandWindows": "bt.exe trace hook",
                "futureHook": {"enabled": true}
            }]
        }]}});
        let parsed: HookFile<HookGroup> = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
        assert!(serde_json::from_value::<HookFile<HookGroup>>(json!({
            "hooks": {"Stop": [{"hooks": [{"type":"command","command":42}]}]}
        }))
        .is_err());
    }

    #[test]
    fn opencode_config_preserves_configured_plugins_and_extensions() {
        let raw =
            json!({"plugin": ["other", ["custom", {"setting": true}]], "model": "vendor/model"});
        let mut config: OpenCodeConfig = serde_json::from_value(raw.clone()).unwrap();
        config
            .plugin
            .as_mut()
            .unwrap()
            .push(OpenCodePlugin::Package("tracing".into()));
        let encoded = serde_json::to_value(config).unwrap();
        assert_eq!(encoded["plugin"][1], raw["plugin"][1]);
        assert_eq!(encoded["model"], raw["model"]);
        assert!(serde_json::from_value::<OpenCodeConfig>(json!({"plugin": "invalid"})).is_err());
    }

    #[test]
    fn cursor_preserves_unrecognized_user_hooks_when_adding_capture() {
        let raw = json!({"version":1,"hooks":{"stop":[{"command":42},{"type":"future","data":true}]},"future":true});
        let mut config: CursorHooks = serde_json::from_value(raw.clone()).unwrap();
        let CursorHookEntries::Entries(entries) =
            config.hooks.as_mut().unwrap().get_mut("stop").unwrap()
        else {
            panic!()
        };
        entries.push(CursorHookEntry::Command(CursorCommandHook::capture(
            "bt trace hook".into(),
            None,
        )));
        let encoded = serde_json::to_value(config).unwrap();
        assert_eq!(encoded["hooks"]["stop"][0], raw["hooks"]["stop"][0]);
        assert_eq!(encoded["hooks"]["stop"][1], raw["hooks"]["stop"][1]);
        assert_eq!(encoded["future"], true);
    }

    #[test]
    fn known_config_fields_are_typed_without_normalizing_missing_or_null() {
        for raw in [
            serde_json::json!({"hooks":{}}),
            serde_json::json!({"hooks":{},"version":null}),
            serde_json::json!({"hooks":{},"version":1}),
        ] {
            let file: HookFile<HookGroup> = serde_json::from_value(raw.clone()).unwrap();
            assert!(!file.extra.contains_key("version"));
            assert_eq!(serde_json::to_value(file).unwrap(), raw);
        }
        for raw in [
            serde_json::json!({}),
            serde_json::json!({"model":null}),
            serde_json::json!({"model":"vendor/model"}),
        ] {
            let config: OpenCodeConfig = serde_json::from_value(raw.clone()).unwrap();
            assert!(!config.extra.contains_key("model"));
            assert_eq!(serde_json::to_value(config).unwrap(), raw);
        }
        assert!(serde_json::from_value::<OpenCodeConfig>(serde_json::json!({"model":42})).is_err());
    }

    #[test]
    fn manifests_accept_both_plugin_and_npm_repository_shapes() {
        for repository in [
            json!("https://github.com/org/repo"),
            json!({"type":"git","url":"git+https://github.com/org/repo.git"}),
        ] {
            let manifest: PluginManifest = serde_json::from_value(
                json!({"name":"plugin","version":"1.2.3","repository":repository}),
            )
            .unwrap();
            assert_eq!(manifest.version.as_deref(), Some("1.2.3"));
        }
    }
}
