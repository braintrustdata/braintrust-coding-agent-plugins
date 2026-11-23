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
    let raw = json!({"plugin": ["other", ["custom", {"setting": true}]], "model": "vendor/model"});
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

#[test]
fn cursor_registration_edits_preserve_unknown_entries_and_absent_events() {
    let mut absent = CursorHooks::default();
    absent.replace_registration("stop", "owned", None).unwrap();
    assert!(absent.hooks.is_none());

    let raw = json!({"hooks":{"stop":[{"type":"future","payload":true},{"command":"owned"},{"command":"owned"},{"command":"foreign"}],"future":{"new":true}}});
    let mut config: CursorHooks = serde_json::from_value(raw.clone()).unwrap();
    let expected = CursorCommandHook::capture("owned".into(), None);
    config
        .replace_registration("stop", "owned", Some(expected.clone()))
        .unwrap();
    assert!(config.contains_registration("stop", &expected));
    config.retain_commands(|command| command != "owned");
    let before = serde_json::to_value(&config).unwrap();
    assert_eq!(
        before["hooks"]["stop"],
        json!([{"type":"future","payload":true},{"command":"foreign"}])
    );
    assert_eq!(before["hooks"]["future"], raw["hooks"]["future"]);
    assert!(config
        .replace_registration("future", "owned", Some(expected))
        .is_err());
    assert_eq!(serde_json::to_value(config).unwrap(), before);
}

#[test]
fn opencode_plugin_edits_preserve_configured_options_and_exact_specs() {
    let mut absent = OpenCodeConfig::default();
    absent.retain_plugins(|_| false);
    assert!(absent.plugin.is_none());

    let raw = json!({"plugin":[["@vendor/plugin",{"setting":null}],"@vendor/plugin@1","@vendor/plugin-other"],"model":null});
    let mut config: OpenCodeConfig = serde_json::from_value(raw).unwrap();
    config.replace_plugin(
        |plugin| plugin.matches_package("@vendor/plugin"),
        OpenCodePlugin::Package("@vendor/plugin@2".into()),
    );
    config.ensure_plugin("@vendor/plugin@2".into());
    assert_eq!(
        serde_json::to_value(config).unwrap(),
        json!({"plugin":[["@vendor/plugin",{"setting":null}],"@vendor/plugin-other","@vendor/plugin@2"],"model":null})
    );
}
