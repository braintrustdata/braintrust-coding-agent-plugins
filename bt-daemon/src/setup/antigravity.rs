//! Google Antigravity: the tracing plugin installed with the `agy` CLI.

use super::common::{edit_object, CommandRunner, FileAccess};
use crate::agents::Antigravity;
use crate::paths;
use crate::setup::Setup;
use anyhow::bail;
use serde_json::Value;
use std::path::Path;

const PLUGIN: &str = "braintrust-antigravity-tracing";
const PLUGIN_SOURCE: &str = "https://github.com/braintrustdata/braintrust-antigravity-plugin";

/// `agy` resolves its configuration from `$HOME/.gemini/config`, so commands
/// run with `HOME` derived from the configured directory.
fn home(config_dir: &Path) -> anyhow::Result<&Path> {
    if config_dir.file_name().and_then(|part| part.to_str()) != Some("config") {
        bail!(
            "Antigravity configuration directory must end in `.gemini/config`: {}",
            config_dir.display()
        );
    }
    let gemini_dir = config_dir.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "Antigravity configuration directory has no parent: {}",
            config_dir.display()
        )
    })?;
    if gemini_dir.file_name().and_then(|part| part.to_str()) != Some(".gemini") {
        bail!(
            "Antigravity configuration directory must end in `.gemini/config`: {}",
            config_dir.display()
        );
    }
    gemini_dir.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "Antigravity configuration directory has no home parent: {}",
            config_dir.display()
        )
    })
}

/// Earlier releases registered hooks directly in `hooks.json`; the plugin
/// now provides them.
fn remove_legacy_registration(config_dir: &Path) -> anyhow::Result<()> {
    edit_object(
        &config_dir.join("hooks.json"),
        FileAccess::Inherited,
        |hooks, _| {
            hooks.remove(PLUGIN);
            Ok(())
        },
    )
}

fn enable_at(runner: &mut dyn CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    let home = home(config_dir)?;
    runner.run_in_home("agy", &["plugin", "install", PLUGIN_SOURCE], home)?;
    runner.run_in_home("agy", &["plugin", "enable", PLUGIN], home)?;
    remove_legacy_registration(config_dir)
}

fn disable_at(runner: &mut dyn CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    // Antigravity's uninstall command is idempotent when the plugin is absent.
    runner.run_in_home("agy", &["plugin", "uninstall", PLUGIN], home(config_dir)?)?;
    remove_legacy_registration(config_dir)
}

fn update_at(runner: &mut dyn CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    let home = home(config_dir)?;
    let plugins = runner.json_in_home("agy", &["plugin", "list"], home)?;
    let installed = plugins
        .get("imports")
        .and_then(Value::as_array)
        .is_some_and(|imports| {
            imports
                .iter()
                .any(|plugin| plugin.get("name").and_then(Value::as_str) == Some(PLUGIN))
        });
    if !installed {
        bail!(
            "Google Antigravity tracing plugin is not installed; run `bt trace enable antigravity`"
        );
    }
    runner.run_in_home("agy", &["plugin", "install", PLUGIN_SOURCE], home)
}

impl Setup for Antigravity {
    fn enable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        enable_at(runner, &paths::antigravity_config_dir())
    }

    fn disable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        disable_at(runner, &paths::antigravity_config_dir())
    }

    fn update(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        update_at(runner, &paths::antigravity_config_dir())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    struct MissingAgyRunner;

    impl CommandRunner for MissingAgyRunner {
        fn json(&mut self, _: &str, _: &[&str]) -> anyhow::Result<Value> {
            unreachable!()
        }

        fn json_in_home(&mut self, _: &str, _: &[&str], _: &Path) -> anyhow::Result<Value> {
            unreachable!()
        }

        fn run(&mut self, _: &str, _: &[&str]) -> anyhow::Result<()> {
            unreachable!()
        }

        fn run_in_home(&mut self, program: &str, _: &[&str], _: &Path) -> anyhow::Result<()> {
            anyhow::bail!("failed to run `{program}`; install {program} and ensure it is on PATH")
        }
    }

    #[test]
    fn installs_published_plugin_and_removes_legacy_registration() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        std::fs::write(
            &hooks_path,
            serde_json::to_vec(&serde_json::json!({
                PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": [{"type": "command", "command": "other"}]}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        enable_at(&mut runner, &config_dir).unwrap();

        assert!(runner.called(&format!("agy plugin install {PLUGIN_SOURCE}")));
        assert!(runner.called(&format!("agy plugin enable {PLUGIN}")));
        let hooks: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert_eq!(hooks["other-plugin"]["Stop"][0]["command"], "other");
        assert!(hooks.get(PLUGIN).is_none());
    }

    #[test]
    fn update_checks_and_updates_the_same_overridden_home() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let expected_home = temp.path().to_path_buf();
        let mut runner = FakeRunner::new([serde_json::json!({
            "imports": [{"name": PLUGIN}],
        })]);

        update_at(&mut runner, &config_dir).unwrap();

        assert_eq!(
            runner.home_calls,
            vec![
                ("agy plugin list".into(), expected_home.clone()),
                (format!("agy plugin install {PLUGIN_SOURCE}"), expected_home),
            ]
        );
    }

    #[test]
    fn setup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("hooks.json"),
            serde_json::to_vec(&serde_json::json!({
                PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": []}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        enable_at(&mut runner, &config_dir).unwrap();
        let first = std::fs::read(config_dir.join("hooks.json")).unwrap();
        enable_at(&mut runner, &config_dir).unwrap();
        let second = std::fs::read(config_dir.join("hooks.json")).unwrap();

        assert_eq!(first, second);
        assert_eq!(
            runner
                .calls
                .iter()
                .filter(|call| call.as_str() == format!("agy plugin install {PLUGIN_SOURCE}"))
                .count(),
            2
        );
    }

    #[test]
    fn setup_relies_on_native_plugin_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        let mut runner = FakeRunner::new([]);

        enable_at(&mut runner, &config_dir).unwrap();

        assert!(!config_dir.join("hooks.json").exists());
        assert!(runner.called(&format!("agy plugin install {PLUGIN_SOURCE}")));
        assert!(runner.called(&format!("agy plugin enable {PLUGIN}")));
    }

    #[test]
    fn setup_reports_a_missing_cli_without_changing_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        let original = br#"{"other-plugin":{"Stop":[]}}"#;
        std::fs::write(&hooks_path, original).unwrap();

        let error = enable_at(&mut MissingAgyRunner, &config_dir).unwrap_err();

        assert_eq!(
            error.to_string(),
            "failed to run `agy`; install agy and ensure it is on PATH"
        );
        assert_eq!(std::fs::read(hooks_path).unwrap(), original);
    }

    #[test]
    fn disable_removes_only_the_managed_registration() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        std::fs::write(
            &hooks_path,
            serde_json::to_vec(&serde_json::json!({
                PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": [{"type": "command", "command": "other"}]}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        disable_at(&mut runner, &config_dir).unwrap();

        assert!(runner.called("agy plugin uninstall braintrust-antigravity-tracing"));
        let hooks: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert!(hooks.get(PLUGIN).is_none());
        assert_eq!(hooks["other-plugin"]["Stop"][0]["command"], "other");
    }
}
