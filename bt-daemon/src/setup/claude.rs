//! Claude Code: the trace-claude-code plugin from the published marketplace.

use super::common::{
    github_repo_matches, installed_json_version_is_older, plugin_source, CommandRunner, Marketplace,
};
use crate::paths;
use anyhow::{bail, Context};
use serde_json::Value;
use std::path::Path;

const MARKETPLACE_NAME: &str = "braintrust-claude-plugin";
const MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";
const PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";
const PLUGIN_MANIFEST: &str =
    plugin_source!("claude/content/plugins/trace-claude-code/.claude-plugin/plugin.json");
const LEGACY_TRACING_ENV_KEYS: [&str; 2] = ["BRAINTRUST_CC_PROJECT", "BRAINTRUST_CC_DEBUG"];

const MARKETPLACE: Marketplace = Marketplace {
    program: "claude",
    name: MARKETPLACE_NAME,
    source: MARKETPLACE_SOURCE,
    refresh: "update",
    find: |value| {
        value
            .as_array()?
            .iter()
            .find(|item| item.get("name").and_then(Value::as_str) == Some(MARKETPLACE_NAME))
    },
    is_published: |item| {
        item.get("source").and_then(Value::as_str) == Some("github")
            && item
                .get("repo")
                .and_then(Value::as_str)
                .is_some_and(|repo| github_repo_matches(repo, MARKETPLACE_SOURCE))
    },
};

fn installed_plugin(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(PLUGIN))
}

fn list_plugins(runner: &mut impl CommandRunner) -> anyhow::Result<Value> {
    runner.json("claude", &["plugin", "list", "--json"])
}

pub(super) fn enable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    install(runner)?;
    warn_legacy_tracing_env();
    Ok(())
}

fn install(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    // Claude removes a marketplace's installed plugins when that marketplace
    // is removed, so replacing a stale source requires a fresh installation.
    if MARKETPLACE.reconcile(runner)? {
        runner.run("claude", &["plugin", "install", PLUGIN])?;
    } else {
        match installed_plugin(&list_plugins(runner)?) {
            None => runner.run("claude", &["plugin", "install", PLUGIN])?,
            Some(plugin) => {
                runner.run("claude", &["plugin", "update", PLUGIN])?;
                if plugin.get("enabled").and_then(Value::as_bool) == Some(false) {
                    runner.run("claude", &["plugin", "enable", PLUGIN])?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn disable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    if installed_plugin(&list_plugins(runner)?).is_some() {
        runner.run("claude", &["plugin", "uninstall", PLUGIN])?;
    }
    Ok(())
}

pub(super) fn update(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    if installed_plugin(&list_plugins(runner)?).is_none() {
        bail!("Claude Code tracing plugin is not installed; run `bt trace enable claude`");
    }
    MARKETPLACE.refresh_published(runner, "Claude Code", "claude")?;
    runner.run("claude", &["plugin", "update", PLUGIN])
}

pub(super) fn stale() -> bool {
    installed_json_version_is_older(
        "claude",
        &["plugin", "list", "--json"],
        |value| {
            installed_plugin(value)
                .and_then(|plugin| plugin.get("version"))
                .and_then(Value::as_str)
        },
        PLUGIN_MANIFEST,
    )
}

fn legacy_tracing_env_keys(path: &Path) -> anyhow::Result<Vec<&'static str>> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect Claude settings: {}", path.display()));
        }
    };
    let value: Value = serde_json::from_slice(&raw)
        .with_context(|| format!("Claude settings are not valid JSON: {}", path.display()))?;
    let env = value.get("env").and_then(Value::as_object);
    Ok(LEGACY_TRACING_ENV_KEYS
        .into_iter()
        .filter(|key| env.is_some_and(|env| env.contains_key(*key)))
        .collect())
}

fn warn_legacy_tracing_env() {
    let path = paths::claude_settings_path();
    let keys = match legacy_tracing_env_keys(&path) {
        Ok(keys) => keys,
        Err(error) => {
            eprintln!("warning: could not inspect legacy Claude tracing settings: {error}");
            return;
        }
    };
    if !keys.is_empty() {
        eprintln!(
            "warning: obsolete Braintrust tracing settings in {}: {}; remove these keys from env. BRAINTRUST_API_KEY is left unchanged because the Braintrust MCP plugin may use it.",
            path.display(),
            keys.join(", ")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    #[test]
    fn installs_from_the_published_marketplace_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!([]), serde_json::json!([])]);

        install(&mut runner).unwrap();

        assert!(
            runner.called("claude plugin marketplace add braintrustdata/braintrust-claude-plugin")
        );
        assert!(runner.called("claude plugin install trace-claude-code@braintrust-claude-plugin"));
    }

    #[test]
    fn refreshes_the_published_marketplace_and_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{
                "name": MARKETPLACE_NAME,
                "source": "github",
                "repo": MARKETPLACE_SOURCE
            }]),
            serde_json::json!([{
                "id": PLUGIN,
                "version": "1.4.4",
                "enabled": true
            }]),
        ]);

        install(&mut runner).unwrap();

        assert!(runner.called("claude plugin marketplace update braintrust-claude-plugin"));
        assert!(runner.called("claude plugin update trace-claude-code@braintrust-claude-plugin"));
        assert!(!runner.called("claude plugin marketplace remove braintrust-claude-plugin"));
    }

    #[test]
    fn replaces_a_same_name_local_marketplace() {
        let mut runner = FakeRunner::new([serde_json::json!([{
            "name": MARKETPLACE_NAME,
            "source": "directory",
            "path": "/tmp/stale"
        }])]);

        install(&mut runner).unwrap();

        assert!(runner.called("claude plugin marketplace remove braintrust-claude-plugin"));
        assert!(
            runner.called("claude plugin marketplace add braintrustdata/braintrust-claude-plugin")
        );
        assert!(runner.called("claude plugin install trace-claude-code@braintrust-claude-plugin"));
    }

    #[test]
    fn updates_then_enables_a_disabled_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{
                "name": MARKETPLACE_NAME,
                "source": "github",
                "repo": MARKETPLACE_SOURCE
            }]),
            serde_json::json!([{"id": PLUGIN, "enabled": false}]),
        ]);

        install(&mut runner).unwrap();

        let update = runner
            .calls
            .iter()
            .position(|call| {
                call == "claude plugin update trace-claude-code@braintrust-claude-plugin"
            })
            .unwrap();
        let enable = runner
            .calls
            .iter()
            .position(|call| {
                call == "claude plugin enable trace-claude-code@braintrust-claude-plugin"
            })
            .unwrap();
        assert!(update < enable);
    }

    #[test]
    fn update_refreshes_the_published_marketplace_then_the_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{"id": PLUGIN}]),
            serde_json::json!([{
                "name": MARKETPLACE_NAME,
                "source": "github",
                "repo": MARKETPLACE_SOURCE
            }]),
        ]);

        update(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "claude plugin list --json",
                "claude plugin marketplace list --json",
                "claude plugin marketplace update braintrust-claude-plugin",
                "claude plugin update trace-claude-code@braintrust-claude-plugin",
            ]
        );
    }

    #[test]
    fn update_refuses_an_unpublished_marketplace() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{"id": PLUGIN}]),
            serde_json::json!([{"name": MARKETPLACE_NAME, "source": "directory"}]),
        ]);

        let error = update(&mut runner).unwrap_err().to_string();

        assert_eq!(
            error,
            "Claude Code tracing marketplace is not the published Braintrust marketplace; run `bt trace enable claude`"
        );
        assert!(!runner.called("claude plugin marketplace update braintrust-claude-plugin"));
    }

    #[test]
    fn disable_uses_the_uninstall_command() {
        let mut runner = FakeRunner::new([serde_json::json!([{"id": PLUGIN}])]);
        disable(&mut runner).unwrap();
        assert!(runner.called("claude plugin uninstall trace-claude-code@braintrust-claude-plugin"));
    }

    #[test]
    fn legacy_env_diagnostic_is_read_only_and_preserves_api_key() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("settings.json");
        let contents = r#"{
            "env": {
                "BRAINTRUST_CC_PROJECT": "old-project",
                "BRAINTRUST_CC_DEBUG": "1",
                "BRAINTRUST_API_KEY": "still-used-by-mcp",
                "OTHER": "value"
            }
        }"#;
        std::fs::write(&path, contents).unwrap();

        assert_eq!(
            legacy_tracing_env_keys(&path).unwrap(),
            ["BRAINTRUST_CC_PROJECT", "BRAINTRUST_CC_DEBUG"]
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), contents);
    }
}
