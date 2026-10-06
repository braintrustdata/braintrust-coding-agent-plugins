//! Codex: the trace-codex plugin from the published Codex marketplace.

use super::common::{
    github_repo_matches, installed_json_version_is_older, plugin_source, CommandRunner, Marketplace,
};
use anyhow::bail;
use serde_json::Value;

const MARKETPLACE_NAME: &str = "braintrust-codex-plugins";
const MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const PLUGIN_MANIFEST: &str =
    plugin_source!("codex/content/plugins/trace-codex/.codex-plugin/plugin.json");

const MARKETPLACE: Marketplace = Marketplace {
    program: "codex",
    name: MARKETPLACE_NAME,
    source: MARKETPLACE_SOURCE,
    refresh: "upgrade",
    find: |value| {
        value
            .get("marketplaces")
            .and_then(Value::as_array)?
            .iter()
            .find(|item| item.get("name").and_then(Value::as_str) == Some(MARKETPLACE_NAME))
    },
    is_published: |item| {
        item.get("marketplaceSource")
            .and_then(|source| source.get("source"))
            .and_then(Value::as_str)
            .is_some_and(|source| github_repo_matches(source, MARKETPLACE_SOURCE))
    },
};

fn installed_plugin(value: &Value) -> Option<&Value> {
    value
        .get("installed")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| item.get("pluginId").and_then(Value::as_str) == Some(PLUGIN))
}

fn list_plugins(runner: &mut impl CommandRunner) -> anyhow::Result<Value> {
    runner.json("codex", &["plugin", "list", "--json"])
}

pub(super) fn enable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    MARKETPLACE.reconcile(runner)?;
    // Adding is idempotent and reconciles the installed cache to the refreshed
    // marketplace snapshot.
    runner.run("codex", &["plugin", "add", PLUGIN])
}

pub(super) fn disable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    if installed_plugin(&list_plugins(runner)?).is_some() {
        runner.run("codex", &["plugin", "remove", PLUGIN, "--json"])?;
    }
    Ok(())
}

pub(super) fn update(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    if installed_plugin(&list_plugins(runner)?).is_none() {
        bail!("Codex tracing plugin is not installed; run `bt trace enable codex`");
    }
    MARKETPLACE.refresh_published(runner, "Codex", "codex")?;
    runner.run("codex", &["plugin", "add", PLUGIN])
}

pub(super) fn stale() -> bool {
    installed_json_version_is_older(
        "codex",
        &["plugin", "list", "--json"],
        |value| {
            installed_plugin(value)
                .and_then(|plugin| plugin.get("version"))
                .and_then(Value::as_str)
        },
        PLUGIN_MANIFEST,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    #[test]
    fn installs_from_the_published_marketplace_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!({"marketplaces": []})]);

        enable(&mut runner).unwrap();

        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
    }

    #[test]
    fn refreshes_the_published_marketplace_and_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!({
            "marketplaces": [{
                "name": MARKETPLACE_NAME,
                "marketplaceSource": {
                    "sourceType": "github",
                    "source": MARKETPLACE_SOURCE
                }
            }]
        })]);

        enable(&mut runner).unwrap();

        assert!(runner.called("codex plugin marketplace upgrade braintrust-codex-plugins"));
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
        assert!(!runner.called("codex plugin marketplace remove braintrust-codex-plugins"));
    }

    #[test]
    fn replaces_a_same_name_local_marketplace() {
        let mut runner = FakeRunner::new([serde_json::json!({
            "marketplaces": [{
                "name": MARKETPLACE_NAME,
                "marketplaceSource": {"sourceType": "local", "source": "/tmp/stale"}
            }]
        })]);

        enable(&mut runner).unwrap();

        assert!(runner.called("codex plugin marketplace remove braintrust-codex-plugins"));
        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
    }

    #[test]
    fn update_refuses_to_install_a_missing_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!({"installed": []})]);

        assert!(update(&mut runner).is_err());
        assert!(!runner
            .calls
            .iter()
            .any(|call| call.starts_with("codex plugin add ")));
    }

    #[test]
    fn update_refreshes_the_published_marketplace_then_the_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!({"installed": [{"pluginId": PLUGIN}]}),
            serde_json::json!({
                "marketplaces": [{
                    "name": MARKETPLACE_NAME,
                    "marketplaceSource": {"source": MARKETPLACE_SOURCE}
                }]
            }),
        ]);

        update(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "codex plugin list --json",
                "codex plugin marketplace list --json",
                "codex plugin marketplace upgrade braintrust-codex-plugins",
                "codex plugin add trace-codex@braintrust-codex-plugins",
            ]
        );
    }

    #[test]
    fn disable_uses_the_remove_command() {
        let mut runner = FakeRunner::new([serde_json::json!({
            "installed": [{"pluginId": PLUGIN}]
        })]);
        disable(&mut runner).unwrap();
        assert!(runner.called("codex plugin remove trace-codex@braintrust-codex-plugins --json"));
    }
}
