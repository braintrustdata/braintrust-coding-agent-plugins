//! Codex: the trace-codex plugin from the published Codex marketplace.

use super::common::{
    github_repo_matches, installed_json_version_is_older, plugin_source, CommandRunner, Marketplace,
};
use crate::agents::Codex;
use crate::setup::Setup;
use serde::Deserialize;

const MARKETPLACE_NAME: &str = "braintrust-codex-plugins";
const MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
// An unfiltered list also fetches Codex's remote catalog. All our checks need
// only this configured marketplace, including doctor's plugin version probe.
const LIST_PLUGINS_ARGS: &[&str] = &[
    "plugin",
    "list",
    "--marketplace",
    MARKETPLACE_NAME,
    "--json",
];
const PLUGIN_MANIFEST: &str =
    plugin_source!("codex/content/plugins/trace-codex/.codex-plugin/plugin.json");

#[derive(Deserialize)]
struct MarketplaceList {
    marketplaces: Vec<MarketplaceEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketplaceEntry {
    name: String,
    marketplace_source: Option<MarketplaceSource>,
}

#[derive(Deserialize)]
struct MarketplaceSource {
    source: Option<String>,
}

#[derive(Deserialize)]
struct InstalledPlugins {
    installed: Vec<InstalledPlugin>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstalledPlugin {
    plugin_id: String,
    version: Option<String>,
}

const MARKETPLACE: Marketplace<MarketplaceList, MarketplaceEntry> = Marketplace {
    program: "codex",
    name: MARKETPLACE_NAME,
    source: MARKETPLACE_SOURCE,
    refresh: "upgrade",
    find: |list| {
        list.marketplaces
            .iter()
            .find(|entry| entry.name == MARKETPLACE_NAME)
    },
    is_published: |entry| {
        entry
            .marketplace_source
            .as_ref()
            .and_then(|source| source.source.as_deref())
            .is_some_and(|source| github_repo_matches(source, MARKETPLACE_SOURCE))
    },
};

fn installed_plugin(plugins: &InstalledPlugins) -> Option<&InstalledPlugin> {
    plugins
        .installed
        .iter()
        .find(|plugin| plugin.plugin_id == PLUGIN)
}

fn list_plugins(runner: &mut dyn CommandRunner) -> anyhow::Result<InstalledPlugins> {
    Ok(serde_json::from_value(
        runner.json("codex", LIST_PLUGINS_ARGS)?,
    )?)
}

impl Setup for Codex {
    fn enable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        MARKETPLACE.reconcile(runner)?;
        // Adding is idempotent and reconciles the installed cache to the refreshed
        // marketplace snapshot.
        runner.run("codex", &["plugin", "add", PLUGIN])
    }

    fn disable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        if installed_plugin(&list_plugins(runner)?).is_some() {
            runner.run("codex", &["plugin", "remove", PLUGIN, "--json"])?;
        }
        Ok(())
    }

    fn update(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        if installed_plugin(&list_plugins(runner)?).is_none() {
            // Setup owns the desired state: repair a removed plugin the same
            // way enable does, then let the caller reapply and verify trust.
            return self.enable(runner);
        }
        MARKETPLACE.refresh_published(runner, "Codex", "codex")?;
        runner.run("codex", &["plugin", "add", PLUGIN])
    }

    fn stale(&self) -> bool {
        installed_json_version_is_older(
            "codex",
            LIST_PLUGINS_ARGS,
            |value| installed_plugin(value).and_then(|plugin| plugin.version.as_deref()),
            PLUGIN_MANIFEST,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    #[test]
    fn installs_from_the_published_marketplace_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!({"marketplaces": []})]);

        Codex.enable(&mut runner).unwrap();

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

        Codex.enable(&mut runner).unwrap();

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

        Codex.enable(&mut runner).unwrap();

        assert!(runner.called("codex plugin marketplace remove braintrust-codex-plugins"));
        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
    }

    #[test]
    fn update_repairs_a_missing_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!({"installed": []}),
            serde_json::json!({"marketplaces": []}),
        ]);

        Codex.update(&mut runner).unwrap();

        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
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

        Codex.update(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "codex plugin list --marketplace braintrust-codex-plugins --json",
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
        Codex.disable(&mut runner).unwrap();
        assert!(runner.called("codex plugin list --marketplace braintrust-codex-plugins --json"));
        assert!(runner.called("codex plugin remove trace-codex@braintrust-codex-plugins --json"));
    }
}
