//! Grok: the trace-grok plugin installed from its GitHub repository.

use super::common::{github_repo_matches, CommandRunner};
use anyhow::bail;
use serde_json::Value;

const PLUGIN: &str = "trace-grok";
const PLUGIN_SOURCE: &str = "braintrustdata/braintrust-grok-plugin";

fn installed_plugin(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(PLUGIN))
}

fn is_published(item: &Value) -> bool {
    item.get("source")
        .and_then(Value::as_str)
        .is_some_and(|source| github_repo_matches(source, PLUGIN_SOURCE))
}

fn list_plugins(runner: &mut impl CommandRunner) -> anyhow::Result<Value> {
    runner.json("grok", &["plugin", "list", "--json"])
}

pub(super) fn enable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = list_plugins(runner)?;
    // `bt trace enable grok` is the user's trust boundary. Grok's `--trust`
    // applies to this plugin installation and does not change folder trust.
    match installed_plugin(&plugins) {
        Some(plugin) if is_published(plugin) => {
            runner.run("grok", &["plugin", "update", PLUGIN])?;
        }
        Some(_) => {
            runner.run("grok", &["plugin", "uninstall", PLUGIN, "--confirm"])?;
            runner.run("grok", &["plugin", "install", PLUGIN_SOURCE, "--trust"])?;
        }
        None => runner.run("grok", &["plugin", "install", PLUGIN_SOURCE, "--trust"])?,
    }
    runner.run("grok", &["plugin", "enable", PLUGIN])
}

pub(super) fn disable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    if installed_plugin(&list_plugins(runner)?).is_some_and(is_published) {
        runner.run("grok", &["plugin", "uninstall", PLUGIN, "--confirm"])?;
    }
    Ok(())
}

pub(super) fn update(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = list_plugins(runner)?;
    let plugin = installed_plugin(&plugins).ok_or_else(|| {
        anyhow::anyhow!("Grok tracing plugin is not installed; run `bt trace enable grok`")
    })?;
    if !is_published(plugin) {
        bail!(
            "Grok tracing plugin is not the published Braintrust plugin; run `bt trace enable grok`"
        );
    }
    runner.run("grok", &["plugin", "update", PLUGIN])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    #[test]
    fn installs_published_plugin_then_enables_it_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        enable(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "grok plugin list --json",
                "grok plugin install braintrustdata/braintrust-grok-plugin --trust",
                "grok plugin enable trace-grok",
            ]
        );
    }

    #[test]
    fn repeated_enable_updates_and_enables_the_published_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": PLUGIN,
                "source": "https://github.com/braintrustdata/braintrust-grok-plugin.git",
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        enable(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "grok plugin list --json",
                "grok plugin update trace-grok",
                "grok plugin enable trace-grok",
            ]
        );
    }

    #[test]
    fn update_requires_the_published_plugin_without_enabling_it() {
        let mut runner = FakeRunner::new([serde_json::json!([{
            "name": PLUGIN,
            "source": PLUGIN_SOURCE,
            "status": "installed"
        }])]);

        update(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            ["grok plugin list --json", "grok plugin update trace-grok"]
        );
    }

    #[test]
    fn enable_reconciles_only_a_conflicting_same_name_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": PLUGIN,
                "source": "/tmp/local-trace-grok",
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        enable(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "grok plugin list --json",
                "grok plugin uninstall trace-grok --confirm",
                "grok plugin install braintrustdata/braintrust-grok-plugin --trust",
                "grok plugin enable trace-grok",
            ]
        );
    }

    #[test]
    fn disable_removes_only_the_published_braintrust_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": PLUGIN,
                "source": PLUGIN_SOURCE,
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        disable(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "grok plugin list --json",
                "grok plugin uninstall trace-grok --confirm",
            ]
        );

        let mut local = FakeRunner::new([serde_json::json!([{
            "name": PLUGIN,
            "source": "/tmp/local-trace-grok",
            "status": "installed"
        }])]);
        disable(&mut local).unwrap();
        assert_eq!(local.calls, ["grok plugin list --json"]);
    }
}
