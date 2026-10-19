//! Cursor: the published plugin cloned into `~/.cursor/plugins/local/trace-cursor`
//! (or the bundled plugin when Git is missing), plus user-level capture hooks.

mod hooks;

pub(crate) use hooks::{
    capture_hooks_are_installed_at, user_capture_hook_is_installed_at, CursorManagedHooks,
    USER_HOOK_ENV,
};

#[cfg(test)]
pub(crate) use hooks::{capture_hook_entry, persistent_specs};

use crate::agents::Cursor;
use crate::paths;
use crate::setup::common::{
    github_repo_matches, package_version, plugin_source, version_is_older, CommandRunner,
};
use crate::setup::Setup;
use anyhow::{bail, Context};
use serde_json::Value;
use std::path::Path;

const PLUGIN_NAME: &str = "trace-cursor";
const PLUGIN_SOURCE: &str = "braintrustdata/braintrust-cursor-extension";
const LEGACY_PLUGIN_SOURCE: &str = "braintrustdata/braintrust-cursor-plugin";
const MONOREPO_SOURCE: &str = "braintrustdata/braintrust-coding-agent-plugins";
const PLUGIN_MANIFEST: &str = plugin_source!("cursor/content/.cursor-plugin/plugin.json");
// User capture hooks follow the protocol supported by this daemon build.
const HOOKS_MANIFEST: &str = plugin_source!("cursor/content/hooks/hooks.json");
const PLUGIN_URL: &str = "https://github.com/braintrustdata/braintrust-cursor-extension.git";

fn write_embedded_plugin_at(plugin: &Path) -> anyhow::Result<()> {
    for (relative, contents) in [
        (".cursor-plugin/plugin.json", PLUGIN_MANIFEST),
        ("hooks/hooks.json", HOOKS_MANIFEST),
        (
            "hooks/trace.sh",
            plugin_source!("cursor/content/hooks/trace.sh"),
        ),
        ("mcp.json", plugin_source!("cursor/content/mcp.json")),
        ("logo.svg", plugin_source!("cursor/content/logo.svg")),
        ("README.md", plugin_source!("cursor/content/README.md")),
        ("LICENSE", plugin_source!("cursor/content/LICENSE")),
    ] {
        let path = plugin.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, contents)
            .with_context(|| format!("failed to write bundled Cursor plugin file {relative}"))?;
        #[cfg(unix)]
        if relative == "hooks/trace.sh" {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    Ok(())
}

fn installed_manifest_at(plugin: &Path) -> Option<Value> {
    let raw = std::fs::read(plugin.join(".cursor-plugin/plugin.json")).ok()?;
    serde_json::from_slice(&raw).ok()
}

fn is_ours(manifest: &Value) -> bool {
    matches!(
        manifest.get("name").and_then(Value::as_str),
        Some("braintrust" | PLUGIN_NAME)
    ) && manifest
        .get("repository")
        .and_then(Value::as_str)
        .is_some_and(|repo| {
            github_repo_matches(repo, MONOREPO_SOURCE)
                || github_repo_matches(repo, PLUGIN_SOURCE)
                || github_repo_matches(repo, LEGACY_PLUGIN_SOURCE)
        })
}

/// Fail unless the plugin at `plugin_dir` is the Braintrust plugin, so setup
/// never replaces or removes someone else's plugin of the same name.
/// `unrecognized` describes a missing or unreadable manifest and `different`
/// a manifest for another plugin.
fn ensure_ours(plugin_dir: &Path, unrecognized: &str, different: &str) -> anyhow::Result<()> {
    let installed: Value = std::fs::read(plugin_dir.join(".cursor-plugin/plugin.json"))
        .context(unrecognized.to_owned())
        .and_then(|raw| {
            serde_json::from_slice(&raw).context("installed plugin manifest is invalid")
        })?;
    if !is_ours(&installed) {
        bail!("{different}");
    }
    Ok(())
}

impl Setup for Cursor {
    fn enable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        enable_at(
            runner,
            &paths::cursor_plugin_dir(),
            &paths::cursor_config_dir(),
        )
    }

    fn disable(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        disable_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())
    }

    fn update(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        update_at(
            runner,
            &paths::cursor_plugin_dir(),
            &paths::cursor_config_dir(),
        )
    }

    fn stale(&self) -> bool {
        let Some(installed_manifest) = installed_manifest_at(&paths::cursor_plugin_dir()) else {
            return false;
        };
        if !is_ours(&installed_manifest) {
            return false;
        }
        let expected = package_version(PLUGIN_MANIFEST).ok();
        let installed = installed_manifest.get("version").and_then(Value::as_str);
        installed
            .zip(expected.as_deref())
            .is_some_and(|(installed, expected)| version_is_older(installed, expected))
    }

    fn activation_warning(&self) -> Option<&'static str> {
        (!capture_hooks_are_installed_at(&paths::cursor_config_dir()))
            .then_some("Cursor user capture hooks are missing; run `bt trace enable cursor`")
    }
}

/// Fetch the published distribution into a staging directory beside
/// `plugin_dir`, then swap it into place, restoring the previous plugin if
/// the swap fails. When Git is missing, stage the bundled plugin instead.
fn install_plugin_at(runner: &mut dyn CommandRunner, plugin_dir: &Path) -> anyhow::Result<()> {
    let parent = plugin_dir.parent().ok_or_else(|| {
        anyhow::anyhow!("Cursor plugin path has no parent: {}", plugin_dir.display())
    })?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create Cursor plugin directory: {}",
            parent.display()
        )
    })?;

    if plugin_dir.exists() {
        ensure_ours(
            plugin_dir,
            &format!(
                "refusing to replace unrecognized Cursor plugin at {}",
                plugin_dir.display()
            ),
            &format!(
                "refusing to replace a different Cursor plugin at {}; remove it manually first",
                plugin_dir.display()
            ),
        )?;
    }

    let stage = tempfile::Builder::new()
        .prefix(".trace-cursor-")
        .tempdir_in(parent)
        .with_context(|| format!("failed to stage Cursor plugin in {}", parent.display()))?;
    let staged_plugin = stage.path().join(PLUGIN_NAME);
    let destination = staged_plugin
        .to_str()
        .context("Cursor plugin staging path is not valid UTF-8")?;
    match runner.run(
        "git",
        &[
            "-c",
            "core.autocrlf=false",
            "clone",
            "--depth",
            "1",
            "--",
            PLUGIN_URL,
            destination,
        ],
    ) {
        Ok(()) => {}
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            eprintln!(
                "warning: Git was not found on PATH; installing the Cursor plugin bundled with this bt build (v{}), which may be older than the latest published plugin. Install Git and run `bt trace update cursor` to get the latest version.",
                package_version(PLUGIN_MANIFEST)?
            );
            write_embedded_plugin_at(&staged_plugin)?;
        }
        Err(error) => return Err(error).context(
            "failed to download the published Cursor plugin; install Git and check network access",
        ),
    }
    ensure_ours(
        &staged_plugin,
        "published Cursor plugin manifest is missing or unreadable",
        "published Cursor plugin is not the Braintrust plugin",
    )?;
    // Validate before touching the existing installation. Install the whole
    // distribution, including components added after this bt build shipped.
    for (relative, field) in [("hooks/hooks.json", "hooks"), ("mcp.json", "mcpServers")] {
        let path = staged_plugin.join(relative);
        let manifest: Value = serde_json::from_slice(
            &std::fs::read(&path)
                .with_context(|| format!("published Cursor plugin is missing {relative}"))?,
        )
        .with_context(|| format!("published Cursor plugin has invalid {relative}"))?;
        if !manifest.get(field).is_some_and(Value::is_object) {
            bail!("published Cursor plugin {relative} must contain a {field} object");
        }
    }

    let backup = stage.path().join("previous");
    let had_previous = plugin_dir.exists();
    if had_previous {
        std::fs::rename(plugin_dir, &backup).with_context(|| {
            format!(
                "failed to stage existing Cursor plugin at {}",
                plugin_dir.display()
            )
        })?;
    }
    if let Err(error) = std::fs::rename(&staged_plugin, plugin_dir) {
        if had_previous {
            let _ = std::fs::rename(&backup, plugin_dir);
        }
        return Err(error).with_context(|| {
            format!(
                "failed to install Cursor plugin at {}",
                plugin_dir.display()
            )
        });
    }
    if had_previous {
        std::fs::remove_dir_all(backup).with_context(|| {
            format!(
                "failed to remove previous Cursor plugin backup for {}",
                plugin_dir.display()
            )
        })?;
    }
    Ok(())
}

fn remove_plugin_at(plugin_dir: &Path) -> anyhow::Result<()> {
    if !plugin_dir.exists() {
        return Ok(());
    }
    ensure_ours(
        plugin_dir,
        &format!(
            "refusing to remove unrecognized Cursor plugin at {}",
            plugin_dir.display()
        ),
        &format!(
            "refusing to remove a different Cursor plugin at {}",
            plugin_dir.display()
        ),
    )?;
    std::fs::remove_dir_all(plugin_dir)
        .with_context(|| format!("failed to remove Cursor plugin at {}", plugin_dir.display()))
}

fn update_plugin_at(runner: &mut dyn CommandRunner, plugin_dir: &Path) -> anyhow::Result<()> {
    const NOT_INSTALLED: &str =
        "Cursor tracing plugin is not installed; run `bt trace enable cursor`";
    if !plugin_dir.exists() {
        bail!(NOT_INSTALLED);
    }
    ensure_ours(
        plugin_dir,
        NOT_INSTALLED,
        "Cursor tracing plugin is not the Braintrust plugin; run `bt trace enable cursor`",
    )?;
    install_plugin_at(runner, plugin_dir)
}

/// Change the plugin with `change_plugin`, then reconcile the capture
/// hooks. The hooks file is validated first, so an unusable `hooks.json`
/// fails before the plugin is touched.
fn with_capture_hooks(
    plugin_dir: &Path,
    config_dir: &Path,
    install: bool,
    change_plugin: impl FnOnce(&Path) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let specs = hooks::persistent_specs()?;
    hooks::validate(config_dir, specs, install)?;
    change_plugin(plugin_dir)?;
    hooks::apply(config_dir, specs, install)
}

fn enable_at(
    runner: &mut dyn CommandRunner,
    plugin_dir: &Path,
    config_dir: &Path,
) -> anyhow::Result<()> {
    with_capture_hooks(plugin_dir, config_dir, true, |plugin| {
        install_plugin_at(runner, plugin)
    })
}

fn disable_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    with_capture_hooks(plugin_dir, config_dir, false, remove_plugin_at)
}

fn update_at(
    runner: &mut dyn CommandRunner,
    plugin_dir: &Path,
    config_dir: &Path,
) -> anyhow::Result<()> {
    with_capture_hooks(plugin_dir, config_dir, true, |plugin| {
        update_plugin_at(runner, plugin)
    })
}

#[cfg(test)]
mod tests;
