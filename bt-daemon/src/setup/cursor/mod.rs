//! Cursor: a local plugin written to `~/.cursor/plugins/local/trace-cursor`
//! from files embedded at build time, plus user-level discovery hooks.

mod hooks;

pub(crate) use hooks::{discovery_hooks_are_installed_at, CursorManagedHooks};

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
const HOOKS_MANIFEST: &str = plugin_source!("cursor/content/hooks/hooks.json");
const HOOK_LAUNCHER: &str = plugin_source!("cursor/content/hooks/trace.sh");
const POWERSHELL_LAUNCHER: &str = r#"$bt = if ($env:BT_BIN) { $env:BT_BIN } else { 'bt' }
$hookArgs = @('trace', 'hook', '--source', 'cursor', '--session-id-field', 'conversation_id', '--event-field', 'hook_event_name', '--transcript-path-field', 'transcript_path', '--flush-on-turn-end', '--capture-timeout-ms', '8000')
try { & $bt @hookArgs *> $null } catch {}
if ($args.Count -gt 0 -and $args[0] -eq 'beforeSubmitPrompt') {
  [Console]::Out.WriteLine('{"continue":true}')
} else {
  [Console]::Out.WriteLine('{}')
}
exit 0
"#;
const README: &str = plugin_source!("cursor/content/README.md");
const LICENSE: &str = plugin_source!("cursor/content/LICENSE");
const MCP_MANIFEST: &str = plugin_source!("cursor/content/mcp.json");
const LOGO: &str = plugin_source!("cursor/content/logo.svg");

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

pub(crate) fn plugin_is_installed_at(plugin: &Path) -> bool {
    plugin_is_installed_for_platform_at(plugin, cfg!(windows))
}

fn plugin_is_installed_for_platform_at(plugin: &Path, windows: bool) -> bool {
    if !installed_manifest_at(plugin).is_some_and(|manifest| is_ours(&manifest)) {
        return false;
    }
    let Ok(actual) = std::fs::read(plugin.join("hooks/hooks.json")) else {
        return false;
    };
    let Ok(actual) = serde_json::from_slice::<Value>(&actual) else {
        return false;
    };
    let Ok(expected) = hooks_manifest_for_platform(plugin, windows) else {
        return false;
    };
    let Ok(expected) = serde_json::from_str::<Value>(&expected) else {
        return false;
    };
    if actual != expected {
        return false;
    }
    let (launcher, expected_contents) = if windows {
        ("hooks/trace.ps1", POWERSHELL_LAUNCHER)
    } else {
        ("hooks/trace.sh", HOOK_LAUNCHER)
    };
    let Ok(contents) = std::fs::read_to_string(plugin.join(launcher)) else {
        return false;
    };
    if contents != expected_contents {
        return false;
    }
    if std::fs::read_to_string(plugin.join("mcp.json"))
        .ok()
        .as_deref()
        != Some(MCP_MANIFEST)
    {
        return false;
    }
    if std::fs::read_to_string(plugin.join("logo.svg"))
        .ok()
        .as_deref()
        != Some(LOGO)
    {
        return false;
    }
    #[cfg(unix)]
    if !windows {
        use std::os::unix::fs::PermissionsExt;
        return std::fs::metadata(plugin.join(launcher))
            .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0);
    }
    true
}

impl Setup for Cursor {
    fn enable(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        enable_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())
    }

    fn disable(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        disable_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())
    }

    fn update(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        update_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())
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
        (!plugin_is_installed_at(&paths::cursor_plugin_dir())
            || !discovery_hooks_are_installed_at(&paths::cursor_config_dir()))
        .then_some(
            "Cursor tracing plugin or lifecycle hooks are missing; run `bt trace enable cursor`",
        )
    }
}

/// The plugin's hook manifest. On Windows each hook runs the PowerShell
/// launcher by absolute path instead of the shell script.
fn hooks_manifest_for_platform(plugin_dir: &Path, windows: bool) -> anyhow::Result<String> {
    if !windows {
        return Ok(HOOKS_MANIFEST.to_owned());
    }
    let mut manifest: Value = serde_json::from_str(HOOKS_MANIFEST)?;
    let script = plugin_dir.join("hooks/trace.ps1");
    let script = script.to_string_lossy().replace('\\', "/");
    let script = format!("\"{}\"", script.replace('"', "\"\""));
    for (event, entries) in manifest["hooks"]
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("Cursor hook manifest has no hooks object"))?
    {
        let command =
            format!("powershell.exe -NoProfile -ExecutionPolicy Bypass -File {script} {event}");
        let first = entries
            .as_array_mut()
            .and_then(|entries| entries.first_mut())
            .ok_or_else(|| anyhow::anyhow!("Cursor hook {event} has no registrations"))?;
        first["command"] = Value::String(command);
    }
    Ok(serde_json::to_string_pretty(&manifest)?)
}

fn install_plugin_at(plugin_dir: &Path) -> anyhow::Result<()> {
    install_plugin_at_for_platform(plugin_dir, cfg!(windows))
}

/// Write the plugin into a staging directory beside `plugin_dir`, then swap
/// it into place, restoring the previous plugin if the swap fails.
fn install_plugin_at_for_platform(plugin_dir: &Path, windows: bool) -> anyhow::Result<()> {
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
    std::fs::create_dir_all(staged_plugin.join(".cursor-plugin"))?;
    std::fs::create_dir_all(staged_plugin.join("hooks"))?;
    let hooks_manifest = hooks_manifest_for_platform(plugin_dir, windows)?;
    let mut files = vec![
        (".cursor-plugin/plugin.json", PLUGIN_MANIFEST.to_owned()),
        ("hooks/hooks.json", hooks_manifest),
        ("hooks/trace.sh", HOOK_LAUNCHER.to_owned()),
        ("mcp.json", MCP_MANIFEST.to_owned()),
        ("logo.svg", LOGO.to_owned()),
        ("README.md", README.to_owned()),
        ("LICENSE", LICENSE.to_owned()),
    ];
    if windows {
        files.push(("hooks/trace.ps1", POWERSHELL_LAUNCHER.to_owned()));
    }
    for (relative, contents) in files {
        std::fs::write(staged_plugin.join(relative), contents)
            .with_context(|| format!("failed to write Cursor plugin file {relative}"))?;
        if relative == "hooks/trace.sh" && !windows {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    staged_plugin.join(relative),
                    std::fs::Permissions::from_mode(0o755),
                )
                .with_context(|| format!("failed to make Cursor hook executable: {relative}"))?;
            }
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

fn update_plugin_at(plugin_dir: &Path) -> anyhow::Result<()> {
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
    install_plugin_at(plugin_dir)
}

/// Change the plugin with `change_plugin`, then reconcile the discovery
/// hooks. The hooks file is validated first, so an unusable `hooks.json`
/// fails before the plugin is touched.
fn with_discovery_hooks(
    plugin_dir: &Path,
    config_dir: &Path,
    install: bool,
    change_plugin: impl FnOnce(&Path) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let specs = hooks::persistent_specs();
    hooks::validate(config_dir, &specs, install)?;
    change_plugin(plugin_dir)?;
    hooks::apply(config_dir, &specs, install)
}

fn enable_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    with_discovery_hooks(plugin_dir, config_dir, true, install_plugin_at)
}

fn disable_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    with_discovery_hooks(plugin_dir, config_dir, false, remove_plugin_at)
}

fn update_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    with_discovery_hooks(plugin_dir, config_dir, true, update_plugin_at)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_is_repeatable_and_preserves_neighbor_plugins() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins/local");
        let plugin = root.join(PLUGIN_NAME);
        let neighbor = root.join("my-plugin/keep.txt");
        std::fs::create_dir_all(neighbor.parent().unwrap()).unwrap();
        std::fs::write(&neighbor, "leave me").unwrap();

        install_plugin_at(&plugin).unwrap();
        install_plugin_at(&plugin).unwrap();

        assert_eq!(std::fs::read_to_string(&neighbor).unwrap(), "leave me");
        assert_eq!(
            std::fs::read_to_string(plugin.join("hooks/hooks.json")).unwrap(),
            hooks_manifest_for_platform(&plugin, cfg!(windows)).unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_ne!(
                std::fs::metadata(plugin.join("hooks/trace.sh"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o111,
                0
            );
        }
        let manifest: Value = serde_json::from_slice(
            &std::fs::read(plugin.join(".cursor-plugin/plugin.json")).unwrap(),
        )
        .unwrap();
        assert!(plugin_is_installed_at(&plugin));
        assert!(is_ours(&manifest));
        assert!(plugin.join("mcp.json").is_file());
    }

    #[test]
    fn persistent_install_generates_a_windows_native_launcher() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("Cursor Plugins/trace-cursor");
        install_plugin_at_for_platform(&plugin, true).unwrap();

        let manifest: Value =
            serde_json::from_slice(&std::fs::read(plugin.join("hooks/hooks.json")).unwrap())
                .unwrap();
        for (event, entries) in manifest["hooks"].as_object().unwrap() {
            let command = entries[0]["command"].as_str().unwrap();
            assert!(
                command.starts_with("powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"")
            );
            assert!(command.contains("Cursor Plugins/trace-cursor/hooks/trace.ps1\""));
            assert!(command.ends_with(&format!(" {event}")));
            assert!(!command.contains("trace.sh"));
        }
        let script = std::fs::read_to_string(plugin.join("hooks/trace.ps1")).unwrap();
        assert!(script.contains("$env:BT_BIN"));
        assert!(script.contains("'--source', 'cursor'"));
        assert!(script.contains("'--event-field', 'hook_event_name'"));
        assert!(script.contains("'{\"continue\":true}'") || script.contains("\"continue\":true"));
        assert!(script.contains("[Console]::Out.WriteLine('{}')"));
    }

    #[test]
    fn plugin_health_requires_valid_platform_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join(PLUGIN_NAME);
        install_plugin_at_for_platform(&plugin, false).unwrap();
        assert!(plugin_is_installed_for_platform_at(&plugin, false));

        std::fs::write(plugin.join("hooks/hooks.json"), "{}").unwrap();
        assert!(!plugin_is_installed_for_platform_at(&plugin, false));
        install_plugin_at_for_platform(&plugin, false).unwrap();
        std::fs::write(plugin.join("hooks/trace.sh"), "broken launcher").unwrap();
        assert!(!plugin_is_installed_for_platform_at(&plugin, false));

        install_plugin_at_for_platform(&plugin, true).unwrap();
        assert!(plugin_is_installed_for_platform_at(&plugin, true));
        std::fs::write(plugin.join("hooks/trace.ps1"), "broken launcher").unwrap();
        assert!(!plugin_is_installed_for_platform_at(&plugin, true));
    }

    #[test]
    fn enable_installs_the_plugin_and_discovery_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join(PLUGIN_NAME);
        let config_dir = temp.path().join(".cursor");

        enable_at(&plugin, &config_dir).unwrap();

        assert!(plugin_is_installed_at(&plugin));
        assert!(discovery_hooks_are_installed_at(&config_dir));
    }

    #[test]
    fn setup_refuses_to_replace_or_remove_an_unrelated_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join(PLUGIN_NAME);
        std::fs::create_dir_all(plugin.join(".cursor-plugin")).unwrap();
        std::fs::write(
            plugin.join(".cursor-plugin/plugin.json"),
            r#"{"name":"trace-cursor","version":"1.0.0","repository":"https://example.com/custom"}"#,
        )
        .unwrap();

        assert!(install_plugin_at(&plugin).is_err());
        assert!(remove_plugin_at(&plugin).is_err());
        assert_eq!(
            update_plugin_at(&plugin).unwrap_err().to_string(),
            "Cursor tracing plugin is not the Braintrust plugin; run `bt trace enable cursor`"
        );
        assert!(!plugin_is_installed_at(&plugin));
        assert!(plugin.join(".cursor-plugin/plugin.json").exists());
    }

    #[test]
    fn disable_removes_only_its_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins/local");
        let plugin = root.join(PLUGIN_NAME);
        let neighbor = root.join("other-plugin/file");
        std::fs::create_dir_all(neighbor.parent().unwrap()).unwrap();
        std::fs::write(&neighbor, "preserve").unwrap();

        install_plugin_at(&plugin).unwrap();
        remove_plugin_at(&plugin).unwrap();

        assert!(!plugin.exists());
        assert_eq!(std::fs::read_to_string(neighbor).unwrap(), "preserve");
    }
}
