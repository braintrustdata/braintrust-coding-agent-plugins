//! Persistent installation and configuration for coding-agent tracing plugins.

use crate::paths;
use crate::trace_command::{EnableArgs, SetupAgent};
use crate::wire::SessionRoute;
use crate::TraceCommandOutput;
use anyhow::{bail, Context};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};

const CODEX_MARKETPLACE: &str = "braintrust-codex-plugins";
const CODEX_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const CODEX_PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const CLAUDE_MARKETPLACE: &str = "braintrust-claude-plugin";
const CLAUDE_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";
const CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";
const GROK_PLUGIN: &str = "trace-grok";
const GROK_PLUGIN_SOURCE: &str = "braintrustdata/braintrust-grok-plugin";
const OPENCODE_PACKAGE: &str = "@braintrust/trace-opencode";
const PI_PACKAGE: &str = "@braintrust/pi-extension";
const OPENCODE_PACKAGE_MANIFEST: &str =
    include_str!("../../src/plugins/opencode/content/package.json");
const ANTIGRAVITY_PLUGIN: &str = "braintrust-antigravity-tracing";
const LEGACY_CLAUDE_TRACING_ENV_KEYS: [&str; 2] = ["BRAINTRUST_CC_PROJECT", "BRAINTRUST_CC_DEBUG"];
const ANTIGRAVITY_PLUGIN_SOURCE: &str =
    "https://github.com/braintrustdata/braintrust-antigravity-plugin";
const CURSOR_PLUGIN_SOURCE: &str = "braintrustdata/braintrust-cursor-plugin";
const CURSOR_PLUGIN_MANIFEST: &str =
    include_str!("../../src/plugins/cursor/content/.cursor-plugin/plugin.json");
const CURSOR_HOOKS_MANIFEST: &str =
    include_str!("../../src/plugins/cursor/content/hooks/hooks.json");
const CURSOR_HOOK_LAUNCHER: &str = include_str!("../../src/plugins/cursor/content/hooks/trace.sh");
const CURSOR_README: &str = include_str!("../../src/plugins/cursor/content/README.md");
const CURSOR_LICENSE: &str = include_str!("../../src/plugins/cursor/content/LICENSE");
fn package_version(manifest: &str) -> anyhow::Result<String> {
    let manifest = serde_json::from_str::<Value>(manifest)?;
    manifest
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("package manifest has no string version"))
}

fn npm_major_spec(package: &str, manifest: &str) -> anyhow::Result<String> {
    let version = package_version(manifest)?;
    let major = version
        .split_once('.')
        .map(|(major, _)| major)
        .ok_or_else(|| anyhow::anyhow!("package version is not semver: {version}"))?
        .parse::<u64>()
        .with_context(|| format!("package version is not semver: {version}"))?;
    Ok(format!("{package}@^{major}"))
}

fn opencode_plugin_spec() -> anyhow::Result<String> {
    npm_major_spec(OPENCODE_PACKAGE, OPENCODE_PACKAGE_MANIFEST)
}

pub(crate) fn pi_plugin_spec() -> String {
    format!("npm:{PI_PACKAGE}")
}

fn version_is_older(installed: &str, expected: &str) -> bool {
    let parse = |version: &str| {
        let mut parts = version.split(['.', '-', '+']);
        Some((
            parts.next()?.parse::<u64>().ok()?,
            parts.next().unwrap_or("0").parse::<u64>().ok()?,
            parts.next().unwrap_or("0").parse::<u64>().ok()?,
        ))
    };
    parse(installed)
        .zip(parse(expected))
        .is_some_and(|(installed, expected)| installed < expected)
}

pub(crate) fn update_warning(source: &str) -> Option<String> {
    let stale = match source {
        "codex" => installed_json_version("codex", &["plugin", "list", "--json"], |value| {
            codex_plugin(value).and_then(|plugin| plugin.get("version")).and_then(Value::as_str)
        }, &package_version(include_str!("../../src/plugins/codex/content/plugins/trace-codex/.codex-plugin/plugin.json")).ok()?),
        "claude" => installed_json_version("claude", &["plugin", "list", "--json"], |value| {
            claude_plugin(value).and_then(|plugin| plugin.get("version")).and_then(Value::as_str)
        }, &package_version(include_str!("../../src/plugins/claude/content/plugins/trace-claude-code/.claude-plugin/plugin.json")).ok()?),
        "opencode" => opencode_update_required(),
        "pi" => pi_update_required(),
        "cursor" => cursor_update_required(),
        _ => false,
    };
    stale.then(|| format!("tracing plugin is out of date; run `bt trace update {source}`"))
}

fn cursor_installed_manifest_at(plugin: &Path) -> Option<Value> {
    let raw = std::fs::read(plugin.join(".cursor-plugin/plugin.json")).ok()?;
    serde_json::from_slice(&raw).ok()
}

pub(crate) fn cursor_plugin_is_installed_at(plugin: &Path) -> bool {
    cursor_installed_manifest_at(plugin).is_some_and(|manifest| cursor_plugin_is_ours(&manifest))
}

fn cursor_update_required() -> bool {
    let plugin = paths::cursor_plugin_dir();
    let Some(installed_manifest) = cursor_installed_manifest_at(&plugin) else {
        return false;
    };
    if !cursor_plugin_is_ours(&installed_manifest) {
        return false;
    }
    let expected = package_version(CURSOR_PLUGIN_MANIFEST).ok();
    let installed = installed_manifest.get("version").and_then(Value::as_str);
    installed
        .zip(expected.as_deref())
        .is_some_and(|(installed, expected)| version_is_older(installed, expected))
}

fn cursor_plugin_is_ours(manifest: &Value) -> bool {
    manifest.get("name").and_then(Value::as_str) == Some("trace-cursor")
        && manifest
            .get("repository")
            .and_then(Value::as_str)
            .is_some_and(|repo| {
                github_repo_matches(repo, "braintrustdata/braintrust-coding-agent-plugins")
                    || github_repo_matches(repo, CURSOR_PLUGIN_SOURCE)
            })
}

fn install_cursor_plugin_at(plugin_dir: &Path) -> anyhow::Result<()> {
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
        let manifest_path = plugin_dir.join(".cursor-plugin/plugin.json");
        let installed: Value = std::fs::read(&manifest_path)
            .with_context(|| {
                format!(
                    "refusing to replace unrecognized Cursor plugin at {}",
                    plugin_dir.display()
                )
            })
            .and_then(|raw| {
                serde_json::from_slice(&raw).context("installed plugin manifest is invalid")
            })?;
        if !cursor_plugin_is_ours(&installed) {
            bail!(
                "refusing to replace a different Cursor plugin at {}; remove it manually first",
                plugin_dir.display()
            );
        }
    }

    let stage = tempfile::Builder::new()
        .prefix(".trace-cursor-")
        .tempdir_in(parent)
        .with_context(|| format!("failed to stage Cursor plugin in {}", parent.display()))?;
    let staged_plugin = stage.path().join("trace-cursor");
    std::fs::create_dir_all(staged_plugin.join(".cursor-plugin"))?;
    std::fs::create_dir_all(staged_plugin.join("hooks"))?;
    for (relative, contents) in [
        (".cursor-plugin/plugin.json", CURSOR_PLUGIN_MANIFEST),
        ("hooks/hooks.json", CURSOR_HOOKS_MANIFEST),
        ("hooks/trace.sh", CURSOR_HOOK_LAUNCHER),
        ("README.md", CURSOR_README),
        ("LICENSE", CURSOR_LICENSE),
    ] {
        std::fs::write(staged_plugin.join(relative), contents)
            .with_context(|| format!("failed to write Cursor plugin file {relative}"))?;
        if relative == "hooks/trace.sh" {
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

fn setup_cursor_at(plugin_dir: &Path) -> anyhow::Result<()> {
    install_cursor_plugin_at(plugin_dir)
}

fn cursor_discovery_hook_specs(marker: &str, windows: bool) -> Vec<(String, String)> {
    [
        ("beforeSubmitPrompt", r#"'{"continue":true}'"#),
        ("afterAgentResponse", "'{}'"),
        ("stop", "'{}'"),
    ]
    .into_iter()
    .map(|(event, response)| {
        let command = if windows {
            let response = if event == "beforeSubmitPrompt" {
                r#"{"continue":true}"#
            } else {
                "{}"
            };
            powershell_encoded_command(&format!(
                "[Console]::Out.WriteLine('{}') # {marker}",
                response.replace('\'', "''")
            ))
        } else {
            format!("printf '%s\\n' {response} # {marker}")
        };
        (event.to_owned(), command)
    })
    .collect()
}

fn cursor_managed_process_identity(command: &str) -> Option<crate::wire::ProcessIdentity> {
    let decoded = command
        .strip_prefix("powershell.exe -NoProfile -EncodedCommand ")
        .and_then(|encoded| {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            let (chunks, remainder) = bytes.as_chunks::<2>();
            if !remainder.is_empty() {
                return None;
            }
            let words = chunks
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>();
            String::from_utf16(&words).ok()
        });
    let command = decoded.as_deref().unwrap_or(command);
    let marker = command.split("braintrust-cursor-managed-").nth(1)?;
    let mut parts = marker.splitn(3, '-');
    Some(crate::wire::ProcessIdentity {
        pid: parts.next()?.parse().ok()?,
        start_time_secs: parts.next()?.parse().ok()?,
    })
}

fn remove_stale_cursor_managed_hooks(
    config: &mut Map<String, Value>,
    mut process_is_alive: impl FnMut(&crate::wire::ProcessIdentity) -> bool,
) {
    let Some(hooks) = config.get_mut("hooks").and_then(Value::as_object_mut) else {
        return;
    };
    for entries in hooks.values_mut().filter_map(Value::as_array_mut) {
        entries.retain(|entry| {
            cursor_managed_process_identity(
                entry
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .is_none_or(|identity| process_is_alive(&identity))
        });
    }
}

fn update_cursor_managed_hooks_at(
    config_dir: &Path,
    specs: &[(String, String)],
) -> anyhow::Result<()> {
    let path = config_dir.join("hooks.json");
    crate::settings::with_settings_lock(&path, || {
        let mut config = load_object(&path)?;
        let original = config.clone();
        remove_stale_cursor_managed_hooks(&mut config, crate::process::process_is_alive);
        apply_cursor_hook_specs(&mut config, &path, specs, true)?;
        if config != original {
            write_object_atomic_unlocked(&path, config, FileAccess::Inherited)?;
        }
        Ok(())
    })
}

fn powershell_encoded_command(script: &str) -> String {
    use base64::Engine;
    let utf16 = script
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    format!(
        "powershell.exe -NoProfile -EncodedCommand {}",
        base64::engine::general_purpose::STANDARD.encode(utf16)
    )
}

pub(crate) fn cursor_discovery_hooks_are_installed_at(config_dir: &Path) -> bool {
    let path = config_dir.join("hooks.json");
    let Ok(config) = load_object(&path) else {
        return false;
    };
    let Some(hooks) = config.get("hooks").and_then(Value::as_object) else {
        return false;
    };
    cursor_discovery_hook_specs("braintrust-cursor-discovery", cfg!(windows))
        .iter()
        .all(|(event, command)| {
            hooks
                .get(event)
                .and_then(Value::as_array)
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("command").and_then(Value::as_str) == Some(command.as_str())
                    })
                })
        })
}

fn apply_cursor_hook_specs(
    config: &mut Map<String, Value>,
    path: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    if enable {
        config
            .entry("version")
            .or_insert_with(|| Value::Number(1.into()));
    }

    if enable || config.contains_key("hooks") {
        let hooks = config
            .entry("hooks")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .ok_or_else(|| {
                anyhow::anyhow!("Cursor hooks must be a JSON object: {}", path.display())
            })?;

        for (event, command) in specs {
            let Some(entries) = hooks.get_mut(event) else {
                if enable {
                    hooks.insert(
                        event.clone(),
                        Value::Array(vec![cursor_discovery_hook(command)]),
                    );
                }
                continue;
            };
            let entries = entries.as_array_mut().ok_or_else(|| {
                anyhow::anyhow!(
                    "Cursor hook event `{event}` must be an array: {}",
                    path.display()
                )
            })?;
            entries.retain(|entry| {
                entry.get("command").and_then(Value::as_str) != Some(command.as_str())
            });
            if enable {
                entries.push(cursor_discovery_hook(command));
            }
        }
    }

    Ok(())
}

fn validate_cursor_hooks_at(
    config_dir: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    let path = config_dir.join("hooks.json");
    let mut config = load_object(&path)?;
    apply_cursor_hook_specs(&mut config, &path, specs, enable)
}

fn update_cursor_hooks_at(
    config_dir: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    let path = config_dir.join("hooks.json");
    crate::settings::with_settings_lock(&path, || {
        let mut config = load_object(&path)?;
        let original = config.clone();
        apply_cursor_hook_specs(&mut config, &path, specs, enable)?;
        if config != original {
            write_object_atomic_unlocked(&path, config, FileAccess::Inherited)?;
        }
        Ok(())
    })
}

fn cursor_discovery_hook(command: &str) -> Value {
    serde_json::json!({
        "command": command,
        "timeout": 10,
        "failClosed": false
    })
}

fn setup_cursor_with_hooks_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    let hooks = cursor_discovery_hook_specs("braintrust-cursor-discovery", cfg!(windows));
    validate_cursor_hooks_at(config_dir, &hooks, true)?;
    setup_cursor_at(plugin_dir)?;
    update_cursor_hooks_at(config_dir, &hooks, true)
}

pub(crate) struct CursorManagedHooks {
    config_dir: PathBuf,
    specs: Vec<(String, String)>,
}

impl CursorManagedHooks {
    pub(crate) fn install(config_dir: &Path, run_id: &str) -> anyhow::Result<Self> {
        let context = crate::process::capture_process_context(std::process::id());
        let identity = context
            .process_chain
            .first()
            .filter(|identity| identity.start_time_secs > 0);
        let marker = identity
            .map(|identity| {
                format!(
                    "braintrust-cursor-managed-{}-{}-{run_id}",
                    identity.pid, identity.start_time_secs
                )
            })
            .unwrap_or_else(|| format!("braintrust-cursor-managed-{run_id}"));
        let specs = cursor_discovery_hook_specs(&marker, cfg!(windows));
        update_cursor_managed_hooks_at(config_dir, &specs)?;
        Ok(Self {
            config_dir: config_dir.to_path_buf(),
            specs,
        })
    }
}

impl Drop for CursorManagedHooks {
    fn drop(&mut self) {
        if let Err(error) = update_cursor_hooks_at(&self.config_dir, &self.specs, false) {
            tracing::warn!(%error, "failed to remove managed Cursor hook discovery entries");
        }
    }
}

fn disable_cursor_at(plugin_dir: &Path) -> anyhow::Result<()> {
    if !plugin_dir.exists() {
        return Ok(());
    }
    let manifest_path = plugin_dir.join(".cursor-plugin/plugin.json");
    let installed: Value = std::fs::read(&manifest_path)
        .with_context(|| {
            format!(
                "refusing to remove unrecognized Cursor plugin at {}",
                plugin_dir.display()
            )
        })
        .and_then(|raw| {
            serde_json::from_slice(&raw).context("installed plugin manifest is invalid")
        })?;
    if !cursor_plugin_is_ours(&installed) {
        bail!(
            "refusing to remove a different Cursor plugin at {}",
            plugin_dir.display()
        );
    }
    std::fs::remove_dir_all(plugin_dir)
        .with_context(|| format!("failed to remove Cursor plugin at {}", plugin_dir.display()))
}

fn disable_cursor_with_hooks_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    let hooks = cursor_discovery_hook_specs("braintrust-cursor-discovery", cfg!(windows));
    validate_cursor_hooks_at(config_dir, &hooks, false)?;
    disable_cursor_at(plugin_dir)?;
    update_cursor_hooks_at(config_dir, &hooks, false)
}

fn update_cursor_at(plugin_dir: &Path) -> anyhow::Result<()> {
    if !plugin_dir.exists() {
        bail!("Cursor tracing plugin is not installed; run `bt trace enable cursor`");
    }
    let manifest_path = plugin_dir.join(".cursor-plugin/plugin.json");
    let installed: Value = std::fs::read(&manifest_path)
        .with_context(|| {
            "Cursor tracing plugin is not installed; run `bt trace enable cursor`".to_owned()
        })
        .and_then(|raw| {
            serde_json::from_slice(&raw).context("installed plugin manifest is invalid")
        })?;
    if !cursor_plugin_is_ours(&installed) {
        bail!("Cursor tracing plugin is not the Braintrust plugin; run `bt trace enable cursor`");
    }
    install_cursor_plugin_at(plugin_dir)
}

fn update_cursor_with_hooks_at(plugin_dir: &Path, config_dir: &Path) -> anyhow::Result<()> {
    let hooks = cursor_discovery_hook_specs("braintrust-cursor-discovery", cfg!(windows));
    validate_cursor_hooks_at(config_dir, &hooks, true)?;
    update_cursor_at(plugin_dir)?;
    update_cursor_hooks_at(config_dir, &hooks, true)
}

fn installed_json_version(
    program: &str,
    args: &[&str],
    find_version: impl Fn(&Value) -> Option<&str>,
    expected: &str,
) -> bool {
    let output = crate::subprocess::background_command(program)
        .args(args)
        .output();
    let Ok(output) = output else { return false };
    if !output.status.success() {
        return false;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .and_then(|value| find_version(&value).map(str::to_owned))
        .is_some_and(|installed| version_is_older(&installed, expected))
}

fn opencode_update_required() -> bool {
    let settings_path = paths::agent_settings_path("opencode", None);
    let path = settings_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json");
    let Ok(raw) = std::fs::read(path) else {
        return false;
    };
    let Ok(config) = serde_json::from_slice::<Value>(&raw) else {
        return false;
    };
    let expected = opencode_plugin_spec().ok();
    config
        .get("plugin")
        .and_then(Value::as_array)
        .and_then(|plugins| {
            plugins.iter().find_map(|plugin| {
                plugin
                    .as_str()
                    .filter(|plugin| plugin.starts_with(OPENCODE_PACKAGE))
            })
        })
        .is_some_and(|plugin| Some(plugin) != expected.as_deref())
}

fn pi_update_required() -> bool {
    let output = crate::subprocess::background_command("pi")
        .arg("list")
        .output();
    let Ok(output) = output else { return false };
    if !output.status.success() {
        return false;
    }
    let installed = String::from_utf8_lossy(&output.stdout);
    let expected = pi_plugin_spec();
    installed.lines().any(|line| {
        let plugin = line.trim();
        plugin.starts_with(&expected) && plugin != expected
    })
}

trait CommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value>;
    fn json_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<Value>;
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()>;
    fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()>;
}

struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
        let output = crate::subprocess::background_command(program)
            .args(args)
            .output()
            .with_context(|| {
                format!("failed to run `{program}`; install {program} and ensure it is on PATH")
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("`{program} {}` failed: {}", args.join(" "), stderr.trim());
        }
        serde_json::from_slice(&output.stdout)
            .with_context(|| format!("`{program} {}` returned invalid JSON", args.join(" ")))
    }

    fn json_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<Value> {
        let output = crate::subprocess::background_command(program)
            .args(args)
            .env("HOME", home)
            .output()
            .with_context(|| {
                format!("failed to run `{program}`; install {program} and ensure it is on PATH")
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("`{program} {}` failed: {}", args.join(" "), stderr.trim());
        }
        serde_json::from_slice(&output.stdout)
            .with_context(|| format!("`{program} {}` returned invalid JSON", args.join(" ")))
    }

    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
        let status = crate::subprocess::interactive_command(program)
            .args(args)
            .status()
            .with_context(|| {
                format!("failed to run `{program}`; install {program} and ensure it is on PATH")
            })?;
        if !status.success() {
            bail!("`{program} {}` failed with {status}", args.join(" "));
        }
        Ok(())
    }

    fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()> {
        let output = crate::subprocess::background_command(program)
            .args(args)
            .env("HOME", home)
            .output()
            .with_context(|| {
                format!("failed to run `{program}`; install {program} and ensure it is on PATH")
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("`{program} {}` failed: {}", args.join(" "), stderr.trim());
        }
        Ok(())
    }
}

fn github_repo_matches(source: &str, expected: &str) -> bool {
    let source = source.trim().trim_end_matches('/');
    let source = source.strip_suffix(".git").unwrap_or(source);
    let source = source
        .strip_prefix("https://github.com/")
        .or_else(|| source.strip_prefix("git@github.com:"))
        .unwrap_or(source);
    source == expected
}

fn codex_marketplace(value: &Value) -> Option<&Value> {
    value
        .get("marketplaces")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(CODEX_MARKETPLACE))
}

fn codex_marketplace_is_published(item: &Value) -> bool {
    item.get("marketplaceSource")
        .and_then(|source| source.get("source"))
        .and_then(Value::as_str)
        .is_some_and(|source| github_repo_matches(source, CODEX_MARKETPLACE_SOURCE))
}

fn setup_codex(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let marketplaces = runner.json("codex", &["plugin", "marketplace", "list", "--json"])?;
    match codex_marketplace(&marketplaces) {
        Some(marketplace) if codex_marketplace_is_published(marketplace) => runner.run(
            "codex",
            &["plugin", "marketplace", "upgrade", CODEX_MARKETPLACE],
        )?,
        Some(_) => {
            runner.run(
                "codex",
                &["plugin", "marketplace", "remove", CODEX_MARKETPLACE],
            )?;
            runner.run(
                "codex",
                &["plugin", "marketplace", "add", CODEX_MARKETPLACE_SOURCE],
            )?;
        }
        None => runner.run(
            "codex",
            &["plugin", "marketplace", "add", CODEX_MARKETPLACE_SOURCE],
        )?,
    }

    // Adding is idempotent and reconciles the installed cache to the refreshed
    // marketplace snapshot.
    runner.run("codex", &["plugin", "add", CODEX_PLUGIN])
}

fn codex_plugin(value: &Value) -> Option<&Value> {
    value
        .get("installed")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| item.get("pluginId").and_then(Value::as_str) == Some(CODEX_PLUGIN))
}

fn disable_codex(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("codex", &["plugin", "list", "--json"])?;
    if codex_plugin(&plugins).is_some() {
        runner.run("codex", &["plugin", "remove", CODEX_PLUGIN, "--json"])?;
    }
    Ok(())
}

fn update_codex(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("codex", &["plugin", "list", "--json"])?;
    if codex_plugin(&plugins).is_none() {
        bail!("Codex tracing plugin is not installed; run `bt trace enable codex`");
    }
    let marketplaces = runner.json("codex", &["plugin", "marketplace", "list", "--json"])?;
    let marketplace = codex_marketplace(&marketplaces).ok_or_else(|| {
        anyhow::anyhow!("Codex tracing marketplace is not installed; run `bt trace enable codex`")
    })?;
    if !codex_marketplace_is_published(marketplace) {
        bail!(
            "Codex tracing marketplace is not the published Braintrust marketplace; run `bt trace enable codex`"
        );
    }
    runner.run(
        "codex",
        &["plugin", "marketplace", "upgrade", CODEX_MARKETPLACE],
    )?;
    runner.run("codex", &["plugin", "add", CODEX_PLUGIN])
}

fn claude_marketplace(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(CLAUDE_MARKETPLACE))
}

fn claude_marketplace_is_published(item: &Value) -> bool {
    item.get("source").and_then(Value::as_str) == Some("github")
        && item
            .get("repo")
            .and_then(Value::as_str)
            .is_some_and(|repo| github_repo_matches(repo, CLAUDE_MARKETPLACE_SOURCE))
}

fn claude_plugin(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(CLAUDE_PLUGIN))
}

fn setup_claude(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let marketplaces = runner.json("claude", &["plugin", "marketplace", "list", "--json"])?;
    let marketplace_replaced = match claude_marketplace(&marketplaces) {
        Some(marketplace) if claude_marketplace_is_published(marketplace) => {
            runner.run(
                "claude",
                &["plugin", "marketplace", "update", CLAUDE_MARKETPLACE],
            )?;
            false
        }
        Some(_) => {
            runner.run(
                "claude",
                &["plugin", "marketplace", "remove", CLAUDE_MARKETPLACE],
            )?;
            runner.run(
                "claude",
                &["plugin", "marketplace", "add", CLAUDE_MARKETPLACE_SOURCE],
            )?;
            true
        }
        None => {
            runner.run(
                "claude",
                &["plugin", "marketplace", "add", CLAUDE_MARKETPLACE_SOURCE],
            )?;
            false
        }
    };

    // Claude removes a marketplace's installed plugins when that marketplace
    // is removed, so replacing a stale source requires a fresh installation.
    if marketplace_replaced {
        return runner.run("claude", &["plugin", "install", CLAUDE_PLUGIN]);
    }

    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    match claude_plugin(&plugins) {
        None => runner.run("claude", &["plugin", "install", CLAUDE_PLUGIN]),
        Some(plugin) => {
            runner.run("claude", &["plugin", "update", CLAUDE_PLUGIN])?;
            if plugin.get("enabled").and_then(Value::as_bool) == Some(false) {
                runner.run("claude", &["plugin", "enable", CLAUDE_PLUGIN])?;
            }
            Ok(())
        }
    }
}

fn legacy_claude_tracing_env_keys(path: &Path) -> anyhow::Result<Vec<&'static str>> {
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
    Ok(LEGACY_CLAUDE_TRACING_ENV_KEYS
        .into_iter()
        .filter(|key| env.is_some_and(|env| env.contains_key(*key)))
        .collect())
}

fn warn_legacy_claude_tracing_env() {
    let path = paths::claude_settings_path();
    let keys = match legacy_claude_tracing_env_keys(&path) {
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

fn disable_claude(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    if claude_plugin(&plugins).is_some() {
        runner.run("claude", &["plugin", "uninstall", CLAUDE_PLUGIN])?;
    }
    Ok(())
}

fn grok_plugin(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(GROK_PLUGIN))
}

fn grok_plugin_is_published(item: &Value) -> bool {
    item.get("source")
        .and_then(Value::as_str)
        .is_some_and(|source| github_repo_matches(source, GROK_PLUGIN_SOURCE))
}

fn setup_grok(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("grok", &["plugin", "list", "--json"])?;
    // `bt trace enable grok` is the user's trust boundary. Grok's `--trust`
    // applies to this plugin installation and does not change folder trust.
    match grok_plugin(&plugins) {
        Some(plugin) if grok_plugin_is_published(plugin) => {
            runner.run("grok", &["plugin", "update", GROK_PLUGIN])?;
        }
        Some(_) => {
            runner.run("grok", &["plugin", "uninstall", GROK_PLUGIN, "--confirm"])?;
            runner.run(
                "grok",
                &["plugin", "install", GROK_PLUGIN_SOURCE, "--trust"],
            )?;
        }
        None => runner.run(
            "grok",
            &["plugin", "install", GROK_PLUGIN_SOURCE, "--trust"],
        )?,
    }
    runner.run("grok", &["plugin", "enable", GROK_PLUGIN])
}

fn disable_grok(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("grok", &["plugin", "list", "--json"])?;
    if grok_plugin(&plugins).is_some_and(grok_plugin_is_published) {
        runner.run("grok", &["plugin", "uninstall", GROK_PLUGIN, "--confirm"])?;
    }
    Ok(())
}

fn update_grok(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("grok", &["plugin", "list", "--json"])?;
    let plugin = grok_plugin(&plugins).ok_or_else(|| {
        anyhow::anyhow!("Grok tracing plugin is not installed; run `bt trace enable grok`")
    })?;
    if !grok_plugin_is_published(plugin) {
        bail!(
            "Grok tracing plugin is not the published Braintrust plugin; run `bt trace enable grok`"
        );
    }
    runner.run("grok", &["plugin", "update", GROK_PLUGIN])
}

fn update_claude(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    if claude_plugin(&plugins).is_none() {
        bail!("Claude Code tracing plugin is not installed; run `bt trace enable claude`");
    }
    let marketplaces = runner.json("claude", &["plugin", "marketplace", "list", "--json"])?;
    let marketplace = claude_marketplace(&marketplaces).ok_or_else(|| {
        anyhow::anyhow!(
            "Claude Code tracing marketplace is not installed; run `bt trace enable claude`"
        )
    })?;
    if !claude_marketplace_is_published(marketplace) {
        bail!(
            "Claude Code tracing marketplace is not the published Braintrust marketplace; run `bt trace enable claude`"
        );
    }
    runner.run(
        "claude",
        &["plugin", "marketplace", "update", CLAUDE_MARKETPLACE],
    )?;
    runner.run("claude", &["plugin", "update", CLAUDE_PLUGIN])
}

fn load_object(path: &Path) -> anyhow::Result<Map<String, Value>> {
    match std::fs::read(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("invalid JSON configuration: {}", path.display()))?;
            value.as_object().cloned().ok_or_else(|| {
                anyhow::anyhow!("configuration must be a JSON object: {}", path.display())
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read configuration: {}", path.display()))
        }
    }
}

/// Whether an atomic replacement keeps the directory's inherited access or is
/// restricted to the current user before it is written and published.
#[derive(Clone, Copy)]
enum FileAccess {
    Inherited,
    OwnerOnly,
}

fn write_object_atomic(path: &Path, object: Map<String, Value>) -> anyhow::Result<()> {
    write_object_atomic_with(path, object, FileAccess::Inherited)
}

fn write_object_atomic_with(
    path: &Path,
    object: Map<String, Value>,
    access: FileAccess,
) -> anyhow::Result<()> {
    crate::settings::with_settings_lock(path, || write_object_atomic_unlocked(path, object, access))
}

fn write_object_atomic_unlocked(
    path: &Path,
    object: Map<String, Value>,
    access: FileAccess,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create configuration directory: {}",
            parent.display()
        )
    })?;
    let mut encoded = serde_json::to_string_pretty(&Value::Object(object))?;
    encoded.push('\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("failed to create temporary file in {}", parent.display()))?;
    if let FileAccess::OwnerOnly = access {
        // Protect the replacement before it holds content or takes the
        // target's place; on failure the previous file stays untouched.
        paths::restrict_file_to_owner(temporary.path())
            .with_context(|| format!("failed to protect {}", path.display()))?;
    }
    temporary.write_all(encoded.as_bytes()).with_context(|| {
        format!(
            "failed to write temporary configuration for {}",
            path.display()
        )
    })?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("failed to replace configuration: {}", path.display()))?;
    Ok(())
}

fn setup_opencode_at(path: &Path) -> anyhow::Result<()> {
    let mut config = load_object(path)?;
    let plugins = config
        .entry("plugin")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "OpenCode `plugin` config must be an array: {}",
                path.display()
            )
        })?;
    plugins.retain(|plugin| {
        plugin.as_str().is_none_or(|plugin| {
            plugin != "@braintrust/trace-opencode"
                && !plugin.starts_with("@braintrust/trace-opencode@")
        })
    });
    plugins.push(Value::String(opencode_plugin_spec()?));
    write_object_atomic(path, config)
}

fn setup_opencode() -> anyhow::Result<()> {
    let settings_path = paths::agent_settings_path("opencode", None);
    let path = settings_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json");
    setup_opencode_at(&path)
}

fn update_opencode_at(path: &Path) -> anyhow::Result<()> {
    let mut config = match std::fs::read(path) {
        Ok(raw) => serde_json::from_slice::<Value>(&raw)
            .with_context(|| format!("invalid JSON configuration: {}", path.display()))?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("configuration must be a JSON object: {}", path.display())
            })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("OpenCode tracing plugin is not installed; run `bt trace enable opencode`")
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read configuration: {}", path.display()));
        }
    };
    let plugins = config
        .get_mut("plugin")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "OpenCode tracing plugin is not installed; run `bt trace enable opencode`"
            )
        })?;
    let found = plugins.iter().any(|plugin| {
        plugin.as_str().is_some_and(|plugin| {
            plugin == OPENCODE_PACKAGE || plugin.starts_with(&format!("{OPENCODE_PACKAGE}@"))
        })
    });
    if !found {
        bail!("OpenCode tracing plugin is not installed; run `bt trace enable opencode`");
    }
    plugins.retain(|plugin| {
        plugin.as_str().is_none_or(|plugin| {
            plugin != OPENCODE_PACKAGE && !plugin.starts_with(&format!("{OPENCODE_PACKAGE}@"))
        })
    });
    plugins.push(Value::String(opencode_plugin_spec()?));
    write_object_atomic(path, config)
}

fn update_opencode() -> anyhow::Result<()> {
    let settings_path = paths::agent_settings_path("opencode", None);
    let path = settings_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json");
    update_opencode_at(&path)
}

fn remove_opencode_plugin_at(path: &Path) -> anyhow::Result<()> {
    let mut config = match std::fs::read(path) {
        Ok(raw) => serde_json::from_slice::<Value>(&raw)
            .with_context(|| format!("invalid JSON configuration: {}", path.display()))?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("configuration must be a JSON object: {}", path.display())
            })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read configuration: {}", path.display()));
        }
    };
    let Some(plugins) = config.get_mut("plugin") else {
        return Ok(());
    };
    let plugins = plugins.as_array_mut().ok_or_else(|| {
        anyhow::anyhow!(
            "OpenCode `plugin` config must be an array: {}",
            path.display()
        )
    })?;
    let original_len = plugins.len();
    plugins.retain(|plugin| {
        plugin.as_str().is_none_or(|plugin| {
            plugin != "@braintrust/trace-opencode"
                && !plugin.starts_with("@braintrust/trace-opencode@")
        })
    });
    if plugins.len() != original_len {
        write_object_atomic(path, config)?;
    }
    Ok(())
}

fn disable_opencode() -> anyhow::Result<()> {
    let settings_path = paths::agent_settings_path("opencode", None);
    let path = settings_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json");
    remove_opencode_plugin_at(&path)
}

fn setup_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugin = pi_plugin_spec();
    runner.run("pi", &["install", &plugin])
}

fn disable_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugin = pi_plugin_spec();
    runner.run("pi", &["uninstall", &plugin])
}

fn update_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugin = pi_plugin_spec();
    runner.run("pi", &["update", &plugin])
}

fn antigravity_home(config_dir: &Path) -> anyhow::Result<&Path> {
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

fn remove_legacy_antigravity_registration(config_dir: &Path) -> anyhow::Result<()> {
    let hooks_path = config_dir.join("hooks.json");
    if !hooks_path.exists() {
        return Ok(());
    }
    let mut hooks = load_object(&hooks_path)?;
    if hooks.remove(ANTIGRAVITY_PLUGIN).is_some() {
        write_object_atomic(&hooks_path, hooks)?;
    }
    Ok(())
}

fn setup_antigravity_at(runner: &mut impl CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    runner.run_in_home(
        "agy",
        &["plugin", "install", ANTIGRAVITY_PLUGIN_SOURCE],
        antigravity_home(config_dir)?,
    )?;
    runner.run_in_home(
        "agy",
        &["plugin", "enable", ANTIGRAVITY_PLUGIN],
        antigravity_home(config_dir)?,
    )?;

    remove_legacy_antigravity_registration(config_dir)
}

fn setup_antigravity(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    setup_antigravity_at(runner, &paths::antigravity_config_dir())
}

fn disable_antigravity_at(
    runner: &mut impl CommandRunner,
    config_dir: &Path,
) -> anyhow::Result<()> {
    // Antigravity's uninstall command is idempotent when the plugin is absent.
    runner.run_in_home(
        "agy",
        &["plugin", "uninstall", ANTIGRAVITY_PLUGIN],
        antigravity_home(config_dir)?,
    )?;
    remove_legacy_antigravity_registration(config_dir)
}

fn disable_antigravity(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    disable_antigravity_at(runner, &paths::antigravity_config_dir())
}

fn update_antigravity_at(runner: &mut impl CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    let home = antigravity_home(config_dir)?;
    let plugins = runner.json_in_home("agy", &["plugin", "list"], home)?;
    let installed = plugins
        .get("imports")
        .and_then(Value::as_array)
        .is_some_and(|imports| {
            imports.iter().any(|plugin| {
                plugin.get("name").and_then(Value::as_str) == Some(ANTIGRAVITY_PLUGIN)
            })
        });
    if !installed {
        bail!(
            "Google Antigravity tracing plugin is not installed; run `bt trace enable antigravity`"
        );
    }
    runner.run_in_home(
        "agy",
        &["plugin", "install", ANTIGRAVITY_PLUGIN_SOURCE],
        home,
    )
}

fn update_antigravity(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    update_antigravity_at(runner, &paths::antigravity_config_dir())
}

fn enable_tracing_at(path: &Path, mut route: SessionRoute) -> anyhow::Result<()> {
    let mut settings = load_object(path)?;
    if route.additional_metadata.is_none() {
        route.additional_metadata = settings
            .get("route")
            .and_then(|route| route.get("additional_metadata"))
            .or_else(|| settings.get("additional_metadata"))
            .filter(|metadata| metadata.is_object())
            .cloned();
    }
    if route.tags.is_empty() {
        route.tags = settings
            .get("route")
            .and_then(|route| route.get("tags"))
            .or_else(|| settings.get("tags"))
            .and_then(Value::as_array)
            .map(|tags| {
                tags.iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();
    }
    if route.span_plugins.is_empty() {
        route.span_plugins = settings
            .get("route")
            .and_then(|route| route.get("span_plugins"))
            .and_then(|plugins| serde_json::from_value(plugins.clone()).ok())
            .unwrap_or_default();
    }
    settings.insert("trace_to_braintrust".into(), Value::Bool(true));
    settings.insert("route".into(), serde_json::to_value(route)?);
    for key in [
        "traceToBraintrust",
        "profile",
        "org_name",
        "api_key",
        "api_url",
        "app_url",
        "project",
        "destination",
        "additional_metadata",
        "tags",
    ] {
        settings.remove(key);
    }
    write_object_atomic_with(path, settings, FileAccess::OwnerOnly)
        .with_context(|| format!("failed to write agent settings: {}", path.display()))
}

fn enable_tracing(source: &str, route: SessionRoute) -> anyhow::Result<PathBuf> {
    let path = paths::agent_settings_path(source, None);
    enable_tracing_at(&path, route)?;
    Ok(path)
}

fn remove_tracing_settings(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to remove tracing settings: {}", path.display())),
    }
}

fn finish_disable(adapter_result: anyhow::Result<()>, settings_path: &Path) -> anyhow::Result<()> {
    let settings_result = remove_tracing_settings(settings_path);
    adapter_result.and(settings_result)
}

/// Uninstall an agent's tracing adapter and remove its Braintrust-owned settings.
pub fn run_disable(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let (source, display_name) = agent_details(agent);
    let adapter_result = match agent {
        SetupAgent::Codex => disable_codex(&mut runner),
        SetupAgent::Claude => disable_claude(&mut runner),
        SetupAgent::OpenCode => disable_opencode(),
        SetupAgent::Pi => disable_pi(&mut runner),
        SetupAgent::Grok => disable_grok(&mut runner),
        SetupAgent::Cursor => {
            disable_cursor_with_hooks_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())
        }
        SetupAgent::Antigravity => disable_antigravity(&mut runner),
    };
    let settings_path = paths::agent_settings_path(source, None);
    finish_disable(adapter_result, &settings_path)?;
    Ok(TraceCommandOutput::disable(
        source,
        display_name,
        settings_path,
    ))
}

/// Update an already installed tracing adapter without writing tracing settings,
/// enabling a disabled plugin, or creating an agent configuration file.
pub fn run_update(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let (source, display_name) = agent_details(agent);
    match agent {
        SetupAgent::Codex => update_codex(&mut runner)?,
        SetupAgent::Claude => update_claude(&mut runner)?,
        SetupAgent::OpenCode => update_opencode()?,
        SetupAgent::Pi => update_pi(&mut runner)?,
        SetupAgent::Grok => update_grok(&mut runner)?,
        SetupAgent::Cursor => {
            update_cursor_with_hooks_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())?
        }
        SetupAgent::Antigravity => update_antigravity(&mut runner)?,
    }
    Ok(TraceCommandOutput::update(source, display_name))
}

fn agent_details(agent: SetupAgent) -> (&'static str, &'static str) {
    match agent {
        SetupAgent::Codex => ("codex", "Codex"),
        SetupAgent::Claude => ("claude", "Claude Code"),
        SetupAgent::OpenCode => ("opencode", "OpenCode"),
        SetupAgent::Pi => ("pi", "Pi"),
        SetupAgent::Grok => ("grok", "Grok"),
        SetupAgent::Cursor => ("cursor", "Cursor"),
        SetupAgent::Antigravity => ("antigravity", "Google Antigravity"),
    }
}

/// Install or refresh one agent's published tracing adapter and persist its
/// non-secret route selection.
pub fn run_enable(args: EnableArgs, route: SessionRoute) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let (source, display_name) = match args.agent {
        SetupAgent::Codex => {
            setup_codex(&mut runner)?;
            ("codex", "Codex")
        }
        SetupAgent::Claude => {
            setup_claude(&mut runner)?;
            warn_legacy_claude_tracing_env();
            ("claude", "Claude Code")
        }
        SetupAgent::OpenCode => {
            setup_opencode()?;
            ("opencode", "OpenCode")
        }
        SetupAgent::Pi => {
            setup_pi(&mut runner)?;
            ("pi", "Pi")
        }
        SetupAgent::Grok => {
            setup_grok(&mut runner)?;
            ("grok", "Grok")
        }
        SetupAgent::Cursor => {
            setup_cursor_with_hooks_at(&paths::cursor_plugin_dir(), &paths::cursor_config_dir())?;
            ("cursor", "Cursor")
        }
        SetupAgent::Antigravity => {
            setup_antigravity(&mut runner)?;
            ("antigravity", "Google Antigravity")
        }
    };
    let settings_path = enable_tracing(source, route)?;
    Ok(TraceCommandOutput::setup(
        source,
        display_name,
        settings_path,
    ))
}

/// Backwards-compatible library entry point for hosts that used the former setup name.
pub fn run_setup(args: EnableArgs, route: SessionRoute) -> anyhow::Result<TraceCommandOutput> {
    run_enable(args, route)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{AuthSelection, TraceDestination};
    use std::collections::VecDeque;

    #[test]
    fn cursor_install_is_repeatable_and_preserves_neighbor_plugins() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins/local");
        let plugin = root.join("trace-cursor");
        let neighbor = root.join("my-plugin/keep.txt");
        std::fs::create_dir_all(neighbor.parent().unwrap()).unwrap();
        std::fs::write(&neighbor, "leave me").unwrap();

        setup_cursor_at(&plugin).unwrap();
        setup_cursor_at(&plugin).unwrap();

        assert_eq!(std::fs::read_to_string(&neighbor).unwrap(), "leave me");
        assert_eq!(
            std::fs::read_to_string(plugin.join("hooks/hooks.json")).unwrap(),
            CURSOR_HOOKS_MANIFEST
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
        assert!(cursor_plugin_is_installed_at(&plugin));
        assert!(cursor_plugin_is_ours(&manifest));
    }

    #[test]
    fn cursor_discovery_health_requires_each_lifecycle_hook() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");
        setup_cursor_with_hooks_at(&temp.path().join("trace-cursor"), &config_dir).unwrap();
        assert!(cursor_discovery_hooks_are_installed_at(&config_dir));

        let hooks_path = config_dir.join("hooks.json");
        let mut config = load_object(&hooks_path).unwrap();
        config["hooks"].as_object_mut().unwrap().remove("stop");
        write_object_atomic(&hooks_path, config).unwrap();

        assert!(!cursor_discovery_hooks_are_installed_at(&config_dir));
    }

    #[test]
    fn cursor_discovery_hooks_generate_shell_independent_windows_commands() {
        use base64::Engine;
        let specs = cursor_discovery_hook_specs("braintrust-cursor-managed-1234-42-run", true);
        assert_eq!(specs.len(), 3);
        for (event, command) in &specs {
            let encoded = command
                .strip_prefix("powershell.exe -NoProfile -EncodedCommand ")
                .unwrap();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let (chunks, remainder) = bytes.as_chunks::<2>();
            assert!(remainder.is_empty());
            let words = chunks
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>();
            let script = String::from_utf16(&words).unwrap();
            assert!(script.contains("braintrust-cursor-managed-1234-42-run"));
            assert_eq!(
                cursor_managed_process_identity(command),
                Some(crate::wire::ProcessIdentity {
                    pid: 1234,
                    start_time_secs: 42,
                })
            );
            if event == "beforeSubmitPrompt" {
                assert!(script.contains(r#"'{"continue":true}'"#));
            } else {
                assert!(script.contains("'{}'"));
            }
        }
    }

    #[test]
    fn managed_cursor_setup_removes_only_hooks_from_exited_runs() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");
        std::fs::create_dir_all(&config_dir).unwrap();
        let context = crate::process::capture_process_context(std::process::id());
        let current = context.process_chain.first().unwrap();
        let stale =
            cursor_discovery_hook_specs("braintrust-cursor-managed-4294967295-1-stale", false);
        let active = cursor_discovery_hook_specs(
            &format!(
                "braintrust-cursor-managed-{}-{}-active",
                current.pid, current.start_time_secs
            ),
            false,
        );
        let mut config = Map::new();
        config.insert("version".into(), serde_json::json!(1));
        let mut hooks = Map::new();
        for ((stale_event, stale_command), (active_event, active_command)) in
            stale.into_iter().zip(active)
        {
            assert_eq!(stale_event, active_event);
            hooks.insert(
                stale_event,
                serde_json::json!([
                    cursor_discovery_hook(&stale_command),
                    cursor_discovery_hook(&active_command)
                ]),
            );
        }
        config.insert("hooks".into(), Value::Object(hooks));
        write_object_atomic(&config_dir.join("hooks.json"), config).unwrap();

        let _managed = CursorManagedHooks::install(&config_dir, "new-run").unwrap();
        let config = load_object(&config_dir.join("hooks.json")).unwrap();

        for event in ["beforeSubmitPrompt", "afterAgentResponse", "stop"] {
            let commands = config["hooks"][event].as_array().unwrap();
            assert_eq!(commands.len(), 2);
            assert!(commands
                .iter()
                .any(|entry| entry["command"].as_str().unwrap().contains("-active")));
            assert!(commands
                .iter()
                .all(|entry| !entry["command"].as_str().unwrap().contains("-stale")));
        }
    }

    #[test]
    fn cursor_setup_refuses_to_replace_or_remove_an_unrelated_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("trace-cursor");
        std::fs::create_dir_all(plugin.join(".cursor-plugin")).unwrap();
        std::fs::write(
            plugin.join(".cursor-plugin/plugin.json"),
            r#"{"name":"trace-cursor","version":"1.0.0","repository":"https://example.com/custom"}"#,
        )
        .unwrap();

        assert!(setup_cursor_at(&plugin).is_err());
        assert!(disable_cursor_at(&plugin).is_err());
        assert!(!cursor_plugin_is_installed_at(&plugin));
        assert!(plugin.join(".cursor-plugin/plugin.json").exists());
    }

    #[test]
    fn cursor_disable_removes_only_its_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins/local");
        let plugin = root.join("trace-cursor");
        let neighbor = root.join("other-plugin/file");
        std::fs::create_dir_all(neighbor.parent().unwrap()).unwrap();
        std::fs::write(&neighbor, "preserve").unwrap();

        setup_cursor_at(&plugin).unwrap();
        disable_cursor_at(&plugin).unwrap();
        disable_cursor_at(&plugin).unwrap();

        assert!(!plugin.exists());
        assert_eq!(std::fs::read_to_string(neighbor).unwrap(), "preserve");
    }

    struct FakeRunner {
        responses: VecDeque<Value>,
        calls: Vec<String>,
        home_calls: Vec<(String, PathBuf)>,
    }

    impl FakeRunner {
        fn new(responses: impl IntoIterator<Item = Value>) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                calls: Vec::new(),
                home_calls: Vec::new(),
            }
        }

        fn called(&self, command: &str) -> bool {
            self.calls.iter().any(|call| call == command)
        }
    }

    impl CommandRunner for FakeRunner {
        fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            self.responses
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake JSON response"))
        }

        fn json_in_home(
            &mut self,
            program: &str,
            args: &[&str],
            home: &Path,
        ) -> anyhow::Result<Value> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            self.home_calls
                .push((format!("{program} {}", args.join(" ")), home.to_path_buf()));
            self.responses
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake JSON response"))
        }

        fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            Ok(())
        }

        fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            self.home_calls
                .push((format!("{program} {}", args.join(" ")), home.to_path_buf()));
            Ok(())
        }
    }

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
    fn codex_installs_from_the_published_marketplace_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!({"marketplaces": []})]);

        setup_codex(&mut runner).unwrap();

        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
    }

    #[test]
    fn codex_refreshes_the_published_marketplace_and_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!({
            "marketplaces": [{
                "name": CODEX_MARKETPLACE,
                "marketplaceSource": {
                    "sourceType": "github",
                    "source": CODEX_MARKETPLACE_SOURCE
                }
            }]
        })]);

        setup_codex(&mut runner).unwrap();

        assert!(runner.called("codex plugin marketplace upgrade braintrust-codex-plugins"));
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
        assert!(!runner.called("codex plugin marketplace remove braintrust-codex-plugins"));
    }

    #[test]
    fn codex_replaces_a_same_name_local_marketplace() {
        let mut runner = FakeRunner::new([serde_json::json!({
            "marketplaces": [{
                "name": CODEX_MARKETPLACE,
                "marketplaceSource": {"sourceType": "local", "source": "/tmp/stale"}
            }]
        })]);

        setup_codex(&mut runner).unwrap();

        assert!(runner.called("codex plugin marketplace remove braintrust-codex-plugins"));
        assert!(
            runner.called("codex plugin marketplace add braintrustdata/braintrust-codex-plugin")
        );
        assert!(runner.called("codex plugin add trace-codex@braintrust-codex-plugins"));
    }

    #[test]
    fn claude_installs_from_the_published_marketplace_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!([]), serde_json::json!([])]);

        setup_claude(&mut runner).unwrap();

        assert!(
            runner.called("claude plugin marketplace add braintrustdata/braintrust-claude-plugin")
        );
        assert!(runner.called("claude plugin install trace-claude-code@braintrust-claude-plugin"));
    }

    #[test]
    fn claude_refreshes_the_published_marketplace_and_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{
                "name": CLAUDE_MARKETPLACE,
                "source": "github",
                "repo": CLAUDE_MARKETPLACE_SOURCE
            }]),
            serde_json::json!([{
                "id": CLAUDE_PLUGIN,
                "version": "1.4.4",
                "enabled": true
            }]),
        ]);

        setup_claude(&mut runner).unwrap();

        assert!(runner.called("claude plugin marketplace update braintrust-claude-plugin"));
        assert!(runner.called("claude plugin update trace-claude-code@braintrust-claude-plugin"));
        assert!(!runner.called("claude plugin marketplace remove braintrust-claude-plugin"));
    }

    #[test]
    fn claude_replaces_a_same_name_local_marketplace() {
        let mut runner = FakeRunner::new([serde_json::json!([{
            "name": CLAUDE_MARKETPLACE,
            "source": "directory",
            "path": "/tmp/stale"
        }])]);

        setup_claude(&mut runner).unwrap();

        assert!(runner.called("claude plugin marketplace remove braintrust-claude-plugin"));
        assert!(
            runner.called("claude plugin marketplace add braintrustdata/braintrust-claude-plugin")
        );
        assert!(runner.called("claude plugin install trace-claude-code@braintrust-claude-plugin"));
    }

    #[test]
    fn claude_updates_then_enables_a_disabled_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{
                "name": CLAUDE_MARKETPLACE,
                "source": "github",
                "repo": CLAUDE_MARKETPLACE_SOURCE
            }]),
            serde_json::json!([{"id": CLAUDE_PLUGIN, "enabled": false}]),
        ]);

        setup_claude(&mut runner).unwrap();

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
    fn claude_legacy_env_diagnostic_is_read_only_and_preserves_api_key() {
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
            legacy_claude_tracing_env_keys(&path).unwrap(),
            ["BRAINTRUST_CC_PROJECT", "BRAINTRUST_CC_DEBUG"]
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), contents);
    }

    #[test]
    fn grok_installs_published_plugin_then_enables_it_when_missing() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        setup_grok(&mut runner).unwrap();

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
    fn grok_repeated_enable_updates_and_enables_the_published_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": GROK_PLUGIN,
                "source": "https://github.com/braintrustdata/braintrust-grok-plugin.git",
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        setup_grok(&mut runner).unwrap();

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
    fn grok_update_requires_the_published_plugin_without_enabling_it() {
        let mut runner = FakeRunner::new([serde_json::json!([{
            "name": GROK_PLUGIN,
            "source": GROK_PLUGIN_SOURCE,
            "status": "installed"
        }])]);

        update_grok(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            ["grok plugin list --json", "grok plugin update trace-grok"]
        );
    }

    #[test]
    fn grok_enable_reconciles_only_a_conflicting_same_name_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": GROK_PLUGIN,
                "source": "/tmp/local-trace-grok",
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        setup_grok(&mut runner).unwrap();

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
    fn grok_disable_removes_only_the_published_braintrust_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!([
            {
                "name": GROK_PLUGIN,
                "source": GROK_PLUGIN_SOURCE,
                "status": "installed"
            },
            {"name": "other-plugin", "source": "somebody/other-plugin"}
        ])]);

        disable_grok(&mut runner).unwrap();

        assert_eq!(
            runner.calls,
            [
                "grok plugin list --json",
                "grok plugin uninstall trace-grok --confirm",
            ]
        );

        let mut local = FakeRunner::new([serde_json::json!([{
            "name": GROK_PLUGIN,
            "source": "/tmp/local-trace-grok",
            "status": "installed"
        }])]);
        disable_grok(&mut local).unwrap();
        assert_eq!(local.calls, ["grok plugin list --json"]);
    }

    #[test]
    fn opencode_reconciles_the_published_plugin_and_preserves_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@0.9.0"],"model":"test/model"}"#,
        )
        .unwrap();

        setup_opencode_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", opencode_plugin_spec().unwrap()])
        );
    }

    #[test]
    fn pi_installs_the_latest_published_extension() {
        let mut runner = FakeRunner::new([]);

        setup_pi(&mut runner).unwrap();

        assert!(runner.called("pi install npm:@braintrust/pi-extension"));
    }

    #[test]
    fn pi_updates_the_npm_extension_without_installing_it() {
        let mut runner = FakeRunner::new([]);

        update_pi(&mut runner).unwrap();

        assert!(runner.called("pi update npm:@braintrust/pi-extension"));
        assert!(!runner
            .calls
            .iter()
            .any(|call| call.starts_with("pi install ")));
    }

    #[test]
    fn opencode_update_requires_an_existing_plugin_and_preserves_other_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@^1"],"model":"test/model"}"#,
        )
        .unwrap();

        update_opencode_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", opencode_plugin_spec().unwrap()])
        );

        let missing = temp.path().join("missing.json");
        assert!(update_opencode_at(&missing).is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn codex_update_refuses_to_install_a_missing_plugin() {
        let mut runner = FakeRunner::new([serde_json::json!({"installed": []})]);

        assert!(update_codex(&mut runner).is_err());
        assert!(!runner
            .calls
            .iter()
            .any(|call| call.starts_with("codex plugin add ")));
    }

    #[test]
    fn antigravity_installs_published_plugin_and_removes_legacy_registration() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        std::fs::write(
            &hooks_path,
            serde_json::to_vec(&serde_json::json!({
                ANTIGRAVITY_PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": [{"type": "command", "command": "other"}]}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        setup_antigravity_at(&mut runner, &config_dir).unwrap();

        assert!(runner.called(&format!("agy plugin install {ANTIGRAVITY_PLUGIN_SOURCE}")));
        assert!(runner.called(&format!("agy plugin enable {ANTIGRAVITY_PLUGIN}")));
        let hooks: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert_eq!(hooks["other-plugin"]["Stop"][0]["command"], "other");
        assert!(hooks.get(ANTIGRAVITY_PLUGIN).is_none());
    }

    #[test]
    fn antigravity_update_checks_and_updates_the_same_overridden_home() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let expected_home = temp.path().to_path_buf();
        let mut runner = FakeRunner::new([serde_json::json!({
            "imports": [{"name": ANTIGRAVITY_PLUGIN}],
        })]);

        update_antigravity_at(&mut runner, &config_dir).unwrap();

        assert_eq!(
            runner.home_calls,
            vec![
                ("agy plugin list".into(), expected_home.clone()),
                (
                    format!("agy plugin install {ANTIGRAVITY_PLUGIN_SOURCE}"),
                    expected_home,
                ),
            ]
        );
    }

    #[test]
    fn antigravity_setup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("hooks.json"),
            serde_json::to_vec(&serde_json::json!({
                ANTIGRAVITY_PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": []}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        setup_antigravity_at(&mut runner, &config_dir).unwrap();
        let first = std::fs::read(config_dir.join("hooks.json")).unwrap();
        setup_antigravity_at(&mut runner, &config_dir).unwrap();
        let second = std::fs::read(config_dir.join("hooks.json")).unwrap();

        assert_eq!(first, second);
        assert_eq!(
            runner
                .calls
                .iter()
                .filter(|call| {
                    call.as_str() == format!("agy plugin install {ANTIGRAVITY_PLUGIN_SOURCE}")
                })
                .count(),
            2
        );
    }

    #[test]
    fn antigravity_setup_relies_on_native_plugin_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        let mut runner = FakeRunner::new([]);

        setup_antigravity_at(&mut runner, &config_dir).unwrap();

        assert!(!config_dir.join("hooks.json").exists());
        assert!(runner.called(&format!("agy plugin install {ANTIGRAVITY_PLUGIN_SOURCE}")));
        assert!(runner.called(&format!("agy plugin enable {ANTIGRAVITY_PLUGIN}")));
    }

    #[test]
    fn antigravity_setup_reports_a_missing_cli_without_changing_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        let original = br#"{"other-plugin":{"Stop":[]}}"#;
        std::fs::write(&hooks_path, original).unwrap();

        let error = setup_antigravity_at(&mut MissingAgyRunner, &config_dir).unwrap_err();

        assert_eq!(
            error.to_string(),
            "failed to run `agy`; install agy and ensure it is on PATH"
        );
        assert_eq!(std::fs::read(hooks_path).unwrap(), original);
    }

    #[test]
    fn antigravity_disable_removes_only_the_managed_registration() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        std::fs::write(
            &hooks_path,
            serde_json::to_vec(&serde_json::json!({
                ANTIGRAVITY_PLUGIN: {"Stop": []},
                "other-plugin": {"Stop": [{"type": "command", "command": "other"}]}
            }))
            .unwrap(),
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        disable_antigravity_at(&mut runner, &config_dir).unwrap();

        assert!(runner.called("agy plugin uninstall braintrust-antigravity-tracing"));
        let hooks: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert!(hooks.get(ANTIGRAVITY_PLUGIN).is_none());
        assert_eq!(hooks["other-plugin"]["Stop"][0]["command"], "other");
    }

    #[test]
    fn tracing_settings_preserve_unrelated_fields_and_remove_legacy_keys() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{
                "traceToBraintrust": false,
                "profile": "stale-profile",
                "org_name": "stale-org",
                "api_key": "stale-secret",
                "api_url": "https://stale-api.example",
                "app_url": "https://stale-app.example",
                "project": "old",
                "destination": "old-destination",
                "additional_metadata": {"migrated": true},
                "auth": {"type": "legacy"}
            }"#,
        )
        .unwrap();
        let route = SessionRoute {
            auth: AuthSelection {
                source: crate::wire::AuthSource::SavedProfile,
                profile_id: None,
                profile: Some("work".into()),
                org_name: Some("Braintrust SDKs".into()),
            },
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("coding-agents".into()),
            }),
            ..SessionRoute::default()
        };

        enable_tracing_at(&path, route).unwrap();

        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(settings["trace_to_braintrust"], true);
        assert_eq!(settings["route"]["auth"]["profile"], "work");
        assert_eq!(
            settings["route"]["destination"]["project_name"],
            "coding-agents"
        );
        assert_eq!(settings["auth"]["type"], "legacy");
        assert_eq!(settings["route"]["additional_metadata"]["migrated"], true);
        for key in [
            "traceToBraintrust",
            "profile",
            "org_name",
            "api_key",
            "api_url",
            "app_url",
            "project",
            "destination",
            "additional_metadata",
        ] {
            assert!(settings.get(key).is_none(), "legacy key {key} remained");
        }
    }

    #[test]
    fn tracing_settings_preserve_metadata_until_setup_explicitly_replaces_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"route":{"additional_metadata":{"ci":true},"tags":["saved"]}}"#,
        )
        .unwrap();

        let route = SessionRoute::default();
        enable_tracing_at(&path, route).unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["additional_metadata"],
            serde_json::json!({"ci": true})
        );
        assert_eq!(settings["route"]["tags"], serde_json::json!(["saved"]));

        let route = SessionRoute {
            additional_metadata: Some(serde_json::json!({"run_id": "new"})),
            tags: vec!["replacement".to_string()],
            ..SessionRoute::default()
        };
        enable_tracing_at(&path, route).unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["additional_metadata"],
            serde_json::json!({"run_id": "new"})
        );
        assert_eq!(
            settings["route"]["tags"],
            serde_json::json!(["replacement"])
        );
    }

    #[test]
    fn disabling_removes_only_the_braintrust_settings_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"traceToBraintrust":true,"route":{"destination":{"project_name":"coding-agents"}},"other":true}"#,
        )
        .unwrap();

        remove_tracing_settings(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn disabling_removes_settings_even_when_adapter_cleanup_fails() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(&path, "{}").unwrap();

        let error = finish_disable(Err(anyhow::anyhow!("adapter unavailable")), &path)
            .unwrap_err()
            .to_string();

        assert_eq!(error, "adapter unavailable");
        assert!(!path.exists());
    }

    #[test]
    fn disabling_installed_plugins_uses_each_agents_uninstall_command() {
        let mut codex = FakeRunner::new([serde_json::json!({
            "installed": [{"pluginId": CODEX_PLUGIN}]
        })]);
        disable_codex(&mut codex).unwrap();
        assert!(codex.called("codex plugin remove trace-codex@braintrust-codex-plugins --json"));

        let mut claude = FakeRunner::new([serde_json::json!([{"id": CLAUDE_PLUGIN}])]);
        disable_claude(&mut claude).unwrap();
        assert!(claude.called("claude plugin uninstall trace-claude-code@braintrust-claude-plugin"));

        let mut pi = FakeRunner::new([]);
        disable_pi(&mut pi).unwrap();
        assert!(pi.called("pi uninstall npm:@braintrust/pi-extension"));
    }

    #[test]
    fn disabling_opencode_removes_only_the_managed_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "plugin": ["other", opencode_plugin_spec().unwrap()],
                "model": "test/model",
            }))
            .unwrap(),
        )
        .unwrap();

        remove_opencode_plugin_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["plugin"], serde_json::json!(["other"]));
        assert_eq!(config["model"], "test/model");
    }

    #[test]
    fn tracing_settings_are_private_to_the_owner() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir(&config).unwrap();
        let path = config.join("braintrust.json");
        std::fs::write(&path, "{}").unwrap();
        // Simulate a configuration directory other users can read.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        #[cfg(windows)]
        crate::win_acl::test_support::set_sddl(&config, "D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)");

        enable_tracing_at(&path, SessionRoute::default()).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        #[cfg(windows)]
        {
            use crate::win_acl::test_support::{assert_owner_only, path_dacl_sddl};
            assert_owner_only(&path_dacl_sddl(&path), false);
        }
    }

    /// Settings must be protected before they replace the old file: if the
    /// protection cannot be applied, the previous settings stay in place
    /// rather than a replacement carrying the directory's shared access.
    #[cfg(windows)]
    #[test]
    fn tracing_settings_are_not_published_when_they_cannot_be_protected() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir(&config).unwrap();
        let path = config.join("braintrust.json");
        std::fs::write(&path, r#"{"trace_to_braintrust":false}"#).unwrap();
        // Everyone may create, modify, rename, and delete files here, but
        // OWNER RIGHTS withholds the creator's implicit WRITE_DAC, so new
        // files cannot be given an owner-only DACL.
        crate::win_acl::test_support::set_sddl(
            &config,
            "D:P(A;OICI;0x1301bf;;;WD)(A;OICI;0x1301bf;;;OW)",
        );

        assert!(enable_tracing_at(&path, SessionRoute::default()).is_err());

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            r#"{"trace_to_braintrust":false}"#
        );
        let leftovers: Vec<_> = std::fs::read_dir(&config)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn tracing_settings_preserve_plugins_until_setup_explicitly_replaces_them() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(&path, r#"{"route":{"span_plugins":["old.mjs"]}}"#).unwrap();

        enable_tracing_at(&path, SessionRoute::default()).unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["span_plugins"],
            serde_json::json!(["old.mjs"])
        );

        enable_tracing_at(
            &path,
            SessionRoute {
                span_plugins: vec![PathBuf::from("first.mjs"), PathBuf::from("second.mjs")],
                ..SessionRoute::default()
            },
        )
        .unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["span_plugins"],
            serde_json::json!(["first.mjs", "second.mjs"])
        );
    }
}
