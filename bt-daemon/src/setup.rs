//! Persistent installation and configuration for coding-agent tracing plugins.

use crate::paths;
use crate::trace_command::{EnableArgs, SetupAgent};
use crate::wire::SessionRoute;
use crate::TraceCommandOutput;
use anyhow::{bail, Context};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

const CODEX_MARKETPLACE: &str = "braintrust-codex-plugins";
const CODEX_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const CODEX_PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const CLAUDE_MARKETPLACE: &str = "braintrust-claude-plugin";
const CLAUDE_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";
const CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";
const OPENCODE_PLUGIN: &str = "@braintrust/trace-opencode@^1";
const PI_PLUGIN: &str = "npm:@braintrust/pi-extension@^1";
const ANTIGRAVITY_PLUGIN: &str = "braintrust-antigravity-tracing";
#[cfg(any(unix, test))]
const ANTIGRAVITY_PLUGIN_JSON: &str = include_str!("../assets/antigravity/plugin.json");
#[cfg(any(unix, test))]
const ANTIGRAVITY_HOOKS_JSON: &str = include_str!("../assets/antigravity/hooks.json");
#[cfg(any(unix, test))]
const ANTIGRAVITY_HOOK_SCRIPT: &str = include_str!("../assets/antigravity/bin/antigravity-hook.sh");

trait CommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value>;
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()>;
    fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()>;
}

struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
        let output = ProcessCommand::new(program)
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

    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
        let status = ProcessCommand::new(program)
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
        let output = ProcessCommand::new(program)
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

fn disable_claude(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    if claude_plugin(&plugins).is_some() {
        runner.run("claude", &["plugin", "uninstall", CLAUDE_PLUGIN])?;
    }
    Ok(())
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

fn write_object_atomic(path: &Path, object: Map<String, Value>) -> anyhow::Result<()> {
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
    plugins.push(Value::String(OPENCODE_PLUGIN.into()));
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
                .with_context(|| format!("failed to read configuration: {}", path.display()))
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
    runner.run("pi", &["install", PI_PLUGIN])
}

fn disable_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["uninstall", PI_PLUGIN])
}

#[cfg(unix)]
fn write_file(path: &Path, contents: &str) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("file path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create directory: {}", parent.display()))?;
    std::fs::write(path, contents)
        .with_context(|| format!("failed to write file: {}", path.display()))
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> anyhow::Result<String> {
    let path = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Antigravity hook path is not valid UTF-8"))?;
    Ok(format!("'{}'", path.replace('\'', "'\"'\"'")))
}

#[cfg(unix)]
fn rewrite_hook_commands(value: &mut Value, event: &str, adapter: &Path) -> anyhow::Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                rewrite_hook_commands(value, event, adapter)?;
            }
        }
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("command") {
                object.insert(
                    "command".into(),
                    Value::String(format!("sh {} {event}", shell_quote(adapter)?)),
                );
            }
            for value in object.values_mut() {
                rewrite_hook_commands(value, event, adapter)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(unix)]
fn antigravity_hook_registration(adapter: &Path) -> anyhow::Result<Value> {
    let mut bundled: Value = serde_json::from_str(ANTIGRAVITY_HOOKS_JSON)?;
    let mut registration = bundled
        .as_object_mut()
        .and_then(|hooks| hooks.remove(ANTIGRAVITY_PLUGIN))
        .ok_or_else(|| anyhow::anyhow!("bundled Antigravity hooks are missing their root entry"))?;
    let events = registration
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("bundled Antigravity hook entry must be an object"))?;
    for (event, hooks) in events {
        rewrite_hook_commands(hooks, event, adapter)?;
    }
    Ok(registration)
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

#[cfg(unix)]
fn setup_antigravity_at(runner: &mut impl CommandRunner, config_dir: &Path) -> anyhow::Result<()> {
    let staging = tempfile::tempdir().context("failed to stage the Antigravity tracing plugin")?;
    write_file(&staging.path().join("plugin.json"), ANTIGRAVITY_PLUGIN_JSON)?;
    write_file(&staging.path().join("hooks.json"), ANTIGRAVITY_HOOKS_JSON)?;
    let staged_adapter = staging.path().join("bin/antigravity-hook.sh");
    write_file(&staged_adapter, ANTIGRAVITY_HOOK_SCRIPT)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staged_adapter, std::fs::Permissions::from_mode(0o755))
            .with_context(|| {
                format!(
                    "failed to make Antigravity hook executable: {}",
                    staged_adapter.display()
                )
            })?;
    }

    let staging_path = staging
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Antigravity plugin staging path is not valid UTF-8"))?;
    runner.run_in_home(
        "agy",
        &["plugin", "install", staging_path],
        antigravity_home(config_dir)?,
    )?;

    let adapter = config_dir
        .join("plugins")
        .join(ANTIGRAVITY_PLUGIN)
        .join("bin")
        .join("antigravity-hook.sh");
    let hooks_path = config_dir.join("hooks.json");
    let mut hooks = load_object(&hooks_path)?;
    hooks.insert(
        ANTIGRAVITY_PLUGIN.into(),
        antigravity_hook_registration(&adapter)?,
    );
    write_object_atomic(&hooks_path, hooks)
}

#[cfg(not(unix))]
fn setup_antigravity_at(_: &mut impl CommandRunner, _: &Path) -> anyhow::Result<()> {
    bail!("Google Antigravity tracing setup currently requires a Unix-compatible `sh`")
}

fn setup_antigravity(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    setup_antigravity_at(runner, &paths::antigravity_config_dir())
}

fn disable_antigravity_at(
    runner: &mut impl CommandRunner,
    config_dir: &Path,
) -> anyhow::Result<()> {
    let hooks_path = config_dir.join("hooks.json");
    let mut hooks = if hooks_path.exists() {
        Some(load_object(&hooks_path)?)
    } else {
        None
    };

    // Antigravity's uninstall command is idempotent when the plugin is absent.
    runner.run_in_home(
        "agy",
        &["plugin", "uninstall", ANTIGRAVITY_PLUGIN],
        antigravity_home(config_dir)?,
    )?;
    if let Some(hooks) = hooks.as_mut() {
        hooks.remove(ANTIGRAVITY_PLUGIN);
        write_object_atomic(&hooks_path, std::mem::take(hooks))?;
    }
    Ok(())
}

fn disable_antigravity(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    disable_antigravity_at(runner, &paths::antigravity_config_dir())
}

fn enable_tracing_at(path: &Path, mut route: SessionRoute) -> anyhow::Result<()> {
    let mut settings = load_object(path)?;
    if route.additional_metadata.is_none() {
        route.additional_metadata = settings
            .get("route")
            .and_then(|route| route.get("additional_metadata"))
            .filter(|metadata| metadata.is_object())
            .cloned();
    }
    settings.insert("trace_to_braintrust".into(), Value::Bool(true));
    settings.insert("route".into(), serde_json::to_value(route)?);
    settings.remove("traceToBraintrust");
    settings.remove("project");
    write_object_atomic(path, settings)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to protect agent settings: {}", path.display()))?;
    }
    Ok(())
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

/// Uninstall an agent's tracing adapter and remove its Braintrust-owned settings.
pub fn run_disable(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let (source, display_name) = agent_details(agent);
    match agent {
        SetupAgent::Codex => disable_codex(&mut runner)?,
        SetupAgent::Claude => disable_claude(&mut runner)?,
        SetupAgent::OpenCode => disable_opencode()?,
        SetupAgent::Pi => disable_pi(&mut runner)?,
        SetupAgent::Antigravity => disable_antigravity(&mut runner)?,
    }
    let settings_path = paths::agent_settings_path(source, None);
    remove_tracing_settings(&settings_path)?;
    Ok(TraceCommandOutput::disable(
        source,
        display_name,
        settings_path,
    ))
}

fn agent_details(agent: SetupAgent) -> (&'static str, &'static str) {
    match agent {
        SetupAgent::Codex => ("codex", "Codex"),
        SetupAgent::Claude => ("claude", "Claude Code"),
        SetupAgent::OpenCode => ("opencode", "OpenCode"),
        SetupAgent::Pi => ("pi", "Pi"),
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

    struct FakeRunner {
        responses: VecDeque<Value>,
        calls: Vec<String>,
    }

    impl FakeRunner {
        fn new(responses: impl IntoIterator<Item = Value>) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                calls: Vec::new(),
            }
        }

        fn called(&self, command: &str) -> bool {
            self.calls.iter().any(|call| call == command)
        }

        #[cfg(unix)]
        fn called_with_prefix(&self, command: &str) -> bool {
            self.calls.iter().any(|call| call.starts_with(command))
        }
    }

    impl CommandRunner for FakeRunner {
        fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            self.responses
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake JSON response"))
        }

        fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            Ok(())
        }

        fn run_in_home(&mut self, program: &str, args: &[&str], _: &Path) -> anyhow::Result<()> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            Ok(())
        }
    }

    #[cfg(unix)]
    struct MissingAgyRunner;

    #[cfg(unix)]
    impl CommandRunner for MissingAgyRunner {
        fn json(&mut self, _: &str, _: &[&str]) -> anyhow::Result<Value> {
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
            serde_json::json!(["other", "@braintrust/trace-opencode@^1"])
        );
    }

    #[test]
    fn pi_installs_the_published_extension_range() {
        let mut runner = FakeRunner::new([]);

        setup_pi(&mut runner).unwrap();

        assert!(runner.called("pi install npm:@braintrust/pi-extension@^1"));
    }

    #[test]
    #[cfg(unix)]
    fn antigravity_installs_and_registers_absolute_hooks_without_clobbering_config() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("home with 'quotes'/.gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hooks_path = config_dir.join("hooks.json");
        std::fs::write(
            &hooks_path,
            r#"{"other-plugin":{"Stop":[{"type":"command","command":"other"}]}}"#,
        )
        .unwrap();
        let mut runner = FakeRunner::new([]);

        setup_antigravity_at(&mut runner, &config_dir).unwrap();

        assert!(runner.called_with_prefix("agy plugin install "));
        let hooks: Value = serde_json::from_slice(&std::fs::read(&hooks_path).unwrap()).unwrap();
        assert_eq!(hooks["other-plugin"]["Stop"][0]["command"], "other");
        let managed = &hooks[ANTIGRAVITY_PLUGIN];
        assert!(managed.get("PreToolUse").is_none());
        assert!(managed.get("PreInvocation").is_some());
        assert!(managed.get("PostInvocation").is_some());
        assert!(managed.get("PostToolUse").is_some());
        assert!(managed.get("Stop").is_some());
        let command = managed["Stop"][0]["command"].as_str().unwrap();
        assert!(command.starts_with("sh '"));
        assert!(command.contains("plugins/braintrust-antigravity-tracing/bin/antigravity-hook.sh"));
        assert!(command.contains("'\"'\"'quotes'\"'\"'"));
        assert!(command.ends_with(" Stop"));
    }

    #[test]
    #[cfg(unix)]
    fn antigravity_setup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".gemini/config");
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
                .filter(|call| call.starts_with("agy plugin install "))
                .count(),
            2
        );
    }

    #[test]
    #[cfg(unix)]
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
    fn antigravity_setup_assets_match_the_deployable_plugin() {
        let plugin_root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../src/plugins/antigravity/content");
        assert_eq!(
            ANTIGRAVITY_PLUGIN_JSON,
            std::fs::read_to_string(plugin_root.join("plugin.json")).unwrap()
        );
        assert_eq!(
            ANTIGRAVITY_HOOKS_JSON,
            std::fs::read_to_string(plugin_root.join("hooks.json")).unwrap()
        );
        assert_eq!(
            ANTIGRAVITY_HOOK_SCRIPT,
            std::fs::read_to_string(plugin_root.join("bin/antigravity-hook.sh")).unwrap()
        );
    }

    #[test]
    fn tracing_settings_preserve_unrelated_fields_and_remove_legacy_keys() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"traceToBraintrust":false,"project":"old","auth":{"type":"legacy"}}"#,
        )
        .unwrap();
        let route = SessionRoute {
            auth: AuthSelection {
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
        assert!(settings.get("traceToBraintrust").is_none());
        assert!(settings.get("project").is_none());
    }

    #[test]
    fn tracing_settings_preserve_metadata_until_setup_explicitly_replaces_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(&path, r#"{"route":{"additional_metadata":{"ci":true}}}"#).unwrap();

        let route = SessionRoute::default();
        enable_tracing_at(&path, route).unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["additional_metadata"],
            serde_json::json!({"ci": true})
        );

        let route = SessionRoute {
            additional_metadata: Some(serde_json::json!({"run_id": "new"})),
            ..SessionRoute::default()
        };
        enable_tracing_at(&path, route).unwrap();
        let settings: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            settings["route"]["additional_metadata"],
            serde_json::json!({"run_id": "new"})
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
        assert!(pi.called("pi uninstall npm:@braintrust/pi-extension@^1"));
    }

    #[test]
    fn disabling_opencode_removes_only_the_managed_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@^1"],"model":"test/model"}"#,
        )
        .unwrap();

        remove_opencode_plugin_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["plugin"], serde_json::json!(["other"]));
        assert_eq!(config["model"], "test/model");
    }
}
