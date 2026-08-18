//! Persistent installation and configuration for coding-agent tracing plugins.

use crate::paths;
use crate::trace_command::{LifecycleArgs, SetupArgs};
use crate::wire::SessionRoute;
use crate::{AgentId, TraceCommandOutput};
use anyhow::{bail, Context};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

const CODEX_MARKETPLACE: &str = "braintrust-codex-plugins";
const CODEX_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const CLAUDE_MARKETPLACE: &str = "braintrust-claude-plugin";
const CLAUDE_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";

trait CommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value>;
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()>;
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
    runner.run(
        "codex",
        &["plugin", "add", AgentId::Codex.spec().setup_package],
    )
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
    value.as_array()?.iter().find(|item| {
        item.get("id").and_then(Value::as_str) == Some(AgentId::Claude.spec().setup_package)
            && item
                .get("scope")
                .and_then(Value::as_str)
                .is_none_or(|scope| scope == "user")
    })
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
        return runner.run(
            "claude",
            &[
                "plugin",
                "install",
                AgentId::Claude.spec().setup_package,
                "--scope",
                "user",
            ],
        );
    }

    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    match claude_plugin(&plugins) {
        None => runner.run(
            "claude",
            &[
                "plugin",
                "install",
                AgentId::Claude.spec().setup_package,
                "--scope",
                "user",
            ],
        ),
        Some(plugin) => {
            runner.run(
                "claude",
                &[
                    "plugin",
                    "update",
                    AgentId::Claude.spec().setup_package,
                    "--scope",
                    "user",
                ],
            )?;
            if plugin.get("enabled").and_then(Value::as_bool) == Some(false) {
                runner.run(
                    "claude",
                    &[
                        "plugin",
                        "enable",
                        AgentId::Claude.spec().setup_package,
                        "--scope",
                        "user",
                    ],
                )?;
            }
            Ok(())
        }
    }
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

fn reconcile_opencode_at(path: &Path, install: bool) -> anyhow::Result<()> {
    if !install && !path.exists() {
        return Ok(());
    }
    let mut config = load_object(path)?;
    if let Some(plugins) = config.get_mut("plugin") {
        let plugins = plugins.as_array_mut().ok_or_else(|| {
            anyhow::anyhow!(
                "OpenCode `plugin` config must be an array: {}",
                path.display()
            )
        })?;
        plugins.retain(|plugin| {
            plugin.as_str().is_none_or(|plugin| {
                plugin != "@braintrust/trace-opencode"
                    && !plugin.starts_with("@braintrust/trace-opencode@")
                    && !plugin.starts_with("@braintrust/trace-opencode/")
            })
        });
        if install {
            plugins.push(Value::String(AgentId::OpenCode.spec().setup_package.into()));
        }
        if plugins.is_empty() {
            config.remove("plugin");
        }
    } else if install {
        config.insert(
            "plugin".into(),
            Value::Array(vec![Value::String(
                AgentId::OpenCode.spec().setup_package.into(),
            )]),
        );
    }
    write_object_atomic(path, config)
}

fn setup_opencode_at(path: &Path) -> anyhow::Result<()> {
    reconcile_opencode_at(path, true)
}

fn setup_opencode() -> anyhow::Result<()> {
    let settings_path = paths::agent_settings_path("opencode", None);
    let path = settings_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json");
    setup_opencode_at(&path)
}

fn setup_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["install", AgentId::Pi.spec().setup_package])
}

fn codex_plugin(value: &Value) -> Option<&Value> {
    value
        .get("installed")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| {
            item.get("pluginId").and_then(Value::as_str)
                == Some(AgentId::Codex.spec().uninstall_package)
        })
}

fn uninstall_codex(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("codex", &["plugin", "list", "--json"])?;
    if codex_plugin(&plugins).is_some() {
        runner.run(
            "codex",
            &["plugin", "remove", AgentId::Codex.spec().uninstall_package],
        )?;
    }
    Ok(())
}

fn uninstall_claude(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    let plugins = runner.json("claude", &["plugin", "list", "--json"])?;
    if claude_plugin(&plugins).is_some() {
        runner.run(
            "claude",
            &[
                "plugin",
                "uninstall",
                AgentId::Claude.spec().uninstall_package,
                "--scope",
                "user",
            ],
        )?;
    }
    Ok(())
}

fn uninstall_pi(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["remove", AgentId::Pi.spec().uninstall_package])
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

fn disable_tracing_at(path: &Path) -> anyhow::Result<()> {
    let mut settings = load_object(path)?;
    settings.insert("trace_to_braintrust".into(), Value::Bool(false));
    settings.remove("traceToBraintrust");
    write_object_atomic(path, settings)
}

fn uninstall_tracing_at(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut settings = load_object(path)?;
    settings.remove("trace_to_braintrust");
    settings.remove("traceToBraintrust");
    settings.remove("route");
    settings.remove("project");
    if settings.is_empty() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove agent settings: {}", path.display()))?;
        Ok(())
    } else {
        write_object_atomic(path, settings)
    }
}

fn settings_path(agent: AgentId) -> PathBuf {
    paths::agent_settings_path(agent.canonical_source(), None)
}

/// Install or refresh one agent's published tracing adapter and persist its
/// non-secret route selection.
pub fn run_setup(args: SetupArgs, route: SessionRoute) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let agent = args.agent;
    match agent {
        AgentId::Codex => {
            setup_codex(&mut runner)?;
        }
        AgentId::Claude => {
            setup_claude(&mut runner)?;
        }
        AgentId::OpenCode => {
            setup_opencode()?;
        }
        AgentId::Pi => {
            setup_pi(&mut runner)?;
        }
    }
    let spec = agent.spec();
    let settings_path = enable_tracing(spec.canonical_source, route)?;
    Ok(TraceCommandOutput::setup(spec, settings_path))
}

/// Disable tracing without removing the adapter or the selected non-secret route.
pub fn run_disable(args: LifecycleArgs) -> anyhow::Result<TraceCommandOutput> {
    let spec = args.agent.spec();
    let settings_path = settings_path(args.agent);
    disable_tracing_at(&settings_path)?;
    Ok(TraceCommandOutput::disable(spec, settings_path))
}

/// Remove only Braintrust-owned adapter registration and route configuration.
pub fn run_uninstall(args: LifecycleArgs) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let agent = args.agent;
    match agent {
        AgentId::Codex => uninstall_codex(&mut runner)?,
        AgentId::Claude => uninstall_claude(&mut runner)?,
        AgentId::OpenCode => {
            let settings_path = paths::agent_settings_path("opencode", None);
            let opencode_path = settings_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("opencode.json");
            reconcile_opencode_at(&opencode_path, false)?;
        }
        AgentId::Pi => uninstall_pi(&mut runner)?,
    }
    let spec = agent.spec();
    let settings_path = settings_path(agent);
    uninstall_tracing_at(&settings_path)?;
    Ok(TraceCommandOutput::uninstall(spec, settings_path))
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
    }

    #[test]
    fn missing_agent_executable_has_an_actionable_error() {
        let mut runner = SystemCommandRunner;
        let error = runner
            .run("bt-test-agent-that-does-not-exist", &[])
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("install bt-test-agent-that-does-not-exist and ensure it is on PATH"));
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
        assert!(runner.called(
            "claude plugin install trace-claude-code@braintrust-claude-plugin --scope user"
        ));
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
                "id": AgentId::Claude.spec().setup_package,
                "version": "1.4.4",
                "enabled": true
            }]),
        ]);

        setup_claude(&mut runner).unwrap();

        assert!(runner.called("claude plugin marketplace update braintrust-claude-plugin"));
        assert!(runner.called(
            "claude plugin update trace-claude-code@braintrust-claude-plugin --scope user"
        ));
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
        assert!(runner.called(
            "claude plugin install trace-claude-code@braintrust-claude-plugin --scope user"
        ));
    }

    #[test]
    fn claude_updates_then_enables_a_disabled_plugin() {
        let mut runner = FakeRunner::new([
            serde_json::json!([{
                "name": CLAUDE_MARKETPLACE,
                "source": "github",
                "repo": CLAUDE_MARKETPLACE_SOURCE
            }]),
            serde_json::json!([{"id": AgentId::Claude.spec().setup_package, "enabled": false}]),
        ]);

        setup_claude(&mut runner).unwrap();

        let update = runner
            .calls
            .iter()
            .position(|call| {
                call
                    == "claude plugin update trace-claude-code@braintrust-claude-plugin --scope user"
            })
            .unwrap();
        let enable = runner
            .calls
            .iter()
            .position(|call| {
                call
                    == "claude plugin enable trace-claude-code@braintrust-claude-plugin --scope user"
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

        let config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode@^1"])
        );

        setup_opencode_at(&path).unwrap();
        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode@^1"])
        );
    }

    #[test]
    fn opencode_uninstall_removes_only_braintrust_plugins() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@0.9.0","@braintrust/trace-opencode/tracing"],"model":"test/model"}"#,
        )
        .unwrap();

        reconcile_opencode_at(&path, false).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["plugin"], serde_json::json!(["other"]));
        assert_eq!(config["model"], "test/model");
    }

    #[test]
    fn opencode_uninstall_is_idempotent_and_does_not_create_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");

        reconcile_opencode_at(&path, false).unwrap();
        reconcile_opencode_at(&path, false).unwrap();

        assert!(!path.exists());
    }

    #[test]
    fn pi_installs_the_published_extension_range() {
        let mut runner = FakeRunner::new([]);

        setup_pi(&mut runner).unwrap();

        assert!(runner.called("pi install npm:@braintrust/pi-extension@^1"));
    }

    #[test]
    fn pi_uninstall_uses_the_unversioned_package_identity() {
        let mut runner = FakeRunner::new([]);

        uninstall_pi(&mut runner).unwrap();

        assert!(runner.called("pi remove npm:@braintrust/pi-extension"));
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
    fn disable_is_idempotent_and_preserves_route_and_unrelated_settings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"trace_to_braintrust":true,"route":{"additional_metadata":{"team":"sdk"}},"unrelated":{"keep":true}}"#,
        )
        .unwrap();

        disable_tracing_at(&path).unwrap();
        disable_tracing_at(&path).unwrap();

        let settings: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(settings["trace_to_braintrust"], false);
        assert_eq!(settings["route"]["additional_metadata"]["team"], "sdk");
        assert_eq!(settings["unrelated"]["keep"], true);
    }

    #[test]
    fn uninstall_removes_only_braintrust_owned_settings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"trace_to_braintrust":false,"traceToBraintrust":true,"route":{"destination":{"type":"project_logs","project_name":"agents"}},"project":"legacy","unrelated":{"keep":true}}"#,
        )
        .unwrap();

        uninstall_tracing_at(&path).unwrap();

        let settings: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(settings, serde_json::json!({"unrelated": {"keep": true}}));
    }

    #[test]
    fn uninstall_removes_an_owned_only_file_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(&path, r#"{"trace_to_braintrust":true,"route":{}}"#).unwrap();

        uninstall_tracing_at(&path).unwrap();
        uninstall_tracing_at(&path).unwrap();

        assert!(!path.exists());
    }

    #[test]
    fn plugin_uninstall_skips_absent_codex_and_claude_plugins() {
        let mut codex = FakeRunner::new([serde_json::json!({"installed": []})]);
        uninstall_codex(&mut codex).unwrap();
        assert_eq!(codex.calls, ["codex plugin list --json"]);

        let mut claude = FakeRunner::new([serde_json::json!([])]);
        uninstall_claude(&mut claude).unwrap();
        assert_eq!(claude.calls, ["claude plugin list --json"]);

        let mut project_claude = FakeRunner::new([serde_json::json!([{
            "id": AgentId::Claude.spec().uninstall_package,
            "scope": "project"
        }])]);
        uninstall_claude(&mut project_claude).unwrap();
        assert_eq!(project_claude.calls, ["claude plugin list --json"]);
    }

    #[test]
    fn plugin_uninstall_removes_exact_braintrust_plugins() {
        let mut codex = FakeRunner::new([serde_json::json!({
            "installed": [{"pluginId": AgentId::Codex.spec().uninstall_package}]
        })]);
        uninstall_codex(&mut codex).unwrap();
        assert!(codex.called("codex plugin remove trace-codex@braintrust-codex-plugins"));

        let mut claude = FakeRunner::new([serde_json::json!([{
            "id": AgentId::Claude.spec().uninstall_package
        }])]);
        uninstall_claude(&mut claude).unwrap();
        assert!(claude.called(
            "claude plugin uninstall trace-claude-code@braintrust-claude-plugin --scope user"
        ));
    }
}
