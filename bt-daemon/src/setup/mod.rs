//! Persistent installation and configuration for coding-agent tracing plugins.
//!
//! Each agent implements [`Setup`] in its own module. This module dispatches
//! to them through the agent registry and owns the Braintrust settings file
//! every agent shares.

mod antigravity;
mod claude;
mod codex;
mod common;
pub(crate) mod cursor;
mod grok;
pub(crate) mod muse;
mod opencode;
pub(crate) mod pi;

use crate::agents::{registrar, Agent};
use crate::paths;
use crate::trace_command::{SettingsScope, SetupAgent, SetupArgs};
use crate::wire::SessionRoute;
use crate::TraceCommandOutput;
use anyhow::Context;
pub(crate) use common::CommandRunner;
use common::{load_object, write_object_atomic_with, FileAccess, SystemCommandRunner};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// One agent's tracing adapter. Implemented in each agent's setup module and
/// registered in [`crate::agents`].
pub(crate) trait Setup: Agent {
    /// Install or refresh the published adapter.
    fn enable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()>;
    /// Remove the adapter if it is the Braintrust one.
    fn disable(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()>;
    /// Update an installed adapter. Must not install or enable it.
    fn update(&self, runner: &mut dyn CommandRunner) -> anyhow::Result<()>;
    /// Whether the installed adapter is older than the one this build installs.
    fn stale(&self) -> bool {
        false
    }
    /// A problem that keeps an enabled adapter from tracing, for `doctor`.
    fn activation_warning(&self) -> Option<&'static str> {
        None
    }
}

fn adapter(agent: SetupAgent) -> &'static dyn Setup {
    let name = agent.identity().id;
    registrar()
        .setup
        .get(name)
        .unwrap_or_else(|| panic!("no setup adapter is registered for {name}"))
}

/// A warning when the agent's installed tracing plugin is older than the one
/// this build would install.
pub(crate) fn update_warning(source: &str) -> Option<String> {
    let stale = registrar()
        .setup
        .get(source)
        .is_some_and(|agent| agent.stale());
    stale.then(|| format!("tracing plugin is out of date; run `bt trace update {source}`"))
}

fn scoped_path(agent: SetupAgent, scope: SettingsScope) -> anyhow::Result<PathBuf> {
    let source = agent.identity().id;
    match scope {
        SettingsScope::User => Ok(paths::agent_settings_path(source, None)),
        SettingsScope::Project => paths::project_settings_path(source, &std::env::current_dir()?),
    }
}

pub(crate) fn run_scoped_toggle(
    agent: SetupAgent,
    scope: SettingsScope,
    enabled: bool,
) -> anyhow::Result<TraceCommandOutput> {
    let path = scoped_path(agent, scope)?;
    let existed = path.exists();
    common::edit_object(&path, FileAccess::OwnerOnly, |settings, _| {
        settings.insert("trace_to_braintrust".into(), Value::Bool(enabled));
        Ok(())
    })?;
    let id = agent.identity();
    Ok((if enabled {
        let output = TraceCommandOutput::enable(id.id, id.display_name, path);
        if existed {
            output
        } else {
            output.into_enable("created")
        }
    } else {
        TraceCommandOutput::disable(id.id, id.display_name, path)
    })
    .with_scope(match scope {
        SettingsScope::User => "user",
        SettingsScope::Project => "project",
    }))
}

/// Remove the shared repository configuration without touching user installations.
pub(crate) fn run_project_uninstall(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let path = scoped_path(agent, SettingsScope::Project)?;
    crate::settings::with_settings_lock(&path, || remove_tracing_settings(&path))?;
    let id = agent.identity();
    Ok(TraceCommandOutput::uninstall(id.id, id.display_name, path).with_scope("project"))
}

pub(crate) fn run_project_setup(
    args: SetupArgs,
    selection: crate::HostRouteSelection,
    as_enable: bool,
) -> anyhow::Result<TraceCommandOutput> {
    let path = scoped_path(args.agent, SettingsScope::Project)?;
    let existed = path.exists();
    write_project_setup_at(&path, &args, &selection)?;
    let id = args.agent.identity();
    let output = TraceCommandOutput::project_install(id.id, id.display_name, path)
        .with_enabled(!args.disable);
    Ok(if as_enable {
        output.into_enable(if existed { "updated" } else { "created" })
    } else {
        output
    })
}

fn write_project_setup_at(
    path: &Path,
    args: &SetupArgs,
    selection: &crate::HostRouteSelection,
) -> anyhow::Result<()> {
    let root = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("project settings path has no parent"))?;
    let root = root.canonicalize()?;
    let plugins = args
        .plugin
        .iter()
        .map(|plugin| {
            let absolute = if plugin.is_absolute() {
                plugin.clone()
            } else {
                root.join(plugin)
            };
            let absolute = absolute
                .canonicalize()
                .with_context(|| format!("could not resolve span plugin {}", absolute.display()))?;
            let relative = absolute.strip_prefix(&root).map_err(|_| {
                anyhow::anyhow!(
                    "project span plugin {} must be inside the repository",
                    absolute.display()
                )
            })?;
            Ok((absolute.clone(), relative.to_path_buf()))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if !plugins.is_empty() {
        crate::span_processor::validate(
            &plugins
                .iter()
                .map(|(absolute, _)| absolute.clone())
                .collect::<Vec<_>>(),
        )?;
    }
    common::edit_object(path, FileAccess::OwnerOnly, |settings, _| {
        settings.insert("trace_to_braintrust".into(), Value::Bool(!args.disable));
        for (key, value) in [
            ("project", selection.project_name.as_deref()),
            ("profile", selection.auth.profile.as_deref()),
            ("org_name", selection.auth.org_name.as_deref()),
        ] {
            if let Some(value) = value {
                settings.insert(key.into(), Value::String(value.to_owned()));
            }
        }
        if args.no_inherit_plugins {
            settings.insert("inherit_plugins".into(), Value::Bool(false));
        }
        if args.inherit_plugins {
            settings.insert("inherit_plugins".into(), Value::Bool(true));
        }
        if !plugins.is_empty() {
            let route = settings
                .entry("route")
                .or_insert_with(|| Value::Object(Default::default()));
            let route = route
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("project route must be an object"))?;
            route.insert(
                "span_plugins".into(),
                serde_json::to_value(
                    plugins
                        .iter()
                        .map(|(_, relative)| {
                            relative
                                .to_string_lossy()
                                .replace(std::path::MAIN_SEPARATOR, "/")
                        })
                        .collect::<Vec<_>>(),
                )?,
            );
        }
        if let Some(metadata) = args.additional_metadata.as_deref() {
            let parsed: Value = serde_json::from_str(metadata)?;
            if !parsed.is_object() {
                anyhow::bail!("additional metadata must be a JSON object");
            }
            settings.insert("additional_metadata".into(), parsed);
        }
        if !args.tags.is_empty() {
            settings.insert("tags".into(), serde_json::to_value(&args.tags)?);
        }
        Ok(())
    })?;
    Ok(())
}

/// Install or refresh one agent's published tracing adapter and persist its
/// non-secret route selection.
pub fn run_setup(args: SetupArgs, route: SessionRoute) -> anyhow::Result<TraceCommandOutput> {
    let agent = adapter(args.agent);
    agent.enable(&mut SystemCommandRunner)?;
    let identity = agent.identity();
    let settings_path = enable_tracing(identity.id, route, !args.disable, args.no_inherit_plugins)?;
    Ok(
        TraceCommandOutput::install(identity.id, identity.display_name, settings_path)
            .with_enabled(!args.disable),
    )
}

/// Check whether there is a usable saved configuration, without overwriting errors.
pub(crate) fn has_saved_setup(agent: SetupAgent) -> anyhow::Result<bool> {
    let path = paths::agent_settings_path(agent.identity().id, None);
    saved_setup_at(&path)
}

fn saved_setup_at(path: &Path) -> anyhow::Result<bool> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    let value: Value = serde_json::from_str(&raw)
        .with_context(|| format!("invalid tracing settings at {}", path.display()))?;
    let object = value.as_object().ok_or_else(|| {
        anyhow::anyhow!("tracing settings at {} must be an object", path.display())
    })?;
    if !object
        .get("trace_to_braintrust")
        .is_some_and(Value::is_boolean)
    {
        anyhow::bail!(
            "tracing settings at {} have no valid `trace_to_braintrust`",
            path.display()
        );
    }
    if let Some(route) = object.get("route") {
        let _: SessionRoute = serde_json::from_value(route.clone())
            .with_context(|| format!("invalid tracing route at {}", path.display()))?;
    }
    Ok(!(object.len() == 1 && object.get("trace_to_braintrust") == Some(&Value::Bool(false))))
}

/// Resume tracing without reinstalling the adapter or replacing its route.
pub fn run_enable(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let identity = agent.identity();
    let path = paths::agent_settings_path(identity.id, None);
    enable_existing_at(&path)?;
    Ok(TraceCommandOutput::enable(
        identity.id,
        identity.display_name,
        path,
    ))
}

fn enable_existing_at(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        anyhow::bail!(
            "tracing is not configured at {}; run `bt trace install` first",
            path.display()
        );
    }
    common::edit_object(path, FileAccess::OwnerOnly, |settings, exists| {
        if !exists
            || !settings
                .get("trace_to_braintrust")
                .is_some_and(Value::is_boolean)
        {
            anyhow::bail!(
                "tracing is not configured at {}; run `bt trace install` first",
                path.display()
            );
        }
        settings.insert("trace_to_braintrust".into(), Value::Bool(true));
        Ok(())
    })
}

/// Disable tracing while retaining the installed adapter and saved route.
pub fn run_disable(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let identity = agent.identity();
    let path = paths::agent_settings_path(identity.id, None);
    disable_tracing_at(&path)?;
    Ok(TraceCommandOutput::disable(
        identity.id,
        identity.display_name,
        path,
    ))
}

fn disable_tracing_at(path: &Path) -> anyhow::Result<()> {
    common::edit_object(path, FileAccess::OwnerOnly, |settings, _| {
        settings.insert("trace_to_braintrust".into(), Value::Bool(false));
        Ok(())
    })
}

/// Uninstall an agent's tracing adapter and remove its Braintrust-owned settings.
pub fn run_uninstall(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let agent = adapter(agent);
    let adapter_result = agent.disable(&mut SystemCommandRunner);
    let identity = agent.identity();
    let settings_path = paths::agent_settings_path(identity.id, None);
    finish_disable(adapter_result, &settings_path)?;
    Ok(TraceCommandOutput::uninstall(
        identity.id,
        identity.display_name,
        settings_path,
    ))
}

/// Update an already installed tracing adapter without writing tracing settings,
/// enabling a disabled plugin, or creating an agent configuration file.
pub fn run_update(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let agent = adapter(agent);
    agent.update(&mut SystemCommandRunner)?;
    let identity = agent.identity();
    Ok(TraceCommandOutput::update(
        identity.id,
        identity.display_name,
    ))
}

fn enable_tracing(
    source: &str,
    route: SessionRoute,
    enabled: bool,
    replace_plugins: bool,
) -> anyhow::Result<PathBuf> {
    let path = paths::agent_settings_path(source, None);
    write_tracing_at(&path, route, enabled, replace_plugins)?;
    Ok(path)
}

/// Write the route to an agent's Braintrust settings, keeping saved metadata,
/// tags, and span plugins unless the new route replaces them, and dropping
/// keys from earlier settings formats.
#[cfg(test)]
fn enable_tracing_at(path: &Path, route: SessionRoute) -> anyhow::Result<()> {
    write_tracing_at(path, route, true, false)
}

fn write_tracing_at(
    path: &Path,
    mut route: SessionRoute,
    enabled: bool,
    replace_plugins: bool,
) -> anyhow::Result<()> {
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
    if route.span_plugins.is_empty() && !replace_plugins {
        route.span_plugins = settings
            .get("route")
            .and_then(|route| route.get("span_plugins"))
            .and_then(|plugins| serde_json::from_value(plugins.clone()).ok())
            .unwrap_or_default();
    }
    settings.insert("trace_to_braintrust".into(), Value::Bool(enabled));
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

fn remove_tracing_settings(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to remove tracing settings: {}", path.display())),
    }
}

/// Remove the settings even when adapter cleanup failed, so tracing stops
/// either way; the adapter's error still takes precedence.
fn finish_disable(adapter_result: anyhow::Result<()>, settings_path: &Path) -> anyhow::Result<()> {
    let settings_result = remove_tracing_settings(settings_path);
    adapter_result.and(settings_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{AuthSelection, TraceDestination};

    #[test]
    fn every_setup_subcommand_has_a_registered_adapter() {
        for agent in [
            SetupAgent::Codex,
            SetupAgent::Claude,
            SetupAgent::OpenCode,
            SetupAgent::Pi,
            SetupAgent::Grok,
            SetupAgent::Cursor,
            SetupAgent::Antigravity,
        ] {
            assert_eq!(adapter(agent).identity().id, agent.identity().id);
        }
    }

    #[test]
    fn project_setup_writes_only_explicit_values_and_preserves_existing_fields() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".braintrust.json");
        let mut args = SetupArgs {
            no_inherit_plugins: false,
            inherit_plugins: false,
            disable: false,
            scope: SettingsScope::Project,
            agent: SetupAgent::Claude,
            additional_metadata: None,
            tags: vec![],
            plugin: vec![],
        };
        write_project_setup_at(&path, &args, &crate::HostRouteSelection::default()).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value, serde_json::json!({"trace_to_braintrust": true}));
        args.tags = vec!["team".into()];
        args.additional_metadata = Some(r#"{"ci":true}"#.into());
        let selection = crate::HostRouteSelection {
            project_name: Some("project".into()),
            ..Default::default()
        };
        write_project_setup_at(&path, &args, &selection).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"trace_to_braintrust": true,"project":"project","tags":["team"],"additional_metadata":{"ci":true}})
        );
        let plugins = temp.path().join("plugins");
        std::fs::create_dir(&plugins).unwrap();
        std::fs::write(plugins.join("redact.mjs"), "export default span => span").unwrap();
        args.plugin.push("plugins/redact.mjs".into());
        args.no_inherit_plugins = true;
        write_project_setup_at(&path, &args, &selection).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            value["route"]["span_plugins"],
            serde_json::json!(["plugins/redact.mjs"])
        );
        assert_eq!(value["inherit_plugins"], false);
        args.no_inherit_plugins = false;
        args.inherit_plugins = true;
        write_project_setup_at(&path, &args, &selection).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["inherit_plugins"], true);
        args.plugin.clear();
        args.plugin.push("bad.js".into());
        assert!(write_project_setup_at(&path, &args, &selection).is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
            value
        );
    }

    #[test]
    fn disable_preserves_route_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        std::fs::write(
            &path,
            r#"{"trace_to_braintrust":true,"route":{"auth":{"profile":"saved-profile","org_name":"saved-org"},"destination":{"type":"project_logs","project_name":"saved-project"},"tags":["saved"],"span_plugins":["/tmp/plugin.js"]},"custom":1}"#,
        )
        .unwrap();
        disable_tracing_at(&path).unwrap();
        disable_tracing_at(&path).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["trace_to_braintrust"], false);
        assert_eq!(value["route"]["tags"][0], "saved");
        assert_eq!(value["custom"], 1);
        enable_existing_at(&path).unwrap();
        let enabled: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(enabled["trace_to_braintrust"], true);
        assert_eq!(enabled["route"], value["route"]);
        assert_eq!(enabled["custom"], value["custom"]);
        let missing = temp.path().join("missing/braintrust.json");
        assert!(enable_existing_at(&missing).is_err());
        assert!(!missing.exists());
        disable_tracing_at(&missing).unwrap();
        let created: Value = serde_json::from_slice(&std::fs::read(&missing).unwrap()).unwrap();
        assert_eq!(created["trace_to_braintrust"], false);
    }

    #[test]
    fn saved_setup_detection_preserves_errors_and_distinguishes_pause_only() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        assert!(!saved_setup_at(&path).unwrap());
        disable_tracing_at(&path).unwrap();
        assert!(!saved_setup_at(&path).unwrap());
        std::fs::write(&path, r#"{"trace_to_braintrust":false,"route":{"destination":{"type":"project_logs","project_name":"saved"}}}"#).unwrap();
        assert!(saved_setup_at(&path).unwrap());
        std::fs::write(&path, "broken").unwrap();
        assert!(saved_setup_at(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "broken");
        std::fs::write(
            &path,
            r#"{"trace_to_braintrust":true,"route":{"destination":"invalid"}}"#,
        )
        .unwrap();
        assert!(saved_setup_at(&path).is_err());
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
            tags: ["replacement".to_string()].into_iter().collect(),
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
