//! Persistent installation and configuration for coding-agent tracing plugins.
//!
//! Each agent's adapter lives in its own module and exposes `enable`,
//! `disable`, and `update`; agents that can detect an outdated installation
//! also expose `stale`. This module dispatches to them and owns the
//! Braintrust settings file every agent shares.

mod antigravity;
mod claude;
mod codex;
mod common;
pub(crate) mod cursor;
mod grok;
mod opencode;
pub(crate) mod pi;

use crate::paths;
use crate::trace_command::{EnableArgs, SetupAgent};
use crate::wire::SessionRoute;
use crate::TraceCommandOutput;
use anyhow::Context;
use common::{load_object, write_object_atomic_with, FileAccess, SystemCommandRunner};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The settings source name and display name for an agent.
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

/// A warning when the agent's installed tracing plugin is older than the one
/// this build would install.
pub(crate) fn update_warning(source: &str) -> Option<String> {
    let stale = match source {
        "codex" => codex::stale(),
        "claude" => claude::stale(),
        "opencode" => opencode::stale(),
        "pi" => pi::stale(),
        "cursor" => cursor::stale(),
        _ => false,
    };
    stale.then(|| format!("tracing plugin is out of date; run `bt trace update {source}`"))
}

/// Install or refresh one agent's published tracing adapter and persist its
/// non-secret route selection.
pub fn run_enable(args: EnableArgs, route: SessionRoute) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    match args.agent {
        SetupAgent::Codex => codex::enable(&mut runner)?,
        SetupAgent::Claude => claude::enable(&mut runner)?,
        SetupAgent::OpenCode => opencode::enable()?,
        SetupAgent::Pi => pi::enable(&mut runner)?,
        SetupAgent::Grok => grok::enable(&mut runner)?,
        SetupAgent::Cursor => cursor::enable()?,
        SetupAgent::Antigravity => antigravity::enable(&mut runner)?,
    }
    let (source, display_name) = agent_details(args.agent);
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

/// Uninstall an agent's tracing adapter and remove its Braintrust-owned settings.
pub fn run_disable(agent: SetupAgent) -> anyhow::Result<TraceCommandOutput> {
    let mut runner = SystemCommandRunner;
    let adapter_result = match agent {
        SetupAgent::Codex => codex::disable(&mut runner),
        SetupAgent::Claude => claude::disable(&mut runner),
        SetupAgent::OpenCode => opencode::disable(),
        SetupAgent::Pi => pi::disable(&mut runner),
        SetupAgent::Grok => grok::disable(&mut runner),
        SetupAgent::Cursor => cursor::disable(),
        SetupAgent::Antigravity => antigravity::disable(&mut runner),
    };
    let (source, display_name) = agent_details(agent);
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
    match agent {
        SetupAgent::Codex => codex::update(&mut runner)?,
        SetupAgent::Claude => claude::update(&mut runner)?,
        SetupAgent::OpenCode => opencode::update()?,
        SetupAgent::Pi => pi::update(&mut runner)?,
        SetupAgent::Grok => grok::update(&mut runner)?,
        SetupAgent::Cursor => cursor::update()?,
        SetupAgent::Antigravity => antigravity::update(&mut runner)?,
    }
    let (source, display_name) = agent_details(agent);
    Ok(TraceCommandOutput::update(source, display_name))
}

fn enable_tracing(source: &str, route: SessionRoute) -> anyhow::Result<PathBuf> {
    let path = paths::agent_settings_path(source, None);
    enable_tracing_at(&path, route)?;
    Ok(path)
}

/// Write the route to an agent's Braintrust settings, keeping saved metadata,
/// tags, and span plugins unless the new route replaces them, and dropping
/// keys from earlier settings formats.
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
