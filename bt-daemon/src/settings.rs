//! Per-agent, non-credential tracing settings.
//!
//! Every coding agent owns an independent persistent file with the same schema.
//! Authentication and backend URLs deliberately stay with the embedding `bt`
//! CLI. `bt trace run` overlays invocation settings without mutating any
//! persistent agent configuration.

use crate::paths;
use crate::wire::SessionRoute;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::Path;

pub(crate) const INVOCATION_SETTINGS_ENV: &str = "BT_TRACE_INVOCATION_SETTINGS";

/// Non-secret settings scoped to one `bt trace run` process tree.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InvocationSettings {
    pub trace_to_braintrust: bool,
    pub route: SessionRoute,
}

impl InvocationSettings {
    pub(crate) fn enabled(route: SessionRoute) -> Self {
        Self {
            trace_to_braintrust: true,
            route,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AgentSettings {
    pub trace_to_braintrust: Option<bool>,
    pub route: Option<SessionRoute>,
}

impl AgentSettings {
    pub(crate) fn load(source: &str) -> Self {
        let invocation = std::env::var(INVOCATION_SETTINGS_ENV).ok();
        Self::load_from_sources(
            &paths::agent_settings_path(source, None),
            invocation.as_deref(),
        )
    }

    fn load_from_sources(path: &Path, invocation: Option<&str>) -> Self {
        let mut settings = Self::load_from(path);
        if let Some(raw) = invocation {
            match serde_json::from_str::<InvocationSettings>(raw) {
                Ok(invocation) => {
                    settings.trace_to_braintrust = Some(invocation.trace_to_braintrust);
                    settings.route = Some(invocation.route);
                }
                Err(error) => {
                    tracing::warn!("managed run settings ignored: {error}");
                    // Never fall back to the persistent route for a managed
                    // child whose invocation selection cannot be decoded.
                    settings.trace_to_braintrust = Some(false);
                    settings.route = None;
                }
            }
        }
        settings
    }

    fn load_from(path: &Path) -> Self {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(error) => {
                tracing::warn!(path = %path.display(), "shared daemon settings ignored: {error}");
                return Self::default();
            }
        };
        match serde_json::from_str(&raw) {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(path = %path.display(), "shared daemon settings ignored: {error}");
                Self::default()
            }
        }
    }

    pub(crate) fn tracing_enabled(&self) -> bool {
        self.trace_to_braintrust.unwrap_or(false)
    }
}

/// Replace a still-matching name-only route with the host-canonical route.
/// The caller must hold the settings lock used by setup while invoking this.
pub(crate) fn migrate_persisted_route(
    source: &str,
    legacy_route: &SessionRoute,
    canonical_route: &SessionRoute,
) -> anyhow::Result<bool> {
    let path = paths::agent_settings_path(source, None);
    migrate_persisted_route_at(&path, legacy_route, canonical_route)
}

fn migrate_persisted_route_at(
    path: &Path,
    legacy_route: &SessionRoute,
    canonical_route: &SessionRoute,
) -> anyhow::Result<bool> {
    with_settings_lock(path, || {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let mut settings: Map<String, Value> = serde_json::from_str(&raw)?;
        let Some(stored) = settings.get("route") else {
            return Ok(false);
        };
        let stored: SessionRoute = serde_json::from_value(stored.clone())?;
        // Do not overwrite a route that setup changed while resolution was in
        // flight. `same_route` deliberately treats a legacy route and its
        // stable-ID successor as equivalent for journal replay, so the
        // persistence guard must compare their exact serialized forms.
        if serde_json::to_value(&stored)? != serde_json::to_value(legacy_route)? {
            return Ok(false);
        }
        settings.insert("route".into(), serde_json::to_value(canonical_route)?);
        write_json_atomic(path, &settings)?;
        Ok(true)
    })
}

/// Serialize all Braintrust-owned updates to an agent settings file. The lock
/// stays separate from the JSON file so atomic replacement never drops it.
pub(crate) fn with_settings_lock<T>(
    path: &Path,
    action: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)?;
    let lock_path = path.with_extension(format!(
        "{}lock",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
    ));
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    lock.lock_exclusive()?;
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

fn write_json_atomic(path: &Path, settings: &Map<String, Value>) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    let mut encoded = serde_json::to_string_pretty(&Value::Object(settings.clone()))?;
    encoded.push('\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(encoded.as_bytes())?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_file_contains_behavior_only() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": true,
                "route": {
                    "destination": {"type": "project_logs", "project_name": "agents"},
                    "flush_mode": "fire_and_forget",
                    "additional_metadata": {"team": "platform"}
                },
                "apiKey": "ignored",
                "apiUrl": "https://ignored.example"
            }"#,
        )
        .unwrap();

        let settings = AgentSettings::load_from(&path);
        assert_eq!(settings.trace_to_braintrust, Some(true));
        let route = settings.route.unwrap();
        assert_eq!(route.destination.unwrap().project_name(), Some("agents"));
        assert_eq!(route.additional_metadata.unwrap()["team"], "platform");
    }

    #[test]
    fn malformed_or_missing_settings_are_fail_open() {
        let temp = tempfile::tempdir().unwrap();
        assert!(AgentSettings::load_from(&temp.path().join("missing.json"))
            .route
            .is_none());
        let malformed = temp.path().join("malformed.json");
        std::fs::write(&malformed, "{").unwrap();
        assert!(AgentSettings::load_from(&malformed).route.is_none());
    }

    #[test]
    fn tracing_enablement_comes_only_from_stored_settings() {
        let enabled = AgentSettings {
            trace_to_braintrust: Some(true),
            ..Default::default()
        };
        assert!(enabled.tracing_enabled());

        let disabled = AgentSettings {
            trace_to_braintrust: Some(false),
            ..Default::default()
        };
        assert!(!disabled.tracing_enabled());

        assert!(!AgentSettings::default().tracing_enabled());
    }

    #[test]
    fn invocation_settings_override_setup_without_mutating_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": false,
                "route": {
                    "auth": {"profile": "global", "org_name": "global-org"},
                    "destination": {"type": "project_logs", "project_name": "global-project"}
                }
            }"#,
        )
        .unwrap();

        let invocation = |profile: &str, project: &str| {
            serde_json::to_string(&InvocationSettings::enabled(SessionRoute {
                auth: crate::wire::AuthSelection {
                    source: crate::wire::AuthSource::SavedProfile,
                    profile_id: None,
                    profile: Some(profile.to_string()),
                    org_name: Some(format!("{profile}-org")),
                },
                destination: Some(crate::wire::TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some(project.to_string()),
                }),
                additional_metadata: Some(serde_json::json!({"profile": profile})),
                ..SessionRoute::default()
            }))
            .unwrap()
        };

        let work =
            AgentSettings::load_from_sources(&path, Some(&invocation("work", "work-project")));
        let personal = AgentSettings::load_from_sources(
            &path,
            Some(&invocation("personal", "personal-project")),
        );
        let global = AgentSettings::load_from_sources(&path, None);

        assert!(work.tracing_enabled());
        assert!(personal.tracing_enabled());
        let work_route = work.route.unwrap();
        assert_eq!(work_route.auth.profile.as_deref(), Some("work"));
        assert_eq!(
            work_route.additional_metadata,
            Some(serde_json::json!({"profile": "work"}))
        );
        let personal_route = personal.route.unwrap();
        assert_eq!(personal_route.auth.profile.as_deref(), Some("personal"));
        assert_eq!(
            personal_route.additional_metadata,
            Some(serde_json::json!({"profile": "personal"}))
        );
        assert!(!global.tracing_enabled());
        let global_route = global.route.unwrap();
        assert_eq!(global_route.auth.profile.as_deref(), Some("global"));
        assert_eq!(
            global_route.destination.unwrap().project_name(),
            Some("global-project")
        );
    }

    #[test]
    fn malformed_invocation_settings_do_not_fall_back_to_setup_route() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": true,
                "route": {
                    "destination": {"type": "project_logs", "project_name": "global-project"}
                }
            }"#,
        )
        .unwrap();

        let settings = AgentSettings::load_from_sources(&path, Some("{"));
        assert!(!settings.tracing_enabled());
        assert!(settings.route.is_none());
    }

    #[test]
    fn migration_replaces_only_the_matching_legacy_route() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let legacy = SessionRoute {
            auth: crate::wire::AuthSelection {
                source: crate::wire::AuthSource::Auto,
                profile_id: None,
                profile: Some("test-profile".into()),
                org_name: Some("test-org".into()),
            },
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("test-project".into()),
            }),
            ..SessionRoute::default()
        };
        let mut canonical = legacy.clone();
        canonical.auth.source = crate::wire::AuthSource::SavedProfile;
        canonical.auth.profile_id = Some("00000000-0000-4000-8000-000000000001".into());
        let settings = serde_json::json!({
            "trace_to_braintrust": true,
            "route": legacy,
            "unrelated": { "preserved": true },
        });
        std::fs::write(&path, serde_json::to_vec(&settings).unwrap()).unwrap();

        assert!(migrate_persisted_route_at(&path, &legacy, &canonical).unwrap());
        let migrated: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            migrated["route"]["auth"]["profile_id"],
            "00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(migrated["unrelated"]["preserved"], true);

        assert!(!migrate_persisted_route_at(&path, &legacy, &canonical).unwrap());
    }
}
