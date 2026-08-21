//! Per-agent, non-credential tracing settings.
//!
//! Every coding agent owns an independent persistent file with the same schema.
//! Authentication and backend URLs deliberately stay with the embedding `bt`
//! CLI. `bt trace run` overlays invocation settings without mutating any
//! persistent agent configuration.

use crate::paths;
use crate::wire::SessionRoute;
use serde::{Deserialize, Serialize};
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

    pub(crate) fn configured_span_plugins(source: &str) -> Vec<std::path::PathBuf> {
        Self::load_from(&paths::agent_settings_path(source, None))
            .route
            .map(|route| route.span_plugins)
            .unwrap_or_default()
    }
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
}
