//! Shared, non-credential settings for every agent connected to the daemon.
//!
//! Authentication and backend URLs deliberately stay with the embedding `bt`
//! CLI. This file only controls tracing behavior that should be consistent
//! across Codex, Claude Code, and future agent plugins.

use crate::paths;
use crate::wire::SessionRoute;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) const INVOCATION_SETTINGS_ENV: &str = "BT_TRACE_INVOCATION_SETTINGS";

/// Non-secret settings scoped to one `bt trace run` process tree.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub(crate) struct SharedSettings {
    pub trace_to_braintrust: Option<bool>,
    pub route: Option<SessionRoute>,
}

impl SharedSettings {
    pub(crate) fn load() -> Self {
        let invocation = std::env::var(INVOCATION_SETTINGS_ENV).ok();
        Self::load_from_sources(&paths::settings_path(None), invocation.as_deref())
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
        self.tracing_enabled_with(env_bool("TRACE_TO_BRAINTRUST"))
    }

    fn tracing_enabled_with(&self, environment: Option<bool>) -> bool {
        self.trace_to_braintrust.or(environment).unwrap_or(false)
    }
}

pub(crate) fn env_bool(name: &str) -> Option<bool> {
    let value = std::env::var(name).ok()?;
    parse_bool(&value)
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
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
                "traceToBraintrust": true,
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

        let settings = SharedSettings::load_from(&path);
        assert_eq!(settings.trace_to_braintrust, Some(true));
        let route = settings.route.unwrap();
        assert_eq!(route.destination.unwrap().project_name(), Some("agents"));
        assert_eq!(route.additional_metadata.unwrap()["team"], "platform");
    }

    #[test]
    fn malformed_or_missing_settings_are_fail_open() {
        let temp = tempfile::tempdir().unwrap();
        assert!(SharedSettings::load_from(&temp.path().join("missing.json"))
            .route
            .is_none());
        let malformed = temp.path().join("malformed.json");
        std::fs::write(&malformed, "{").unwrap();
        assert!(SharedSettings::load_from(&malformed).route.is_none());
    }

    #[test]
    fn file_enablement_overrides_environment_fallback() {
        let enabled = SharedSettings {
            trace_to_braintrust: Some(true),
            ..Default::default()
        };
        assert!(enabled.tracing_enabled_with(Some(false)));

        let disabled = SharedSettings {
            trace_to_braintrust: Some(false),
            ..Default::default()
        };
        assert!(!disabled.tracing_enabled_with(Some(true)));

        assert!(SharedSettings::default().tracing_enabled_with(Some(true)));
        assert!(!SharedSettings::default().tracing_enabled_with(None));
    }

    #[test]
    fn invocation_settings_override_setup_without_mutating_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "traceToBraintrust": false,
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
                ..SessionRoute::default()
            }))
            .unwrap()
        };

        let work =
            SharedSettings::load_from_sources(&path, Some(&invocation("work", "work-project")));
        let personal = SharedSettings::load_from_sources(
            &path,
            Some(&invocation("personal", "personal-project")),
        );
        let global = SharedSettings::load_from_sources(&path, None);

        assert!(work.tracing_enabled_with(None));
        assert!(personal.tracing_enabled_with(None));
        assert_eq!(work.route.unwrap().auth.profile.as_deref(), Some("work"));
        assert_eq!(
            personal.route.unwrap().auth.profile.as_deref(),
            Some("personal")
        );
        assert!(!global.tracing_enabled_with(None));
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
                "traceToBraintrust": true,
                "route": {
                    "destination": {"type": "project_logs", "project_name": "global-project"}
                }
            }"#,
        )
        .unwrap();

        let settings = SharedSettings::load_from_sources(&path, Some("{"));
        assert!(!settings.tracing_enabled_with(None));
        assert!(settings.route.is_none());
    }

    #[test]
    fn boolean_environment_values_match_launcher_contract() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert_eq!(parse_bool(value), Some(true));
        }
        for value in ["0", "false", "FALSE", "no", "off"] {
            assert_eq!(parse_bool(value), Some(false));
        }
        assert_eq!(parse_bool("sometimes"), None);
    }
}
