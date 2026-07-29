//! Shared, non-credential settings for every agent connected to the daemon.
//!
//! Authentication and backend URLs deliberately stay with the embedding `bt`
//! CLI. This file only controls tracing behavior that should be consistent
//! across Codex, Claude Code, and future agent plugins.

use crate::paths;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::path::Path;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SharedSettings {
    pub trace_to_braintrust: Option<bool>,
    pub project: Option<String>,
    pub flush_on_turn_end: Option<bool>,
    pub additional_metadata: Option<Map<String, Value>>,
}

impl SharedSettings {
    pub(crate) fn load() -> Self {
        Self::load_from(&paths::settings_path(None))
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
                "project": "agents",
                "flushOnTurnEnd": false,
                "additionalMetadata": {"team": "platform"},
                "apiKey": "ignored",
                "apiUrl": "https://ignored.example"
            }"#,
        )
        .unwrap();

        let settings = SharedSettings::load_from(&path);
        assert_eq!(settings.trace_to_braintrust, Some(true));
        assert_eq!(settings.project.as_deref(), Some("agents"));
        assert_eq!(settings.flush_on_turn_end, Some(false));
        assert_eq!(
            settings.additional_metadata.unwrap()["team"],
            Value::String("platform".into())
        );
    }

    #[test]
    fn malformed_or_missing_settings_are_fail_open() {
        let temp = tempfile::tempdir().unwrap();
        assert!(SharedSettings::load_from(&temp.path().join("missing.json"))
            .project
            .is_none());
        let malformed = temp.path().join("malformed.json");
        std::fs::write(&malformed, "{").unwrap();
        assert!(SharedSettings::load_from(&malformed).project.is_none());
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
