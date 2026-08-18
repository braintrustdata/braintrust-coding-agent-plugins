//! Stable, host-independent output contracts for user-facing trace commands.
//!
//! Embedders such as `bt` own their global `--json` flag, but should delegate
//! the output shape to this crate so every front-end reports daemon commands
//! consistently and JSON mode never falls back to human prose.

use crate::wire::StatusResult;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
}

impl From<bool> for OutputFormat {
    fn from(json: bool) -> Self {
        if json {
            Self::Json
        } else {
            Self::Human
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusCommandOutput {
    pub running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_ms: Option<u64>,
    pub sessions: Vec<crate::wire::SessionStatus>,
}

impl From<Option<StatusResult>> for StatusCommandOutput {
    fn from(status: Option<StatusResult>) -> Self {
        match status {
            Some(status) => Self {
                running: true,
                daemon_version: Some(status.daemon_version),
                uptime_ms: Some(status.uptime_ms),
                sessions: status.sessions,
            },
            None => Self {
                running: false,
                daemon_version: None,
                uptime_ms: None,
                sessions: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SetupCommandOutput {
    pub source: String,
    pub display_name: String,
    pub settings_path: PathBuf,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StopCommandOutput {
    pub running: bool,
    pub stopped: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TraceCommandOutput {
    Status(StatusCommandOutput),
    Enable(SetupCommandOutput),
    Disable(SetupCommandOutput),
    Stop(StopCommandOutput),
}

impl TraceCommandOutput {
    pub fn status(status: Option<StatusResult>) -> Self {
        Self::Status(status.into())
    }

    pub fn setup(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Enable(SetupCommandOutput {
            source: source.into(),
            display_name: display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
        })
    }

    pub fn stop(running: bool, stopped: bool) -> Self {
        Self::Stop(StopCommandOutput { running, stopped })
    }

    pub fn disable(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Disable(SetupCommandOutput {
            source: source.into(),
            display_name: display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
        })
    }

    pub fn render(&self, format: OutputFormat) -> anyhow::Result<String> {
        match format {
            OutputFormat::Json => Ok(serde_json::to_string(self)?),
            OutputFormat::Human => self.render_human(),
        }
    }

    fn render_human(&self) -> anyhow::Result<String> {
        match self {
            Self::Status(status) if !status.running => Ok("bt-daemon is not running".into()),
            Self::Status(status) => Ok(serde_json::to_string_pretty(&StatusResult {
                daemon_version: status.daemon_version.clone().unwrap_or_default(),
                uptime_ms: status.uptime_ms.unwrap_or_default(),
                sessions: status.sessions.clone(),
            })?),
            Self::Enable(setup) => Ok(format!(
                "The Braintrust tracing plugin is installed for {} and configured in {}.\nRestart the coding agent to load the tracing plugin.",
                setup.display_name,
                setup.settings_path.display()
            )),
            Self::Disable(disable) => Ok(format!(
                "The Braintrust tracing plugin and configuration were removed for {} from {}.\nRestart the coding agent to apply the change.",
                disable.display_name,
                disable.settings_path.display()
            )),
            Self::Stop(stop) if stop.stopped => Ok("Tracing daemon stopped.".into()),
            Self::Stop(_) => Ok("No tracing daemon is running.".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_status_is_machine_readable_in_json_mode() {
        let output = TraceCommandOutput::status(None);
        let rendered = output.render(OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["command"], "status");
        assert_eq!(value["running"], false);
        assert_eq!(value["sessions"], serde_json::json!([]));
        assert!(value.get("daemon_version").is_none());
        assert!(!rendered.contains("not running"));
    }

    #[test]
    fn enable_json_contains_stable_selection_fields_without_prose() {
        let output = TraceCommandOutput::setup(
            "opencode",
            "OpenCode",
            PathBuf::from("/tmp/opencode/braintrust.json"),
        );
        let rendered = output.render(OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["command"], "enable");
        assert_eq!(value["source"], "opencode");
        assert_eq!(value["restart_required"], true);
        assert!(!rendered.contains("installed for"));
    }

    #[test]
    fn disable_output_is_explicit() {
        let output = TraceCommandOutput::disable(
            "antigravity",
            "Google Antigravity",
            PathBuf::from("/tmp/antigravity/braintrust.json"),
        );
        let value: serde_json::Value =
            serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
        assert_eq!(value["command"], "disable");
        assert!(output
            .render(OutputFormat::Human)
            .unwrap()
            .contains("removed for Google Antigravity"));
    }

    #[test]
    fn stop_json_reports_idempotent_and_successful_shutdowns() {
        let absent: serde_json::Value = serde_json::from_str(
            &TraceCommandOutput::stop(false, false)
                .render(OutputFormat::Json)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            absent,
            serde_json::json!({
                "command": "stop",
                "running": false,
                "stopped": false
            })
        );

        let stopped: serde_json::Value = serde_json::from_str(
            &TraceCommandOutput::stop(true, true)
                .render(OutputFormat::Json)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            stopped,
            serde_json::json!({
                "command": "stop",
                "running": true,
                "stopped": true
            })
        );
    }

    #[test]
    fn human_output_preserves_existing_messages() {
        assert_eq!(
            TraceCommandOutput::status(None)
                .render(OutputFormat::Human)
                .unwrap(),
            "bt-daemon is not running"
        );
        assert_eq!(
            TraceCommandOutput::stop(false, false)
                .render(OutputFormat::Human)
                .unwrap(),
            "No tracing daemon is running."
        );
    }
}
