//! Stable, host-independent output contracts for user-facing trace commands.
//!
//! Embedders such as `bt` own their global `--json` flag, but should delegate
//! the output shape to this crate so every front-end reports daemon commands
//! consistently and JSON mode never falls back to human prose.

use crate::wire::StatusResult;
use crate::AgentSpec;
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

/// Setup, disable, and uninstall report the same stable agent lifecycle shape.
pub type LifecycleCommandOutput = SetupCommandOutput;

#[derive(Debug, Clone, Serialize)]
pub struct StopCommandOutput {
    pub running: bool,
    pub stopped: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TraceCommandOutput {
    Status(StatusCommandOutput),
    Setup(SetupCommandOutput),
    Disable(LifecycleCommandOutput),
    Uninstall(LifecycleCommandOutput),
    Stop(StopCommandOutput),
}

impl TraceCommandOutput {
    pub fn status(status: Option<StatusResult>) -> Self {
        Self::Status(status.into())
    }

    pub fn setup(spec: &AgentSpec, settings_path: impl Into<PathBuf>) -> Self {
        Self::Setup(SetupCommandOutput {
            source: spec.canonical_source.into(),
            display_name: spec.display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
        })
    }

    pub fn disable(spec: &AgentSpec, settings_path: impl Into<PathBuf>) -> Self {
        Self::Disable(SetupCommandOutput {
            source: spec.canonical_source.into(),
            display_name: spec.display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
        })
    }

    pub fn uninstall(spec: &AgentSpec, settings_path: impl Into<PathBuf>) -> Self {
        Self::Uninstall(SetupCommandOutput {
            source: spec.canonical_source.into(),
            display_name: spec.display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
        })
    }

    pub fn stop(running: bool, stopped: bool) -> Self {
        Self::Stop(StopCommandOutput { running, stopped })
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
            Self::Setup(setup) => Ok(format!(
                "The Braintrust tracing plugin is installed for {} and configured in {}.\nRestart the coding agent to load the tracing plugin.",
                setup.display_name,
                setup.settings_path.display()
            )),
            Self::Disable(disable) => Ok(format!(
                "Braintrust tracing is disabled for {} in {}.\nRestart the coding agent to unload tracing.",
                disable.display_name,
                disable.settings_path.display()
            )),
            Self::Uninstall(uninstall) => Ok(format!(
                "The Braintrust tracing plugin is uninstalled for {} and its route was removed from {}.\nRestart the coding agent to unload tracing.",
                uninstall.display_name,
                uninstall.settings_path.display()
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
    fn setup_json_contains_stable_selection_fields_without_prose() {
        let output = TraceCommandOutput::setup(
            crate::AgentId::OpenCode.spec(),
            PathBuf::from("/tmp/opencode/braintrust.json"),
        );
        let rendered = output.render(OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["command"], "setup");
        assert_eq!(value["source"], "opencode");
        assert_eq!(value["restart_required"], true);
        assert!(!rendered.contains("installed for"));
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
    fn lifecycle_json_uses_canonical_source_names() {
        let path = PathBuf::from("/tmp/claude/braintrust.json");
        let disabled: serde_json::Value = serde_json::from_str(
            &TraceCommandOutput::disable(crate::AgentId::Claude.spec(), path.clone())
                .render(OutputFormat::Json)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(disabled["command"], "disable");
        assert_eq!(disabled["source"], "claude-code");
        assert_eq!(disabled["restart_required"], true);

        let uninstalled: serde_json::Value = serde_json::from_str(
            &TraceCommandOutput::uninstall(crate::AgentId::Claude.spec(), path)
                .render(OutputFormat::Json)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(uninstalled["command"], "uninstall");
        assert_eq!(uninstalled["source"], "claude-code");
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
