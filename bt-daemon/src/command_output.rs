//! Stable, host-independent output contracts for user-facing trace commands.
//!
//! Embedders such as `bt` own their global `--json` flag, but should delegate
//! the output shape to this crate so every front-end reports daemon commands
//! consistently and JSON mode never falls back to human prose.

use crate::wire::{SessionRoute, StatusResult, TraceDestination};
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
pub struct ImportSummary {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<TraceDestination>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_span_id: Option<String>,
    pub span_count: usize,
    pub finalized: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthDiagnostic {
    pub status: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorCommandOutput {
    pub source: String,
    pub display_name: String,
    pub settings_path: PathBuf,
    pub settings_present: bool,
    pub enabled: bool,
    pub route_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
    pub auth: AuthDiagnostic,
    pub warnings: Vec<String>,
    pub plugin_diagnostics: Vec<crate::PluginDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TraceCommandOutput {
    Status(StatusCommandOutput),
    Doctor(Box<DoctorCommandOutput>),
    Enable(SetupCommandOutput),
    Disable(SetupCommandOutput),
    Stop(StopCommandOutput),
    Import { summaries: Vec<ImportSummary> },
}

impl TraceCommandOutput {
    pub fn status(status: Option<StatusResult>) -> Self {
        Self::Status(status.into())
    }

    pub fn doctor(output: DoctorCommandOutput) -> Self {
        Self::Doctor(Box::new(output))
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

    pub fn import(summaries: Vec<ImportSummary>) -> Self {
        Self::Import { summaries }
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
            Self::Doctor(doctor) => {
                let route = doctor
                    .route
                    .as_ref()
                    .map(serde_json::to_string_pretty)
                    .transpose()?
                    .unwrap_or_else(|| "(unresolved)".into());
                let mut rendered = format!(
                    "Braintrust tracing doctor: {}\nEnabled: {}\nSettings: {}{}\nRoute source: {}\nRoute: {}\nAuth: {} ({})",
                    doctor.display_name,
                    doctor.enabled,
                    doctor.settings_path.display(),
                    if doctor.settings_present { "" } else { " (missing)" },
                    doctor.route_source,
                    route,
                    doctor.auth.status,
                    doctor.auth.source,
                );
                if let Some(profile) = &doctor.auth.profile {
                    rendered.push_str(&format!("\nProfile: {profile}"));
                }
                if let Some(org_name) = &doctor.auth.org_name {
                    rendered.push_str(&format!("\nOrganization: {org_name}"));
                }
                if let Some(error) = &doctor.auth.error {
                    rendered.push_str(&format!("\nAuth error: {error}"));
                }
                for warning in &doctor.warnings {
                    rendered.push_str(&format!("\nWarning: {warning}"));
                }
                for diagnostic in &doctor.plugin_diagnostics {
                    rendered.push_str(&format!(
                        "\nPlugin error: {} ({} occurrence{})\nFirst seen: {}\nLast seen: {}\n{}",
                        diagnostic.plugin_path.display(),
                        diagnostic.occurrences,
                        if diagnostic.occurrences == 1 { "" } else { "s" },
                        render_timestamp(diagnostic.first_seen_ms),
                        render_timestamp(diagnostic.last_seen_ms),
                        diagnostic.exception
                    ));
                }
                Ok(rendered)
            }
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
            Self::Import { summaries } => Ok(summaries
                .iter()
                .map(|summary| {
                    let destination = summary
                        .destination
                        .as_ref()
                        .map(render_destination)
                        .unwrap_or_else(|| "the configured destination".into());
                    let root = summary
                        .root_span_id
                        .as_deref()
                        .map(|id| format!(", root span {id}"))
                        .unwrap_or_default();
                    format!(
                        "Imported session {} to {}: {} spans{}.",
                        summary.session_id, destination, summary.span_count, root
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")),
        }
    }
}

fn render_timestamp(timestamp_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn render_destination(destination: &TraceDestination) -> String {
    match destination {
        TraceDestination::ProjectLogs {
            project_id,
            project_name,
        } => project_name
            .as_ref()
            .map(|name| format!("project {name}"))
            .or_else(|| project_id.as_ref().map(|id| format!("project {id}")))
            .unwrap_or_else(|| "project logs".into()),
        TraceDestination::Experiment { experiment_id } => {
            format!("experiment {experiment_id}")
        }
        TraceDestination::ParentSpan { components } => components
            .span_id
            .as_ref()
            .map(|id| format!("parent span {id}"))
            .unwrap_or_else(|| "a parent span".into()),
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
    fn import_summary_has_stable_human_and_json_output() {
        let output = TraceCommandOutput::import(vec![ImportSummary {
            session_id: "session-1".into(),
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("Agents".into()),
            }),
            root_span_id: Some("root-1".into()),
            span_count: 3,
            finalized: true,
        }]);
        assert_eq!(
            output.render(OutputFormat::Human).unwrap(),
            "Imported session session-1 to project Agents: 3 spans, root span root-1."
        );
        let value: serde_json::Value =
            serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
        assert_eq!(value["command"], "import");
        assert_eq!(value["summaries"][0]["session_id"], "session-1");
        assert_eq!(value["summaries"][0]["span_count"], 3);
        assert_eq!(value["summaries"][0]["finalized"], true);
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

    #[test]
    fn doctor_json_is_structured_and_contains_no_credentials() {
        let output = TraceCommandOutput::doctor(DoctorCommandOutput {
            source: "codex".into(),
            display_name: "Codex".into(),
            settings_path: PathBuf::from("/tmp/braintrust.json"),
            settings_present: true,
            enabled: true,
            route_source: "settings_file".into(),
            route: Some(SessionRoute::default()),
            auth: AuthDiagnostic {
                status: "ready".into(),
                source: "saved_profile".into(),
                kind: Some("oauth".into()),
                profile: Some("test-profile".into()),
                org_name: Some("test-org".into()),
                expires_at_ms: Some(123),
                error: None,
            },
            warnings: Vec::new(),
            plugin_diagnostics: vec![crate::PluginDiagnostic {
                source: "codex".into(),
                plugin_path: PathBuf::from("/tmp/redact.mjs"),
                plugin_digest: Some("abc".into()),
                exception: "Error: raw secret\n    at redact (redact.mjs:1)".into(),
                first_seen_ms: 1,
                last_seen_ms: 2,
                occurrences: 3,
            }],
        });
        let rendered = output.render(OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["command"], "doctor");
        assert_eq!(value["auth"]["source"], "saved_profile");
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("api_key"));
        assert_eq!(
            value["plugin_diagnostics"][0]["exception"],
            "Error: raw secret\n    at redact (redact.mjs:1)"
        );
    }
}
