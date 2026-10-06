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
pub struct UpdateCommandOutput {
    pub source: String,
    pub display_name: String,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStatus {
    Running,
    #[default]
    NotRunning,
    Unreachable,
}

/// What the running daemon, rather than the doctor's own process, sees. The
/// two can read different credential stores or environments.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DaemonDiagnostic {
    pub status: DaemonStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// The daemon's own resolution of the route's credentials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthDiagnostic>,
    /// Distinct latest errors the daemon recorded for this agent's sessions.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub session_errors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DaemonDiagnostic {
    /// ` (<version>)` when the daemon reported one.
    pub fn version_suffix(&self) -> String {
        self.version
            .as_deref()
            .map(|version| format!(" ({version})"))
            .unwrap_or_default()
    }

    /// The daemon answered and could not authenticate the route.
    pub fn auth_failed(&self) -> bool {
        self.auth
            .as_ref()
            .is_some_and(|auth| auth.status == "error")
    }
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
    pub daemon: DaemonDiagnostic,
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub recovery_incidents: Vec<crate::RecoveryIncident>,
    pub plugin_diagnostics: Vec<crate::PluginDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TraceCommandOutput {
    Status(StatusCommandOutput),
    Doctor(Box<DoctorCommandOutput>),
    Enable(SetupCommandOutput),
    Disable(SetupCommandOutput),
    Update(UpdateCommandOutput),
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

    pub fn update(source: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self::Update(UpdateCommandOutput {
            source: source.into(),
            display_name: display_name.into(),
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
                    "Braintrust tracing doctor: {}\nEnabled: {}\nSettings: {}{}\nRoute source: {}\nRoute: {}",
                    doctor.display_name,
                    doctor.enabled,
                    doctor.settings_path.display(),
                    if doctor.settings_present { "" } else { " (missing)" },
                    doctor.route_source,
                    route,
                );
                push_auth_lines(&mut rendered, "", &doctor.auth);
                let daemon = &doctor.daemon;
                let status = match daemon.status {
                    DaemonStatus::Running => "running",
                    DaemonStatus::NotRunning => "not running",
                    DaemonStatus::Unreachable => "unreachable",
                };
                rendered.push_str(&format!("\nDaemon: {status}{}", daemon.version_suffix()));
                if let Some(error) = &daemon.error {
                    rendered.push_str(&format!("\nDaemon error: {error}"));
                }
                if let Some(auth) = &daemon.auth {
                    push_auth_lines(&mut rendered, "Daemon ", auth);
                }
                for error in &daemon.session_errors {
                    rendered.push_str(&format!("\nDaemon session error: {error}"));
                }
                for warning in &doctor.warnings {
                    rendered.push_str(&format!("\nWarning: {warning}"));
                }
                for incident in &doctor.recovery_incidents {
                    let (source, session_id) = incident.scope.source_session();
                    rendered.push_str(&format!(
                        "\nRecovery: {:?}\nSource: {}\nSession: {}\nFirst blocked journal offset: {}\nOperation index: {}\nCause: {:?}\nDetail: {}\nAttempts: {}",
                        incident.state,
                        source,
                        session_id,
                        incident.first_unprocessed,
                        incident.operation_index,
                        incident.cause,
                        incident.local_error,
                        incident.attempts,
                    ));
                }
                for diagnostic in &doctor.plugin_diagnostics {
                    rendered.push_str(&format!(
                        "\nSpan plugin failure history: {}\nSource: {}\nSession: {}\nSpan: {}\nOperation: {}\nFailure journal offset: {}\nPending journal bytes at last report: {}\nFirst failure: {}\nLast failure: {}\nCause: {}",
                        diagnostic.plugin_path.display(),
                        diagnostic.source,
                        diagnostic.session_id.as_deref().unwrap_or("unknown"),
                        diagnostic.span_id.as_deref().unwrap_or("unknown"),
                        diagnostic.operation.as_deref().unwrap_or("unknown"),
                        diagnostic.journal_start.unwrap_or(0),
                        diagnostic.pending_bytes.unwrap_or(0),
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
            Self::Update(update) => Ok(format!(
                "The Braintrust tracing plugin was updated for {}.\nRestart the coding agent to load the update.",
                update.display_name,
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

/// Render one auth view; `prefix` distinguishes the daemon's from the local.
fn push_auth_lines(rendered: &mut String, prefix: &str, auth: &AuthDiagnostic) {
    let label = |name: &str| {
        if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}{}", name.to_lowercase())
        }
    };
    rendered.push_str(&format!(
        "\n{}: {} ({})",
        label("Auth"),
        auth.status,
        auth.source
    ));
    for (name, value) in [
        ("Profile", &auth.profile),
        ("Organization", &auth.org_name),
        ("Auth error", &auth.error),
    ] {
        if let Some(value) = value {
            rendered.push_str(&format!("\n{}: {value}", label(name)));
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
            daemon: DaemonDiagnostic {
                status: DaemonStatus::Running,
                version: Some("1.2.3".into()),
                auth: Some(AuthDiagnostic {
                    status: "error".into(),
                    source: "saved_profile".into(),
                    kind: None,
                    profile: None,
                    org_name: None,
                    expires_at_ms: None,
                    error: Some("saved profile ID 'p' no longer exists".into()),
                }),
                session_errors: vec!["could not resolve Braintrust auth for codex".into()],
                error: None,
            },
            warnings: Vec::new(),
            recovery_incidents: vec![crate::RecoveryIncident {
                scope: crate::WorkScope::SourceSession {
                    source: "pi".into(),
                    session_id: "pi-session".into(),
                },
                state: crate::WorkState::Paused,
                first_unprocessed: 42,
                operation_index: 0,
                cause: crate::FailureCause::InputShape {
                    event: "tool_execution_start".into(),
                    translator_revision: "revision-a".into(),
                },
                local_error: "missing toolCallId".into(),
                marker_span_id: None,
                first_seen_ms: 1,
                last_seen_ms: 2,
                attempts: 0,
                resolved_at_ms: None,
            }],
            plugin_diagnostics: vec![crate::PluginDiagnostic {
                source: "codex".into(),
                plugin_path: PathBuf::from("/tmp/redact.mjs"),
                plugin_digest: Some("abc".into()),
                exception: "Error: raw secret\n    at redact (redact.mjs:1)".into(),
                first_seen_ms: 1,
                last_seen_ms: 2,
                occurrences: 3,
                session_id: Some("session-1".into()),
                span_id: Some("span-1".into()),
                operation: Some("merge".into()),
                pending_bytes: Some(1024),
                ..Default::default()
            }],
        });
        let rendered = output.render(OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["command"], "doctor");
        assert_eq!(value["auth"]["source"], "saved_profile");
        assert_eq!(value["daemon"]["status"], "running");
        assert_eq!(value["daemon"]["auth"]["status"], "error");
        assert_eq!(
            value["recovery_incidents"][0]["scope"]["session_id"],
            "pi-session"
        );
        assert_eq!(
            value["recovery_incidents"][0]["cause"]["kind"],
            "input_shape"
        );
        assert_eq!(
            value["daemon"]["session_errors"][0],
            "could not resolve Braintrust auth for codex"
        );
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("api_key"));
        assert_eq!(
            value["plugin_diagnostics"][0]["exception"],
            "Error: raw secret\n    at redact (redact.mjs:1)"
        );
        let human = output.render(OutputFormat::Human).unwrap();
        assert!(human.contains("Span plugin failure history: /tmp/redact.mjs"));
        assert!(human.contains("Session: session-1"));
        assert!(human.contains("Span: span-1"));
        assert!(human.contains("Pending journal bytes at last report: 1024"));
    }
}
