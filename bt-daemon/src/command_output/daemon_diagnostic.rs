use super::{AuthDiagnostic, DaemonStatus};
use serde::Serialize;

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
    /// Current holds, separate from persisted historical plugin errors.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub recovering_sessions: Vec<crate::wire::SessionStatus>,
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
        self.auth.as_ref().is_some_and(AuthDiagnostic::is_error)
    }

    pub(super) fn append_human(&self, rendered: &mut String) {
        rendered.push_str(&format!(
            "\nDaemon: {}{}",
            self.status,
            self.version_suffix()
        ));
        if let Some(error) = &self.error {
            rendered.push_str(&format!("\nDaemon error: {error}"));
        }
        if let Some(auth) = &self.auth {
            auth.append_human(rendered, "Daemon ");
        }
        for error in &self.session_errors {
            rendered.push_str(&format!("\nDaemon session error: {error}"));
        }
        for session in &self.recovering_sessions {
            if let Some(recovery) = &session.recovery {
                rendered.push_str(&format!(
                    "\nActive delivery hold: {} / {}\nStage: {}\nQueued span operations: {}\nRetry attempts: {}\nWaiting for: {}",
                    session.source, session.session_id, recovery.stage,
                    session.queued_span_ops.map_or_else(|| "unknown".into(), |count| count.to_string()), recovery.attempt, recovery.retry
                ));
            }
        }
    }
}
