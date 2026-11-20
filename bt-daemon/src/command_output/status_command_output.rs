use crate::wire::StatusResult;
use serde::Serialize;

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

impl StatusCommandOutput {
    pub(super) fn render_human(&self) -> anyhow::Result<String> {
        if !self.running {
            return Ok("bt-daemon is not running".into());
        }
        Ok(serde_json::to_string_pretty(&StatusResult {
            daemon_version: self.daemon_version.clone().unwrap_or_default(),
            uptime_ms: self.uptime_ms.unwrap_or_default(),
            sessions: self.sessions.clone(),
        })?)
    }
}
