use super::{BackendAuth, CaptureContext, Envelope, SessionRoute};
use serde::{Deserialize, Serialize};

/// Journal form of [`Envelope`] with the token redacted. Deserializable so a
/// replay pass can read it back (and re-supply live credentials separately).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactedEnvelope {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    pub session_id: String,
    pub event: String,
    pub ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureContext>,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
}

impl RedactedEnvelope {
    /// Restore translator state without credentials. A replay route supplies
    /// non-secret configuration; live delivery must resolve credentials again.
    pub fn into_envelope(self) -> Envelope {
        let route = self.route;
        let config = route.as_ref().map(|route| {
            route.with_auth(BackendAuth {
                token: String::new(),
                api_url: None,
                app_url: None,
                org_name: route.auth.org_name.clone(),
                org_id: None,
            })
        });
        Envelope {
            source: self.source,
            source_version: self.source_version,
            plugin_version: self.plugin_version,
            session_id: self.session_id,
            event: self.event,
            ts_ms: self.ts_ms,
            managed_run_id: self.managed_run_id,
            capture: self.capture,
            payload: self.payload,
            route,
            config,
        }
    }
}
