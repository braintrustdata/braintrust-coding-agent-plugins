//! The `event.log` envelope and its session config, plus auth redaction for
//! the journal.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// One captured hook event, forwarded from a shim to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Which daemon-side translator interprets `payload` (e.g. `codex`,
    /// `claude-code`, `debug`).
    pub source: String,
    /// The agent version, for payload-drift handling. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    /// Per-session queue + state key.
    pub session_id: String,
    /// Agent-native hook event name (not normalized).
    pub event: String,
    /// Epoch milliseconds, stamped by the shim at capture time.
    pub ts_ms: i64,
    /// The raw agent-native hook payload; opaque except to the translator.
    pub payload: serde_json::Value,
    /// Shim-resolved credentials + trace settings. Present on every event from
    /// a stateless shim; the daemon keeps the latest per session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<SessionConfig>,
}

/// Trace settings and backend credentials resolved by the shim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    pub auth: BackendAuth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_span_id: Option<String>,
    #[serde(default)]
    pub flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Value>,
}

/// Backend credentials. `token` is an API key or an OAuth access token; the
/// daemon does not care which. Never persisted (see [`SessionConfig::redacted`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendAuth {
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    /// Optional org id. `bt` may or may not know it; the SDK's project
    /// registration works from `org_name` alone, so this is best-effort and
    /// only feeds the SDK's per-session credential/batch key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlushMode {
    /// Deliver in the background; flush on session end / idle. The default.
    #[default]
    FireAndForget,
    /// Additionally block on `session.flush` at each turn boundary.
    FlushOnTurnEnd,
}

/// A non-secret fingerprint of [`BackendAuth`], written to the journal in
/// place of the token so replay can detect a credential change without ever
/// persisting the secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthFingerprint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    /// First 12 hex chars of SHA-256(token). Enough to detect rotation, far
    /// too little to recover the token.
    pub token_sha256_prefix: String,
}

impl BackendAuth {
    pub fn fingerprint(&self) -> AuthFingerprint {
        let digest = Sha256::digest(self.token.as_bytes());
        let hex = digest.iter().fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(s, "{b:02x}");
            s
        });
        AuthFingerprint {
            api_url: self.api_url.clone(),
            app_url: self.app_url.clone(),
            org_name: self.org_name.clone(),
            token_sha256_prefix: hex[..12].to_string(),
        }
    }
}

impl Envelope {
    /// A copy of this envelope safe to write to the journal: the live token is
    /// replaced by an [`AuthFingerprint`]. The rest of `config` (project,
    /// span-attach ids, flush mode, metadata) is retained — none of it secret.
    pub fn redacted(&self) -> RedactedEnvelope {
        RedactedEnvelope {
            source: self.source.clone(),
            source_version: self.source_version.clone(),
            session_id: self.session_id.clone(),
            event: self.event.clone(),
            ts_ms: self.ts_ms,
            payload: self.payload.clone(),
            config: self.config.as_ref().map(|c| RedactedConfig {
                auth: c.auth.fingerprint(),
                project: c.project.clone(),
                parent_span_id: c.parent_span_id.clone(),
                root_span_id: c.root_span_id.clone(),
                flush_mode: c.flush_mode,
                additional_metadata: c.additional_metadata.clone(),
            }),
        }
    }
}

/// Journal form of [`Envelope`] with the token redacted. Deserializable so a
/// replay pass can read it back (and re-supply live credentials separately).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactedEnvelope {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    pub session_id: String,
    pub event: String,
    pub ts_ms: i64,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<RedactedConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactedConfig {
    pub auth: AuthFingerprint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_span_id: Option<String>,
    #[serde(default)]
    pub flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Envelope {
        Envelope {
            source: "codex".into(),
            source_version: Some("1.2.3".into()),
            session_id: "sess-1".into(),
            event: "PostToolUse".into(),
            ts_ms: 1_753_639_552_123,
            payload: serde_json::json!({ "session_id": "sess-1", "tool_name": "shell" }),
            config: Some(SessionConfig {
                auth: BackendAuth {
                    token: "sk-super-secret".into(),
                    api_url: Some("https://api.braintrust.dev".into()),
                    app_url: None,
                    org_name: Some("acme".into()),
                    org_id: None,
                },
                project: Some("codex".into()),
                parent_span_id: None,
                root_span_id: None,
                flush_mode: FlushMode::FireAndForget,
                additional_metadata: None,
            }),
        }
    }

    #[test]
    fn envelope_round_trips() {
        let e = sample();
        let s = serde_json::to_string(&e).unwrap();
        let back: Envelope = serde_json::from_str(&s).unwrap();
        assert_eq!(back.session_id, "sess-1");
        assert_eq!(back.config.unwrap().auth.token, "sk-super-secret");
    }

    #[test]
    fn redaction_drops_the_token_but_keeps_settings() {
        let e = sample();
        let r = e.redacted();
        let s = serde_json::to_string(&r).unwrap();
        assert!(
            !s.contains("sk-super-secret"),
            "token leaked into journal form: {s}"
        );
        let cfg = r.config.unwrap();
        assert_eq!(cfg.project.as_deref(), Some("codex"));
        assert_eq!(cfg.auth.org_name.as_deref(), Some("acme"));
        assert_eq!(cfg.auth.token_sha256_prefix.len(), 12);
    }

    #[test]
    fn fingerprint_changes_with_token() {
        let mut a = sample().config.unwrap().auth;
        let f1 = a.fingerprint();
        a.token = "sk-different".into();
        let f2 = a.fingerprint();
        assert_ne!(f1.token_sha256_prefix, f2.token_sha256_prefix);
    }

    #[test]
    fn flush_mode_defaults_to_fire_and_forget() {
        let json = serde_json::json!({
            "auth": { "token": "t" },
        });
        let cfg: SessionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.flush_mode, FlushMode::FireAndForget);
    }
}
