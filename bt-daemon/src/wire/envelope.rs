//! The `event.log` envelope and its session config, plus auth redaction for
//! the journal.

use braintrust_sdk_rust::SpanComponents;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// One captured hook event, forwarded from a shim to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Which daemon-side translator interprets `payload` (e.g. `codex`,
    /// `claude-code`, `debug`).
    pub source: String,
    /// The agent version, for payload-drift handling. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    /// Version of the Braintrust instrumentation package that captured the
    /// event. Distinct from the coding agent's `source_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    /// Per-session queue + state key.
    pub session_id: String,
    /// Agent-native hook event name (not normalized).
    pub event: String,
    /// Epoch milliseconds, stamped by the shim at capture time.
    pub ts_ms: i64,
    /// Invocation-local identifier supplied by `bt trace run`. The daemon uses
    /// it only to flush the sessions created by one managed child process tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_run_id: Option<String>,
    /// The raw agent-native hook payload; opaque except to the translator.
    pub payload: serde_json::Value,
    /// Environment visible to span plugins for this live event. It is sent over
    /// the private daemon transport but deliberately omitted from the journal.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub plugin_env: BTreeMap<String, String>,
    /// Non-secret, immutable routing intent for this session. New clients use
    /// this instead of resolving credentials themselves. The daemon host maps
    /// the selected profile and organization to live credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
    /// Daemon-internal resolved configuration. This field is never serialized;
    /// clients can only submit `route`.
    #[serde(skip)]
    pub config: Option<SessionConfig>,
}

/// Non-secret profile selection. A profile identifies the stored Braintrust
/// user credentials; an optional organization constrains profiles that can
/// address more than one organization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthSelection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
}

/// Immutable, journal-safe routing and trace settings for one agent session.
/// Credentials are deliberately absent and are resolved inside the daemon.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionRoute {
    #[serde(default)]
    pub auth: AuthSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<TraceDestination>,
    #[serde(default)]
    pub flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Value>,
    /// Ordered JavaScript span transforms. Paths are resolved by explicit
    /// setup, run, and import commands before entering persistent settings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub span_plugins: Vec<PathBuf>,
}

impl SessionRoute {
    pub fn with_auth(&self, auth: BackendAuth) -> SessionConfig {
        SessionConfig {
            auth,
            destination: self.destination.clone(),
            flush_mode: self.flush_mode,
            additional_metadata: self.additional_metadata.clone(),
            span_plugins: self.span_plugins.clone(),
        }
    }

    pub fn same_route(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }

    /// Raw journal entries can be replayed through a newer plugin chain as
    /// long as their Braintrust delivery route is otherwise unchanged.
    pub fn same_replay_route(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.span_plugins.clear();
        right.span_plugins.clear();
        left.same_route(&right)
    }
}

/// Trace settings and backend credentials resolved by the shim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    pub auth: BackendAuth,
    /// Typed trace destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<TraceDestination>,
    #[serde(default)]
    pub flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub span_plugins: Vec<PathBuf>,
}

/// Where a session's root span should be logged.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TraceDestination {
    /// Project logs selected by stable id, display name, or both.
    ProjectLogs {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_name: Option<String>,
    },
    /// An existing experiment.
    Experiment { experiment_id: String },
    /// A child of an exported Braintrust span.
    ParentSpan { components: SpanComponents },
}

impl SessionConfig {
    /// External parent/root ids used to shape translator rows. The sink keeps
    /// the full destination components for object routing and propagation.
    pub fn attached_span_ids(&self) -> (Option<String>, Option<String>) {
        if let Some(TraceDestination::ParentSpan { components }) = &self.destination {
            return (components.span_id.clone(), components.root_span_id.clone());
        }
        (None, None)
    }

    pub fn project_name(&self) -> Option<&str> {
        match &self.destination {
            Some(TraceDestination::ProjectLogs { project_name, .. }) => project_name.as_deref(),
            _ => None,
        }
    }
}

impl std::str::FromStr for TraceDestination {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, id) = value.split_once(':').ok_or_else(|| {
            "destination must be project_logs:<project-id> or experiment:<experiment-id>"
                .to_string()
        })?;
        if id.is_empty() {
            return Err("destination id must not be empty".to_string());
        }
        match kind {
            "project_logs" => Ok(Self::ProjectLogs {
                project_id: Some(id.to_string()),
                project_name: None,
            }),
            "experiment" => Ok(Self::Experiment {
                experiment_id: id.to_string(),
            }),
            _ => Err(format!(
                "unsupported destination {kind:?}; expected project_logs or experiment"
            )),
        }
    }
}

impl TraceDestination {
    pub fn project_name(&self) -> Option<&str> {
        match self {
            Self::ProjectLogs { project_name, .. } => project_name.as_deref(),
            _ => None,
        }
    }
}

/// Backend credentials. `token` is an API key or an OAuth access token; the
/// daemon does not care which. Never serialized or persisted.
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

impl Envelope {
    /// A copy safe to journal. Only the non-secret route is serializable; the
    /// daemon-internal resolved config is excluded by construction.
    pub fn redacted(&self) -> RedactedEnvelope {
        RedactedEnvelope {
            source: self.source.clone(),
            source_version: self.source_version.clone(),
            plugin_version: self.plugin_version.clone(),
            session_id: self.session_id.clone(),
            event: self.event.clone(),
            ts_ms: self.ts_ms,
            managed_run_id: self.managed_run_id.clone(),
            payload: self.payload.clone(),
            route: self.route.clone(),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    pub session_id: String,
    pub event: String,
    pub ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_run_id: Option<String>,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Envelope {
        Envelope {
            source: "codex".into(),
            source_version: Some("1.2.3".into()),
            plugin_version: Some("0.4.0".into()),
            session_id: "sess-1".into(),
            event: "PostToolUse".into(),
            ts_ms: 1_753_639_552_123,
            managed_run_id: Some("run-1".into()),
            payload: serde_json::json!({ "session_id": "sess-1", "tool_name": "shell" }),
            plugin_env: BTreeMap::from([("SECRET".into(), "not-journaled".into())]),
            route: Some(SessionRoute {
                auth: AuthSelection {
                    profile: Some("work".into()),
                    org_name: Some("acme".into()),
                },
                destination: Some(TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some("codex".into()),
                }),
                ..SessionRoute::default()
            }),
            config: Some(SessionConfig {
                auth: BackendAuth {
                    token: "sk-super-secret".into(),
                    api_url: Some("https://api.braintrust.dev".into()),
                    app_url: None,
                    org_name: Some("acme".into()),
                    org_id: None,
                },
                destination: None,
                flush_mode: FlushMode::FireAndForget,
                additional_metadata: None,
                span_plugins: Vec::new(),
            }),
        }
    }

    #[test]
    fn envelope_round_trips() {
        let e = sample();
        let s = serde_json::to_string(&e).unwrap();
        let back: Envelope = serde_json::from_str(&s).unwrap();
        assert_eq!(back.session_id, "sess-1");
        assert_eq!(back.managed_run_id.as_deref(), Some("run-1"));
        assert_eq!(back.route.unwrap().auth.profile.as_deref(), Some("work"));
        assert!(back.config.is_none());
        assert!(!s.contains("sk-super-secret"));
    }

    #[test]
    fn journal_form_contains_only_the_route() {
        let e = sample();
        let r = e.redacted();
        let s = serde_json::to_string(&r).unwrap();
        assert!(
            !s.contains("sk-super-secret"),
            "token leaked into journal form: {s}"
        );
        assert_eq!(r.route.unwrap().auth.profile.as_deref(), Some("work"));
        assert_eq!(r.managed_run_id.as_deref(), Some("run-1"));
    }

    #[test]
    fn route_is_journal_safe_and_builds_resolved_config() {
        let route = SessionRoute {
            auth: AuthSelection {
                profile: Some("work".into()),
                org_name: Some("acme".into()),
            },
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("agent-traces".into()),
            }),
            ..SessionRoute::default()
        };
        let config = route.with_auth(BackendAuth {
            token: "secret".into(),
            api_url: None,
            app_url: None,
            org_name: Some("acme".into()),
            org_id: None,
        });
        assert!(matches!(
            config.destination,
            Some(TraceDestination::ProjectLogs { project_name: Some(ref name), .. })
                if name == "agent-traces"
        ));
        assert_eq!(route.auth.profile.as_deref(), Some("work"));

        let mut envelope = sample();
        envelope.route = Some(route);
        envelope.config = Some(config);
        let journal = serde_json::to_string(&envelope.redacted()).unwrap();
        assert!(journal.contains("work"));
        assert!(!journal.contains("secret"));
    }

    #[test]
    fn flush_mode_defaults_to_fire_and_forget() {
        let json = serde_json::json!({
            "auth": { "token": "t" },
        });
        let cfg: SessionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.flush_mode, FlushMode::FireAndForget);
    }

    #[test]
    fn import_destination_references_are_typed() {
        let project: TraceDestination = "project_logs:proj-123".parse().unwrap();
        assert!(matches!(
            project,
            TraceDestination::ProjectLogs {
                project_id: Some(ref id),
                project_name: None
            } if id == "proj-123"
        ));
        let experiment: TraceDestination = "experiment:exp-456".parse().unwrap();
        assert!(matches!(
            experiment,
            TraceDestination::Experiment { ref experiment_id } if experiment_id == "exp-456"
        ));
        assert!("project:ambiguous".parse::<TraceDestination>().is_err());
    }
}
