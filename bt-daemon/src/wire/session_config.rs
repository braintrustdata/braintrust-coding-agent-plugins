use super::{BackendAuth, FlushMode, TraceDestination};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

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
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub tags: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub span_plugins: Vec<PathBuf>,
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
        self.destination
            .as_ref()
            .and_then(TraceDestination::project_name)
    }
}
