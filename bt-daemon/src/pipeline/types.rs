//! Logical identities shared by every pipeline component.
//!
//! Journals, consumers, and routes are named by stable strings so they can key
//! SQLite rows and in-memory maps alike. Positions always travel with their
//! journal identity, so an event node can never be compared to a span node.

use crate::translate::SpanRow;
use crate::wire::{AuthSelection, FlushMode, RedactedEnvelope, SessionRoute, TraceDestination};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const SEP: char = '\u{1f}';

/// One canonical source plus nonempty session ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct PartitionId {
    pub(crate) source: String,
    pub(crate) session_id: String,
}

impl PartitionId {
    pub(crate) fn new(source: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            session_id: session_id.into(),
        }
    }

    pub(crate) fn key(&self) -> String {
        format!("{}{SEP}{}", self.source, self.session_id)
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.source, self.session_id)
    }
}

/// Which registration created a route. Every import/attach is its own route,
/// even when a live route already uses the same destination.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteOrigin {
    Live,
    Import(String),
}

/// Route identity: destination, credential selection, and origin. Any change
/// to these is a different route and therefore a different branch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RouteKey {
    pub(crate) auth: AuthSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) destination: Option<TraceDestination>,
    pub(crate) origin: RouteOrigin,
}

impl RouteKey {
    pub(crate) fn live(route: &SessionRoute) -> Self {
        Self::with_origin(route, RouteOrigin::Live)
    }

    pub(crate) fn with_origin(route: &SessionRoute, origin: RouteOrigin) -> Self {
        Self {
            auth: canonical_auth(&route.auth),
            destination: route.destination.clone(),
            origin,
        }
    }

    /// Stable identity string. A saved-profile route named both by display name
    /// and by its later-learned stable ID is the same route.
    pub(crate) fn id(&self) -> String {
        serde_json::to_string(self).expect("route keys serialize")
    }

    /// The external `(parent span id, root span id)` an attached destination
    /// supplies, if any.
    pub(crate) fn attachment(&self) -> (Option<String>, Option<String>) {
        match &self.destination {
            Some(TraceDestination::ParentSpan { components }) => {
                (components.span_id.clone(), components.root_span_id.clone())
            }
            _ => (None, None),
        }
    }
}

impl PartialEq for RouteKey {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for RouteKey {}

impl std::hash::Hash for RouteKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

fn canonical_auth(auth: &AuthSelection) -> AuthSelection {
    let mut auth = auth.clone();
    auth.source = auth.effective_source();
    if auth.profile.is_some() {
        auth.profile_id = None;
    }
    auth
}

/// Mutable route settings owned by the branch. Not journal data.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct RouteConfig {
    pub(crate) version: u64,
    #[serde(default)]
    pub(crate) flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) additional_metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) span_plugins: Vec<PathBuf>,
}

impl RouteConfig {
    pub(crate) fn from_route(route: &SessionRoute, version: u64) -> Self {
        Self {
            version,
            flush_mode: route.flush_mode,
            additional_metadata: route.additional_metadata.clone(),
            tags: route.tags.clone(),
            span_plugins: route.span_plugins.clone(),
        }
    }

    /// Whether the user-visible settings differ, ignoring the version.
    pub(crate) fn same_settings(&self, other: &Self) -> bool {
        self.flush_mode == other.flush_mode
            && self.additional_metadata == other.additional_metadata
            && self.tags == other.tags
            && self.span_plugins == other.span_plugins
    }

    /// Rebuild the nonsecret session route this branch delivers through.
    pub(crate) fn session_route(&self, key: &RouteKey) -> SessionRoute {
        SessionRoute {
            auth: key.auth.clone(),
            destination: key.destination.clone(),
            flush_mode: self.flush_mode,
            additional_metadata: self.additional_metadata.clone(),
            tags: self.tags.clone(),
            span_plugins: self.span_plugins.clone(),
        }
    }
}

/// Node identities start at 1 and are never reused within a journal.
pub(crate) type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum JournalKind {
    Events,
    Spans,
    Backfill(String),
}

/// A journal's identity: its partition and kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct JournalId {
    pub(crate) partition: PartitionId,
    pub(crate) kind: JournalKind,
}

impl JournalId {
    pub(crate) fn events(partition: &PartitionId) -> Self {
        Self {
            partition: partition.clone(),
            kind: JournalKind::Events,
        }
    }

    pub(crate) fn spans(partition: &PartitionId) -> Self {
        Self {
            partition: partition.clone(),
            kind: JournalKind::Spans,
        }
    }

    pub(crate) fn backfill(partition: &PartitionId, route: &RouteKey) -> Self {
        Self {
            partition: partition.clone(),
            kind: JournalKind::Backfill(route.id()),
        }
    }

    pub(crate) fn key(&self) -> String {
        match &self.kind {
            JournalKind::Events => format!("{}{SEP}events", self.partition.key()),
            JournalKind::Spans => format!("{}{SEP}spans", self.partition.key()),
            JournalKind::Backfill(route) => {
                format!("{}{SEP}backfill{SEP}{route}", self.partition.key())
            }
        }
    }
}

/// A named reader of one journal with one durable head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ConsumerId {
    Translation,
    Branch(String),
}

impl ConsumerId {
    pub(crate) fn branch(route: &RouteKey) -> Self {
        Self::Branch(route.id())
    }

    pub(crate) fn key(&self) -> String {
        match self {
            Self::Translation => "translation".into(),
            Self::Branch(route) => format!("branch{SEP}{route}"),
        }
    }
}

/// Why a translator is being finalized. Retirement and shutdown finalize a
/// translator that is discarded immediately afterwards, so replay skips them
/// exactly as a fresh translator never saw them. Terminal finalization (a
/// managed run's completion) remains part of the translator's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalizeKind {
    Terminal,
    Retire,
}

/// Ordered daemon controls in the event journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "control", rename_all = "snake_case")]
pub(crate) enum TranslationControl {
    Checkpoint { barrier: String },
    Finalize { barrier: String, kind: FinalizeKind },
}

/// One event journal entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TranslationInput {
    Native(Box<RedactedEnvelope>),
    Control(TranslationControl),
}

/// Capture versions of the event that produced an operation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct CaptureVersions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) plugin_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_version: Option<String>,
}

/// A persisted, route-neutral span operation.
#[derive(Debug, Clone)]
pub(crate) struct JournalSpanOp {
    pub(crate) merge: bool,
    pub(crate) row: SpanRow,
    pub(crate) capture: CaptureVersions,
}

/// One span (or backfill) journal entry.
#[derive(Debug, Clone)]
pub(crate) enum SpanEntry {
    Op(Box<JournalSpanOp>),
    Barrier { id: String, finalize: bool },
    BackfillComplete,
}

/// Fresh, collision-free barrier identity.
pub(crate) fn new_barrier_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::AuthSource;

    #[test]
    fn route_identity_ignores_learned_profile_ids() {
        let named = SessionRoute {
            auth: AuthSelection {
                profile: Some("work".into()),
                ..AuthSelection::default()
            },
            ..SessionRoute::default()
        };
        let mut learned = named.clone();
        learned.auth.profile_id = Some("abc".into());
        learned.auth.source = AuthSource::SavedProfile;
        assert_eq!(RouteKey::live(&named), RouteKey::live(&learned));
        assert_ne!(
            RouteKey::live(&named),
            RouteKey::with_origin(&named, RouteOrigin::Import("i".into()))
        );
    }
}
