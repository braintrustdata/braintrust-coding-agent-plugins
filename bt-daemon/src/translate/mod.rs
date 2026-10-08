//! Translators turn agent-native hook events into a sink-neutral span
//! representation ([`SpanOp`]). Each session gets its own stateful translator
//! instance (created by a [`TranslatorFactory`]); the state machine that pairs
//! start/stop events and builds the span tree lives inside that instance.
//!
//! Keeping the output ([`SpanRow`]) independent of the Braintrust SDK lets the
//! whole pipeline be exercised with a debug sink and makes translators unit-
//! testable without any network.

mod antigravity;
mod claude;
mod codex;
pub(crate) mod cursor;
mod debug;
mod decode;
mod git;
mod grok;
mod opencode;
mod pi;
pub(crate) use pi::request_config as pi_request_config;
mod recent;
mod tool;
mod turn_lineage;
pub use turn_lineage::TURN_SPAN_ID_KEY;

use debug::DebugTranslatorFactory;

use crate::agents::{registrar, Agent};
use crate::wire::{Envelope, SessionConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// The local account name supplied by the shell environment. Translators add
/// this to the agent session root, which is the only place that can identify
/// that root correctly when the session is attached to an existing trace.
pub(crate) fn local_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default()
}

/// Braintrust span kinds we emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanType {
    #[default]
    Task,
    Llm,
    Tool,
}

/// Immutable versions captured when a session root or turn is first created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginSnapshot {
    pub plugin_version: Option<String>,
    pub bt_version: String,
    pub source_version: Option<String>,
}

/// A resolved span row, ready for a sink to insert or merge. Field set is the
/// subset every current plugin uses; extend as translators need more.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpanRow {
    pub span_id: String,
    pub root_span_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_span_ids: Vec<String>,
    pub name: String,
    pub span_type: SpanType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Sink-only identity for a deterministic enrichment that may arrive
    /// after this span's terminal row was durably delivered. Replays of the
    /// same key are suppressed; distinct keys for the same span are retained.
    #[serde(skip)]
    #[doc(hidden)]
    pub late_merge_key: Option<String>,
    /// Span id of the user turn that owns this row, recorded by the translator
    /// from its native turn tracking. [`turn_lineage`] publishes it as
    /// `metadata.turn_span_id`.
    #[serde(skip)]
    #[doc(hidden)]
    pub turn_span_id: Option<String>,
    /// Labels for filtering in Braintrust (e.g. `compaction`, `permission-request`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Translator-owned marker for turn creation, not other task spans.
    #[serde(skip)]
    pub is_turn: bool,
    /// Delivery-owned creation snapshot, present on session roots and turns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<OriginSnapshot>,
}

/// A span operation. `Insert` creates (or replaces) a row; `Merge` updates an
/// existing row by id (maps to `_is_merge` at the sink). Re-emitting an
/// `Insert` after journal replay merges server-side thanks to deterministic
/// ids, so replay is idempotent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanOp {
    Insert(SpanRow),
    Merge(SpanRow),
}

/// Cross-cutting per-session context handed to the translator on each call.
/// The translator's own state lives in the translator instance; this carries
/// only what the dispatcher owns.
pub struct SessionCtx {
    pub session_id: String,
    /// Latest config seen for this session (auth, project, span-attach ids).
    pub config: Option<SessionConfig>,
}

/// Tags configured for every root span in the current agent session.
pub(crate) fn root_tags(ctx: &SessionCtx) -> Option<Vec<String>> {
    ctx.config
        .as_ref()
        .map(|config| config.tags.clone())
        .filter(|tags| !tags.is_empty())
}

/// A per-session state machine. One instance per session; `&mut self` so it
/// can hold open-span maps, transcript offsets, etc.
pub trait AgentTranslator: Send {
    /// Handle one event, returning span ops to emit.
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>>;

    /// Continue bounded work started by [`Self::handle`], [`Self::checkpoint`],
    /// or [`Self::finalize`].
    /// `Some` means the caller must emit this batch and call again; `None`
    /// means the translator is fully caught up.
    fn drain_pending(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        Ok(None)
    }

    /// Catch up externally buffered observations without ending the logical
    /// agent session. Delivery barriers call this before flushing the sink.
    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let _ = ctx;
        Ok(Vec::new())
    }

    /// Finish the logical agent session and defensively close work whose
    /// terminal native event never arrived. Called only when the actor itself
    /// is shutting down or being retired.
    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let _ = ctx;
        Ok(Vec::new())
    }

    /// Backward-compatible terminal flush used by transcript import callers.
    /// Live delivery barriers use [`Self::checkpoint`] instead.
    fn flush(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.finalize(ctx)
    }
}

/// Builds translator instances for an agent's hook source. Each agent type
/// implements this in its translator module and registers itself in
/// [`crate::agents`]; the source is the agent's [`Identity::source`].
///
/// [`Identity::source`]: crate::agents::Identity::source
pub(crate) trait TranslatorFactory: Agent {
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator>;
}

/// Maps canonical and supported alias source strings to translator factories.
pub struct Registry {
    factories: HashMap<String, Arc<dyn TranslatorFactory>>,
}

/// Map an agent name or alias to the source its hooks report. Unknown
/// sources pass through.
pub(crate) fn canonical_source_name(source: &str) -> &str {
    registrar()
        .translate
        .get(source)
        .map_or(source, |agent| agent.identity().source)
}

impl Registry {
    /// The production registry with every real agent translator registered.
    pub fn default_agents() -> Self {
        let mut r = Registry {
            factories: HashMap::new(),
        };
        r.insert(Arc::new(DebugTranslatorFactory));
        for agent in registrar().translate.shared() {
            r.insert(agent.clone());
        }
        r
    }

    fn insert(&mut self, factory: Arc<dyn TranslatorFactory>) {
        self.factories
            .insert(factory.identity().source.to_string(), factory);
    }

    /// Known sources, for the `initialize` capabilities list.
    pub fn sources(&self) -> Vec<String> {
        let mut v: Vec<String> = self.factories.keys().cloned().collect();
        v.sort();
        v
    }

    /// Resolve daemon source aliases to one stable identity.
    pub fn canonical_source<'a>(&'a self, source: &'a str) -> Option<&'a str> {
        // Only agent sources and the debug translator are accepted.
        let canonical = match registrar().translate.get(source) {
            Some(agent) => agent.identity().source,
            None if source == "debug" => "debug",
            None => return None,
        };
        self.factories.contains_key(canonical).then_some(canonical)
    }

    pub fn create_checked(
        &self,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Box<dyn AgentTranslator>> {
        let canonical = self
            .canonical_source(source)
            .ok_or_else(|| anyhow::anyhow!("unsupported coding-agent source {source:?}"))?;
        let namespace = crate::ids::session_namespace(canonical, session_id);
        self.create_checked_with_session_namespace(canonical, &namespace)
    }

    pub(crate) fn create_checked_with_session_namespace(
        &self,
        source: &str,
        session_namespace: &str,
    ) -> anyhow::Result<Box<dyn AgentTranslator>> {
        let canonical = self
            .canonical_source(source)
            .ok_or_else(|| anyhow::anyhow!("unsupported coding-agent source {source:?}"))?;
        let factory = self.factories.get(canonical);
        let factory = factory.expect("canonical source must have a factory");
        Ok(Box::new(turn_lineage::TurnLineage::new(
            factory.create(session_namespace),
        )))
    }

    /// Create a known translator. Production ingress uses
    /// [`Self::create_checked`] and returns an RPC error for unsupported
    /// sources; this convenience remains for focused tests.
    pub fn create(&self, source: &str, session_id: &str) -> Box<dyn AgentTranslator> {
        self.create_checked(source, session_id)
            .unwrap_or_else(|error| panic!("{error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_agent_source_has_a_translator() {
        assert_eq!(
            Registry::default_agents().sources(),
            [
                "antigravity",
                "claude-code",
                "codex",
                "cursor",
                "debug",
                "grok",
                "opencode",
                "pi"
            ]
        );
    }

    #[test]
    fn source_aliases_resolve_to_hook_sources() {
        let registry = Registry::default_agents();
        for (alias, source) in [
            ("claude", "claude-code"),
            ("claude-code", "claude-code"),
            ("open-code", "opencode"),
            ("opencode", "opencode"),
            ("codex", "codex"),
            ("debug", "debug"),
        ] {
            assert_eq!(registry.canonical_source(alias), Some(source));
        }
        assert_eq!(registry.canonical_source("agy"), None);
        assert_eq!(canonical_source_name("claude"), "claude-code");
        assert_eq!(canonical_source_name("unknown"), "unknown");
    }
}
