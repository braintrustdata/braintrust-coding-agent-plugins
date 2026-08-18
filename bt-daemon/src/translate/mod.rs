//! Translators turn agent-native hook events into a sink-neutral span
//! representation ([`SpanOp`]). Each session gets its own stateful translator
//! instance (created by a [`TranslatorFactory`]); the state machine that pairs
//! start/stop events and builds the span tree lives inside that instance.
//!
//! Keeping the output ([`SpanRow`]) independent of the Braintrust SDK lets the
//! whole pipeline be exercised with a debug sink and makes translators unit-
//! testable without any network.

mod claude;
mod codex;
mod debug;
mod git;
mod opencode;
mod pi;
mod recent;

pub use claude::ClaudeTranslatorFactory;
pub use codex::CodexTranslatorFactory;
pub use debug::DebugTranslatorFactory;
pub use opencode::OpenCodeTranslatorFactory;
pub use pi::PiTranslatorFactory;

use crate::wire::{Envelope, SessionConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Braintrust span kinds we emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanType {
    #[default]
    Task,
    Llm,
    Tool,
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
    /// Labels for filtering in Braintrust (e.g. `compaction`, `permission-request`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
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

/// Apply user-supplied metadata while protecting daemon-owned identity fields.
/// All agent roots use this so routing internals and canonical identity cannot
/// drift between translators.
pub(crate) fn root_metadata(
    additional: Option<&Value>,
    source: &str,
    session_id: &str,
) -> Map<String, Value> {
    let mut metadata = additional
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    metadata.retain(|key, _| !key.starts_with("_bt_"));
    metadata.insert("session_id".into(), Value::String(session_id.to_string()));
    metadata.insert("source".into(), Value::String(source.to_string()));
    metadata
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

    /// Catch up any externally buffered observations without ending the
    /// logical agent session. Delivery barriers call this before flushing the
    /// sink, so a session can keep accepting later turns.
    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let _ = ctx;
        Ok(Vec::new())
    }

    /// Finish the logical agent session and defensively close any work whose
    /// terminal native event never arrived. Called only when the session actor
    /// itself is shutting down or being retired.
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

/// Builds translator instances for a given `source`.
pub trait TranslatorFactory: Send + Sync {
    fn source(&self) -> &str;
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator>;
}

/// Maps canonical and supported alias source strings to translator factories.
/// Production rejects unknown sources; the debug registry maps every known
/// agent identity to the pass-through translator for pipeline tests.
pub struct Registry {
    factories: HashMap<String, Box<dyn TranslatorFactory>>,
    debug_known_agents: bool,
}

impl Registry {
    /// A pass-through registry for debug sinks and pipeline tests. Known agent
    /// identities are accepted, while arbitrary unknown sources are rejected.
    pub fn debug_only() -> Self {
        let mut r = Registry {
            factories: HashMap::new(),
            debug_known_agents: true,
        };
        r.register(Box::new(DebugTranslatorFactory));
        r
    }

    /// The production registry with every real agent translator registered.
    pub fn default_agents() -> Self {
        let mut r = Registry {
            factories: HashMap::new(),
            debug_known_agents: false,
        };
        r.register(Box::new(DebugTranslatorFactory));
        let git = Arc::new(git::GitMetadataCache::default());
        r.register(Box::new(ClaudeTranslatorFactory::new(git.clone())));
        r.register(Box::new(CodexTranslatorFactory::new(git.clone())));
        r.register(Box::new(OpenCodeTranslatorFactory::new(git.clone())));
        r.register(Box::new(PiTranslatorFactory::new(git)));
        r
    }

    pub fn register(&mut self, factory: Box<dyn TranslatorFactory>) {
        self.factories.insert(factory.source().to_string(), factory);
    }

    /// Known sources, for the `initialize` capabilities list.
    pub fn sources(&self) -> Vec<String> {
        let mut v: Vec<String> = self.factories.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn canonical_source<'a>(&'a self, source: &'a str) -> Option<&'a str> {
        if self.factories.contains_key(source) {
            return Some(source);
        }
        let canonical = crate::AgentId::parse(source)?.canonical_source();
        (self.factories.contains_key(canonical) || self.debug_known_agents).then_some(canonical)
    }

    pub fn create_checked(
        &self,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Box<dyn AgentTranslator>> {
        let canonical = self
            .canonical_source(source)
            .ok_or_else(|| anyhow::anyhow!("unsupported coding-agent source {source:?}"))?;
        let factory = self.factories.get(canonical).or_else(|| {
            self.debug_known_agents
                .then(|| self.factories.get("debug"))
                .flatten()
        });
        let factory = factory.expect("canonical source must have a factory");
        let namespace = crate::ids::session_namespace(canonical, session_id);
        Ok(factory.create(&namespace))
    }

    /// Create a known translator. Production ingress uses
    /// [`Self::create_checked`] and returns an RPC error for unsupported
    /// sources; this convenience remains for focused tests.
    pub fn create(&self, source: &str, session_id: &str) -> Box<dyn AgentTranslator> {
        self.create_checked(source, session_id)
            .unwrap_or_else(|error| panic!("{error}"))
    }
}
