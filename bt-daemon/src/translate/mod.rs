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

pub use claude::ClaudeTranslatorFactory;
pub use codex::CodexTranslatorFactory;
pub use debug::DebugTranslatorFactory;
pub use opencode::OpenCodeTranslatorFactory;
pub use pi::PiTranslatorFactory;

use crate::wire::{Envelope, SessionConfig};
use serde::{Deserialize, Serialize};
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

/// A per-session state machine. One instance per session; `&mut self` so it
/// can hold open-span maps, transcript offsets, etc.
pub trait AgentTranslator: Send {
    /// Handle one event, returning span ops to emit.
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>>;

    /// Emit any pending spans (e.g. close dangling turns) at flush/shutdown.
    fn flush(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let _ = ctx;
        Ok(Vec::new())
    }
}

/// Builds translator instances for a given `source`.
pub trait TranslatorFactory: Send + Sync {
    fn source(&self) -> &str;
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator>;
}

/// Maps a `source` string to its factory, with a fallback for unknown sources.
pub struct Registry {
    factories: HashMap<String, Box<dyn TranslatorFactory>>,
    fallback: Box<dyn TranslatorFactory>,
}

impl Registry {
    /// A registry whose only translator (and fallback) is the debug
    /// pass-through. This is the Phase 1 default.
    pub fn debug_only() -> Self {
        let mut r = Registry {
            factories: HashMap::new(),
            fallback: Box::new(DebugTranslatorFactory),
        };
        r.register(Box::new(DebugTranslatorFactory));
        r
    }

    /// The production registry: all real agent translators registered, debug
    /// as the fallback for unknown sources.
    pub fn default_agents() -> Self {
        let mut r = Registry::debug_only();
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

    /// Create a translator for `source`, falling back (with a warning) to the
    /// debug translator for an unknown source.
    pub fn create(&self, source: &str, session_id: &str) -> Box<dyn AgentTranslator> {
        match self.factories.get(source) {
            Some(f) => f.create(session_id),
            None => {
                tracing::warn!(source, "no translator registered; using debug fallback");
                self.fallback.create(session_id)
            }
        }
    }
}
