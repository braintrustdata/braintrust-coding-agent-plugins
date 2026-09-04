//! Codex translator.
//!
//! Ported from the TS `trace-codex` event-processor. Codex hook events are only
//! *triggers*; the session transcript ("rollout" JSONL at `transcript_path`) is
//! the source of truth for LLM calls, token usage, and execution order. On each
//! hook event this reads the relevant transcript from a saved byte offset and
//! turns new records into spans.
//!
//! Hierarchy: root (session, task) → turn (task) → { llm, tool } spans.
//! Subagents get their own transcript *scope* whose turns hang under a
//! `subagent` root span that is a sibling of the `spawn_agent` tool span (both
//! under the spawning turn). Compaction turns are relabeled `compaction` with a
//! synthetic llm span showing the before/after context.
//!
//! Turn-terminal transcript *polling* (TS waits up to 10s for a late
//! `task_complete`) is replaced by re-reading on the next event and on
//! `flush()`. Native turn ids keep those late records correlated even when a
//! newer turn has already started.

use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet};
use super::tool::{error_text, tool_approval_metadata, ToolApproval};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};

const SPAWN_AGENT_TOOL: &str = "spawn_agent";
const MISSING_TOOL_OUTPUT_ERROR: &str = "Tool output missing before turn ended";
/// A hook can arrive after a daemon restart or against an existing rollout.
/// Keep a single translator batch small even when the unread suffix is large.
const CATCH_UP_BYTE_BUDGET: usize = 64 * 1024;

pub struct CodexTranslatorFactory {
    git: Arc<GitMetadataCache>,
}

impl CodexTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}

impl TranslatorFactory for CodexTranslatorFactory {
    fn source(&self) -> &str {
        "codex"
    }
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(CodexTranslator {
            session_id: session_id.to_string(),
            root_span_id: ids::span_id(session_id, "root"),
            external_parent_span_id: None,
            root_opened: false,
            root_ended: false,
            session_source: None,
            permission_mode: None,
            root_cwd: None,
            project: None,
            additional_metadata: Map::new(),
            main_path: None,
            // The main scope is created lazily once we learn its transcript path.
            scopes: HashMap::new(),
            spawn_turn_by_call_id: RecentMap::default(),
            spawn_turn_by_agent_id: RecentMap::default(),
            compaction_trigger_by_turn: RecentMap::default(),
            compaction_spans: RecentSet::default(),
            pending: None,
            git: self.git.clone(),
        })
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ScopeKind {
    Main,
    Subagent,
}

struct OpenTurn {
    turn_id: String,
    span_id: String,
    start_ms: i64,
    last_child_end_ms: Option<i64>,
    llm_seq: u32,
    explicit_skill_names: Vec<String>,
}

struct OpenLlm {
    span_id: String,
    turn_id: String,
    start_ms: i64,
    last_output_ms: i64,
    output: Vec<Value>,
    output_preset: bool,
}

struct Scope {
    path: String,
    kind: ScopeKind,
    offset: u64,
    /// Parent span id for this scope's turn spans (main root, or subagent root).
    turn_parent_span_id: String,
    /// Whether this scope's session/root span has been emitted.
    root_created: bool,
    model: Option<String>,
    current_cwd: Option<String>,
    open_turns: Vec<OpenTurn>,
    conversation_history: Vec<Value>,
    open_llm: Option<OpenLlm>,
    open_tools: HashMap<String, (String, String)>, // call_id -> (tool span_id, turn_id)
    last_turn_end_ms: Option<i64>,
    turn_seq: u32,
    // Subagent-only:
    agent_id: Option<String>,
    agent_type: Option<String>,
    spawning_turn_span_id: Option<String>,
    subagent_ended: bool,
}

enum DeferredHook {
    None,
    PostToolUse(Value),
    SubagentStop {
        path: Option<String>,
        ts: i64,
    },
    Stop {
        is_main: bool,
        payload: Value,
        ts: i64,
    },
    PostCompact {
        payload: Value,
        ts: i64,
    },
}

enum PendingWork {
    Hook {
        path: String,
        hook_ts: i64,
        through_ms: Option<i64>,
        through_bytes: Option<u64>,
        after: DeferredHook,
    },
    CatchUp {
        paths: Vec<String>,
        next_path: usize,
        finalize: bool,
    },
}

struct CodexTranslator {
    session_id: String,
    root_span_id: String,
    external_parent_span_id: Option<String>,
    root_opened: bool,
    root_ended: bool,
    session_source: Option<String>,
    permission_mode: Option<String>,
    root_cwd: Option<String>,
    project: Option<String>,
    additional_metadata: Map<String, Value>,
    main_path: Option<String>,
    scopes: HashMap<String, Scope>,
    spawn_turn_by_call_id: RecentMap<String, String>,
    spawn_turn_by_agent_id: RecentMap<String, String>,
    compaction_trigger_by_turn: RecentMap<String, String>,
    compaction_spans: RecentSet<String>,
    pending: Option<PendingWork>,
    git: Arc<GitMetadataCache>,
}

impl AgentTranslator for CodexTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        anyhow::ensure!(
            self.pending.is_none(),
            "Codex translator has pending catch-up work; drain it before handling another event"
        );
        let payload = &event.payload;
        let mut ops = Vec::new();

        if let Some(config) = &ctx.config {
            self.external_parent_span_id = config.attached_span_ids().0;
            self.project = config.project_name().map(ToOwned::to_owned);
            self.additional_metadata = config
                .additional_metadata
                .as_ref()
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
        }

        // --- hook-specific side effects (before catch-up) ---
        match event.event.as_str() {
            "SessionStart" => {
                self.session_source = str_field(payload, "source");
                self.permission_mode = str_field(payload, "permission_mode");
            }
            "SubagentStart" => self.handle_subagent_start(event),
            "PreCompact" | "PostCompact" => self.record_compaction_trigger(payload, &mut ops),
            _ => {}
        }

        // --- pick the scope and catch up its transcript ---
        let agent_id = str_field(payload, "agent_id");
        let path = effective_transcript_path(event);

        if let Some(path) = path {
            if agent_id.is_none() {
                self.main_path.get_or_insert(path.clone());
                self.ensure_main_scope(&path);
            }
            let import_through_ms = payload.get("_bt_import_through_ms").and_then(Value::as_i64);
            let through_bytes = payload
                .pointer("/_bt_transcript_mirror/through")
                .and_then(Value::as_u64);
            let after = self.deferred_hook(event, agent_id.is_none());
            if self.catch_up_chunk(
                &path,
                event.ts_ms,
                import_through_ms,
                through_bytes,
                &mut ops,
            ) {
                self.finish_deferred_hook(after, &mut ops);
            } else {
                self.pending = Some(PendingWork::Hook {
                    path,
                    hook_ts: event.ts_ms,
                    through_ms: import_through_ms,
                    through_bytes,
                    after,
                });
            }
        } else {
            self.finish_deferred_hook(self.deferred_hook(event, agent_id.is_none()), &mut ops);
        }

        Ok(ops)
    }

    fn drain_pending(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        let mut ops = Vec::new();
        match pending {
            PendingWork::Hook {
                path,
                hook_ts,
                through_ms,
                through_bytes,
                after,
            } => {
                if self.catch_up_chunk(&path, hook_ts, through_ms, through_bytes, &mut ops) {
                    self.finish_deferred_hook(after, &mut ops);
                } else {
                    self.pending = Some(PendingWork::Hook {
                        path,
                        hook_ts,
                        through_ms,
                        through_bytes,
                        after,
                    });
                }
            }
            PendingWork::CatchUp {
                paths,
                mut next_path,
                finalize,
            } => {
                while next_path < paths.len() {
                    let path = &paths[next_path];
                    if self.catch_up_chunk(path, 0, None, None, &mut ops) {
                        if finalize {
                            if let Some(mut scope) = self.scopes.remove(path) {
                                self.close_dangling(&mut scope, None, &mut ops);
                                self.scopes.insert(path.clone(), scope);
                            }
                        }
                        next_path += 1;
                    }
                    // Return after any completed scope or a bounded partial read.
                    // This keeps catch-up over many scopes bounded as well.
                    if !ops.is_empty() || next_path < paths.len() {
                        self.pending = (next_path < paths.len()).then_some(PendingWork::CatchUp {
                            paths,
                            next_path,
                            finalize,
                        });
                        break;
                    }
                }
            }
        }
        Ok(Some(ops))
    }

    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.start_catch_up(ctx, false)
    }

    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.start_catch_up(ctx, true)
    }
}

impl CodexTranslator {
    fn turn_parent_span_ids(&self, turn_id: &str) -> Vec<String> {
        vec![ids::span_id(&self.session_id, &format!("turn:{turn_id}"))]
    }

    fn scope_root_parent_span_ids(&self, scope: &Scope) -> Vec<String> {
        match scope.kind {
            ScopeKind::Main => self.external_parent_span_id.clone().into_iter().collect(),
            ScopeKind::Subagent => vec![scope
                .spawning_turn_span_id
                .clone()
                .unwrap_or_else(|| self.root_span_id.clone())],
        }
    }

    fn start_catch_up(&mut self, ctx: &SessionCtx, finalize: bool) -> anyhow::Result<Vec<SpanOp>> {
        anyhow::ensure!(
            self.pending.is_none(),
            "Codex translator has pending catch-up work; drain it before checkpointing"
        );
        // Re-read each scope to catch a late task_complete. Finalization also
        // closes dangling work whose terminal native event never arrived.
        let paths: Vec<String> = self.scopes.keys().cloned().collect();
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        self.pending = Some(PendingWork::CatchUp {
            paths,
            next_path: 0,
            finalize,
        });
        Ok(self.drain_pending(ctx)?.unwrap_or_default())
    }

    fn ensure_main_scope(&mut self, path: &str) {
        if self.scopes.contains_key(path) {
            return;
        }
        let turn_parent = self.root_span_id.clone();
        self.scopes.insert(
            path.to_string(),
            Scope::new(path, ScopeKind::Main, turn_parent),
        );
    }

    fn record_compaction_trigger(&mut self, payload: &Value, ops: &mut Vec<SpanOp>) {
        let Some(turn_id) = str_field(payload, "turn_id") else {
            return;
        };
        let trigger = str_field(payload, "trigger").unwrap_or_else(|| "manual".to_string());
        self.compaction_trigger_by_turn
            .insert(turn_id.clone(), trigger.clone());
        // Back-fill onto an already-built compaction span.
        if self.compaction_spans.remove(&turn_id) {
            self.compaction_trigger_by_turn.remove(&turn_id);
            let span_id = ids::span_id(&self.session_id, &format!("turn:{turn_id}"));
            let parent_span_ids = str_field(payload, "transcript_path")
                .and_then(|path| self.scopes.get(&path))
                .map(|scope| vec![scope.turn_parent_span_id.clone()])
                .unwrap_or_else(|| vec![self.root_span_id.clone()]);
            ops.push(SpanOp::Merge(SpanRow {
                span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids,
                metadata: Some(json!({ "compaction": { "trigger": trigger } })),
                ..Default::default()
            }));
        }
    }

    fn record_spawned_agent(&mut self, payload: &Value) {
        if str_field(payload, "tool_name").as_deref() != Some(SPAWN_AGENT_TOOL) {
            return;
        }
        let Some(call_id) = str_field(payload, "tool_use_id") else {
            return;
        };
        let agent_id = payload.get("tool_response").and_then(|r| match r {
            Value::String(s) => serde_json::from_str::<Value>(s)
                .ok()
                .and_then(|v| v.get("agent_id").and_then(Value::as_str).map(String::from)),
            Value::Object(_) => r.get("agent_id").and_then(Value::as_str).map(String::from),
            _ => None,
        });
        let Some(agent_id) = agent_id else { return };
        if let Some(turn_span) = self.spawn_turn_by_call_id.remove(&call_id) {
            self.spawn_turn_by_agent_id.insert(agent_id, turn_span);
        }
    }

    fn handle_subagent_start(&mut self, event: &Envelope) {
        let payload = &event.payload;
        let (Some(agent_id), Some(path)) = (
            str_field(payload, "agent_id"),
            effective_transcript_path(event),
        ) else {
            return;
        };
        if self.scopes.contains_key(&path) {
            return;
        }
        let parent = self
            .spawn_turn_by_agent_id
            .remove(&agent_id)
            .unwrap_or_else(|| self.root_span_id.clone());
        let subagent_root = ids::span_id(&self.session_id, &format!("subagent:{agent_id}"));
        let mut scope = Scope::new(&path, ScopeKind::Subagent, subagent_root);
        scope.agent_id = Some(agent_id);
        scope.agent_type = str_field(payload, "agent_type");
        scope.spawning_turn_span_id = Some(parent);
        self.scopes.insert(path, scope);
    }

    fn deferred_hook(&self, event: &Envelope, is_main_scope: bool) -> DeferredHook {
        match event.event.as_str() {
            // Catch up first: this same hook may be the first observation of
            // the spawn_agent transcript record that establishes call -> turn.
            "PostToolUse" => DeferredHook::PostToolUse(event.payload.clone()),
            "SubagentStop" => DeferredHook::SubagentStop {
                path: effective_transcript_path(event),
                ts: event.ts_ms,
            },
            // Codex writes task_complete slightly after the Stop hook in real
            // sessions. Do this only after the entire bounded catch-up finishes.
            "Stop" => DeferredHook::Stop {
                is_main: is_main_scope,
                payload: event.payload.clone(),
                ts: event.ts_ms,
            },
            "PostCompact" => DeferredHook::PostCompact {
                payload: event.payload.clone(),
                ts: event.ts_ms,
            },
            _ => DeferredHook::None,
        }
    }

    fn finish_deferred_hook(&mut self, after: DeferredHook, ops: &mut Vec<SpanOp>) {
        match after {
            DeferredHook::None => {}
            DeferredHook::PostToolUse(payload) => self.record_spawned_agent(&payload),
            DeferredHook::SubagentStop { path, ts } => {
                if let Some(path) = path {
                    self.close_subagent(&path, ts, ops);
                }
            }
            DeferredHook::Stop {
                is_main: true,
                payload,
                ts,
            } => {
                self.close_main_turn(&payload, ts, ops);
                self.end_main_root(ts, ops);
            }
            DeferredHook::Stop { is_main: false, .. } => {}
            DeferredHook::PostCompact { payload, ts } => {
                self.close_compaction_turn(&payload, ts, ops)
            }
        }
    }

    /// Read a bounded batch of transcript lines for `path` and process them
    /// against its scope. `true` means the requested catch-up is complete.
    fn catch_up_chunk(
        &mut self,
        path: &str,
        hook_ts: i64,
        through_ms: Option<i64>,
        through_bytes: Option<u64>,
        ops: &mut Vec<SpanOp>,
    ) -> bool {
        let Some(mut scope) = self.scopes.remove(path) else {
            return true;
        };
        let read = read_new_lines(
            &scope.path,
            &mut scope.offset,
            through_ms,
            through_bytes,
            CATCH_UP_BYTE_BUDGET,
        );
        for line in read.lines {
            if let Ok(rec) = serde_json::from_str::<Value>(&line) {
                self.process_record(&mut scope, &rec, hook_ts, ops);
            }
        }
        self.scopes.insert(path.to_string(), scope);
        read.complete
    }

    fn process_record(
        &mut self,
        scope: &mut Scope,
        rec: &Value,
        hook_ts: i64,
        ops: &mut Vec<SpanOp>,
    ) {
        let op_start = ops.len();
        let ts = parse_ts(rec).unwrap_or(hook_ts);
        let ty = rec.get("type").and_then(Value::as_str).unwrap_or("");
        let payload = rec.get("payload").cloned().unwrap_or(Value::Null);
        if matches!(ty, "session_meta" | "turn_context") {
            if let Some(cwd) = str_field(&payload, "cwd") {
                scope.current_cwd = Some(cwd);
            }
        }

        match ty {
            "session_meta" => self.open_root(scope, &payload, ts, ops),
            "turn_context" => {
                if let Some(m) = str_field(&payload, "model") {
                    let model_turn_id = str_field(&payload, "turn_id");
                    scope.model = Some(m.clone());
                    if scope.root_created {
                        let input = if scope.kind == ScopeKind::Main {
                            json!({
                                "model": m,
                                "cwd": self.root_cwd,
                                "source": self.session_source,
                            })
                        } else {
                            json!({ "model": m })
                        };
                        ops.push(SpanOp::Merge(SpanRow {
                            span_id: scope.turn_parent_span_id.clone(),
                            root_span_id: self.root_span_id.clone(),
                            parent_span_ids: self.scope_root_parent_span_ids(scope),
                            input: Some(input),
                            metadata: Some(json!({ "model": m })),
                            ..Default::default()
                        }));
                    }
                    if let Some(turn) = model_turn_id.as_deref().and_then(|turn_id| {
                        scope.open_turns.iter().find(|turn| turn.turn_id == turn_id)
                    }) {
                        ops.push(SpanOp::Merge(SpanRow {
                            span_id: turn.span_id.clone(),
                            root_span_id: self.root_span_id.clone(),
                            parent_span_ids: vec![scope.turn_parent_span_id.clone()],
                            metadata: Some(json!({ "model": m })),
                            ..Default::default()
                        }));
                    }
                }
            }
            "event_msg" => {
                let sub = payload.get("type").and_then(Value::as_str).unwrap_or("");
                match sub {
                    "task_started" => self.open_turn(scope, &payload, ts, ops),
                    "user_message" => self.set_turn_input(scope, &payload, ops),
                    "token_count" => self.close_llm_with_tokens(scope, &payload, ts, ops),
                    "task_complete" => self.close_turn(scope, &payload, ts, ops),
                    _ => {}
                }
            }
            "response_item" => {
                let sub = payload.get("type").and_then(Value::as_str).unwrap_or("");
                match sub {
                    "message" => self.on_message(scope, &payload, ts, ops),
                    "reasoning" => self.on_reasoning(scope, &payload, ts, ops),
                    "function_call" | "custom_tool_call" | "tool_search_call" => {
                        self.on_tool_call(scope, &payload, ts, ops)
                    }
                    "function_call_output" | "custom_tool_call_output" | "tool_search_output" => {
                        self.on_tool_output(scope, &payload, ts, ops)
                    }
                    _ => {}
                }
            }
            "compacted" => self.on_compacted(scope, rec, &payload, ts, ops),
            _ => {}
        }
        let cwd = scope.current_cwd.as_deref().or(self.root_cwd.as_deref());
        self.git.enrich_rows(cwd, &mut ops[op_start..]);
    }

    fn open_root(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        match scope.kind {
            ScopeKind::Main => {
                if self.root_opened {
                    return;
                }
                self.root_opened = true;
                let name = match str_field(payload, "cwd") {
                    Some(cwd) => format!("codex: {}", basename(&cwd)),
                    None => "codex session".to_string(),
                };
                let cwd = str_field(payload, "cwd");
                self.root_cwd = cwd.clone();
                let mut md = self.additional_metadata.clone();
                for k in ["id", "cwd", "cli_version"] {
                    if let Some(v) = str_field(payload, k) {
                        md.insert(
                            if k == "id" {
                                "session_id".into()
                            } else {
                                k.to_string()
                            },
                            json!(v),
                        );
                    }
                }
                // `source` identifies the agent consistently across all coding-agent
                // integrations. Codex's SessionStart source (for example, startup or
                // resume) describes how this session began, so keep it separately.
                md.insert("source".into(), json!("codex"));
                if let Some(session_source) = &self.session_source {
                    md.insert("session_source".into(), json!(session_source));
                }
                if let Some(pm) = &self.permission_mode {
                    md.insert("permission_mode".into(), json!(pm));
                }
                if let Some(tp) = &self.main_path {
                    md.insert("transcript_path".into(), json!(tp));
                }
                if let Some(m) = &scope.model {
                    md.insert("model".into(), json!(m));
                }
                if let Some(project) = &self.project {
                    md.insert("project".into(), json!(project));
                }
                md.insert("username".into(), json!(username()));
                scope.root_created = true;
                ops.push(SpanOp::Insert(SpanRow {
                    span_id: self.root_span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: self.scope_root_parent_span_ids(scope),
                    name,
                    span_type: SpanType::Task,
                    start_ms: Some(ts),
                    input: Some(json!({
                        "model": scope.model,
                        "cwd": cwd,
                        "source": self.session_source,
                    })),
                    metadata: Some(Value::Object(md)),
                    ..Default::default()
                }));
            }
            ScopeKind::Subagent => {
                if scope.root_created {
                    return;
                }
                scope.root_created = true;
                let agent_id = scope.agent_id.clone().unwrap_or_default();
                let parent = scope
                    .spawning_turn_span_id
                    .clone()
                    .unwrap_or_else(|| self.root_span_id.clone());
                ops.push(SpanOp::Insert(SpanRow {
                    span_id: scope.turn_parent_span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![parent],
                    name: format!("subagent: {agent_id}"),
                    span_type: SpanType::Task,
                    start_ms: Some(ts),
                    metadata: Some(json!({
                        "agent_id": agent_id,
                        "agent_type": scope.agent_type,
                        "transcript_path": scope.path,
                    })),
                    ..Default::default()
                }));
            }
        }
    }

    fn open_turn(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let turn_id = str_field(payload, "turn_id").unwrap_or_else(|| {
            scope.turn_seq += 1;
            format!("turn-{}", scope.turn_seq)
        });
        if scope.open_turns.iter().any(|turn| turn.turn_id == turn_id) {
            return;
        }
        let span_id = ids::span_id(&self.session_id, &format!("turn:{turn_id}"));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![scope.turn_parent_span_id.clone()],
            name: format!("turn: {turn_id}"),
            span_type: SpanType::Task,
            start_ms: Some(ts),
            metadata: Some(json!({ "turn_id": turn_id, "model": scope.model })),
            ..Default::default()
        }));
        scope.open_turns.push(OpenTurn {
            turn_id,
            span_id,
            start_ms: ts,
            last_child_end_ms: None,
            llm_seq: 0,
            explicit_skill_names: Vec::new(),
        });
    }

    fn set_turn_input(&mut self, scope: &mut Scope, payload: &Value, ops: &mut Vec<SpanOp>) {
        let text = str_field(payload, "message")
            .or_else(|| str_field(payload, "text"))
            .or_else(|| str_field(payload, "prompt"));
        let Some(text) = text else { return };
        // Explicit skill mentions in the prompt (e.g. "$skill", "/skills name").
        let names = explicit_skill_names(&text);
        let turn_parent_span_id = scope.turn_parent_span_id.clone();
        if let Some(turn) = scope.open_turns.last_mut() {
            for n in names {
                if !turn.explicit_skill_names.contains(&n) {
                    turn.explicit_skill_names.push(n);
                }
            }
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![turn_parent_span_id],
                input: Some(json!(text)),
                metadata: explicit_skill_metadata(&turn.explicit_skill_names),
                ..Default::default()
            }));
        }
    }

    fn ensure_llm(
        &mut self,
        scope: &mut Scope,
        turn_id: Option<&str>,
        _ts: i64,
        ops: &mut Vec<SpanOp>,
    ) {
        if scope.open_llm.is_some() {
            return;
        }
        let index = turn_id
            .and_then(|id| scope.open_turns.iter().position(|turn| turn.turn_id == id))
            .or_else(|| scope.open_turns.len().checked_sub(1));
        let Some(index) = index else {
            return;
        };
        let input = Value::Array(scope.conversation_history.clone());
        let turn = &mut scope.open_turns[index];
        let seq = turn.llm_seq;
        turn.llm_seq += 1;
        // Start where the model's work began — end of the turn's last child, or
        // the turn's start for the first child — not the record time (which is
        // when output landed, yielding a near-instant span).
        let start = turn.last_child_end_ms.unwrap_or(turn.start_ms);
        let span_id = ids::span_id(&self.session_id, &format!("llm:{}:{}", turn.turn_id, seq));
        let name = scope.model.clone().unwrap_or_else(|| "llm".to_string());
        let turn_span = turn.span_id.clone();
        let turn_id = turn.turn_id.clone();
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![turn_span],
            name,
            span_type: SpanType::Llm,
            start_ms: Some(start),
            input: Some(input),
            metadata: Some(json!({ "model": scope.model, "turn_id": turn_id })),
            ..Default::default()
        }));
        scope.open_llm = Some(OpenLlm {
            span_id,
            turn_id,
            start_ms: start,
            last_output_ms: start,
            output: Vec::new(),
            output_preset: false,
        });
    }

    fn on_message(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let role = str_field(payload, "role").unwrap_or_else(|| "user".to_string());
        let text = message_text(payload);
        if text.is_empty() {
            return;
        }
        let msg = json!({ "role": role, "content": text });
        if role == "assistant" {
            self.ensure_llm(scope, None, ts, ops);
            if let Some(llm) = &mut scope.open_llm {
                llm.output.push(msg.clone());
                llm.last_output_ms = llm.last_output_ms.max(ts);
            }
        } else if role == "user" {
            let names = explicit_skill_names(&text);
            let turn_parent_span_id = scope.turn_parent_span_id.clone();
            if let Some(turn) = scope.open_turns.last_mut() {
                for name in names {
                    if !turn.explicit_skill_names.contains(&name) {
                        turn.explicit_skill_names.push(name);
                    }
                }
                if let Some(metadata) = explicit_skill_metadata(&turn.explicit_skill_names) {
                    ops.push(SpanOp::Merge(SpanRow {
                        span_id: turn.span_id.clone(),
                        root_span_id: self.root_span_id.clone(),
                        parent_span_ids: vec![turn_parent_span_id],
                        metadata: Some(metadata),
                        ..Default::default()
                    }));
                }
            }
        }
        scope.conversation_history.push(msg);
    }

    fn on_reasoning(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        self.ensure_llm(scope, None, ts, ops);
        if let Some(llm) = &mut scope.open_llm {
            llm.last_output_ms = llm.last_output_ms.max(ts);
        }
        let summary: Vec<Value> = payload
            .get("summary")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|s| {
                        str_field(s, "text")
                            .filter(|text| !text.is_empty())
                            .map(|text| json!({ "type": "summary_text", "text": text }))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if summary.is_empty() {
            return; // encrypted reasoning: only opens/advances the span
        }
        let item = json!({ "type": "reasoning", "summary": summary });
        if let Some(llm) = &mut scope.open_llm {
            llm.output.push(item.clone());
        }
        scope.conversation_history.push(item);
    }

    fn on_tool_call(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let call_id = str_field(payload, "call_id");
        let tool_name = str_field(payload, "name").unwrap_or_else(|| {
            payload
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .trim_end_matches("_call")
                .to_string()
        });
        let input = payload
            .get("arguments")
            .cloned()
            .or_else(|| payload.get("input").cloned());
        let args_string = input
            .as_ref()
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                serde_json::to_string(input.as_ref().unwrap_or(&Value::Null)).unwrap()
            });
        let tool_call_message = json!({
            "role": "assistant",
            "content": Value::Null,
            "tool_calls": [{
                "id": call_id.clone().unwrap_or_default(),
                "type": "function",
                "function": { "name": tool_name, "arguments": args_string },
            }],
        });
        scope.conversation_history.push(tool_call_message.clone());

        let Some(call_id) = call_id else { return };
        let turn_id = payload
            .get("metadata")
            .and_then(|metadata| str_field(metadata, "turn_id"))
            .or_else(|| scope.open_turns.last().map(|turn| turn.turn_id.clone()));
        let Some(turn_id) = turn_id else { return };
        let Some(turn_index) = scope
            .open_turns
            .iter()
            .position(|turn| turn.turn_id == turn_id)
        else {
            return;
        };
        if scope.open_tools.contains_key(&call_id) {
            return;
        }
        let turn_span = scope.open_turns[turn_index].span_id.clone();
        let explicit_skills = scope.open_turns[turn_index].explicit_skill_names.clone();

        self.ensure_llm(scope, Some(&turn_id), ts, ops);
        if let Some(llm) = &mut scope.open_llm {
            llm.output.push(tool_call_message);
            llm.last_output_ms = llm.last_output_ms.max(ts);
        }

        let span_id = ids::span_id(&self.session_id, &format!("tool:{call_id}"));
        // spawn_agent: remember which turn ran it, so a later SubagentStart can
        // nest the subagent root under this turn in either the main or a nested
        // agent scope.
        if tool_name == SPAWN_AGENT_TOOL {
            self.spawn_turn_by_call_id
                .insert(call_id.clone(), turn_span.clone());
        }

        // Skill / permission classification.
        let skill = detect_skill(&tool_name, input.as_ref());
        let permission = permission_info(input.as_ref());
        let mut name = tool_name.clone();
        let mut tags: Vec<String> = Vec::new();
        let mut metadata = Map::new();
        metadata.insert("tool_name".into(), json!(tool_name));
        metadata.insert("call_id".into(), json!(call_id));
        metadata.insert("turn_id".into(), json!(turn_id));
        if let Some(skill) = &skill {
            if let Some(skill_name) = &skill.name {
                name = format!("skill: {skill_name}");
                metadata.insert("skill_name".into(), json!(skill_name));
                if explicit_skills.contains(skill_name) {
                    metadata.insert("skill_load_trigger".into(), json!("explicit"));
                }
            }
            if let Some(skill_path) = &skill.path {
                metadata.insert("skill_path".into(), json!(skill_path));
            }
            metadata.insert("tool_kind".into(), json!("skill"));
        }
        if let Some(permission) = permission {
            metadata.insert("permission".into(), permission);
            tags.push("permission-request".to_string());
        }

        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![turn_span],
            name,
            span_type: SpanType::Tool,
            start_ms: Some(ts),
            input,
            metadata: Some(Value::Object(metadata)),
            tags: if tags.is_empty() { None } else { Some(tags) },
            ..Default::default()
        }));
        scope.open_tools.insert(call_id, (span_id, turn_id));
    }

    fn on_tool_output(
        &mut self,
        scope: &mut Scope,
        payload: &Value,
        ts: i64,
        ops: &mut Vec<SpanOp>,
    ) {
        let Some(call_id) = str_field(payload, "call_id") else {
            push_tool_result(scope, None, payload);
            return;
        };
        push_tool_result(scope, Some(&call_id), payload);
        let Some((span_id, turn_id)) = scope.open_tools.remove(&call_id) else {
            return;
        };
        if let Some(turn) = scope
            .open_turns
            .iter_mut()
            .find(|turn| turn.turn_id == turn_id)
        {
            turn.last_child_end_ms = Some(turn.last_child_end_ms.map_or(ts, |p| p.max(ts)));
        }
        let output = payload
            .get("output")
            .or_else(|| payload.get("result"))
            .cloned();
        let error = output.as_ref().and_then(classify_tool_output);
        ops.push(SpanOp::Merge(SpanRow {
            span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: self.turn_parent_span_ids(&turn_id),
            end_ms: Some(ts),
            output,
            metadata: Some(tool_approval_metadata(Some(ToolApproval::Approved))),
            error,
            ..Default::default()
        }));
    }

    fn close_llm_with_tokens(
        &mut self,
        scope: &mut Scope,
        payload: &Value,
        ts: i64,
        ops: &mut Vec<SpanOp>,
    ) {
        let Some(llm) = scope.open_llm.take() else {
            return;
        };
        let usage = payload.get("info").and_then(|i| i.get("last_token_usage"));
        let metrics = usage.map(token_metrics).filter(|m| !m.is_empty());
        let usage_metadata = if metrics.is_none() {
            Some(json!({
                "usage_unavailable_reason": if usage
                    .and_then(Value::as_object)
                    .is_none_or(Map::is_empty)
                {
                    "codex_token_count_missing_usage"
                } else {
                    "codex_token_count_unrecognized_usage"
                }
            }))
        } else {
            None
        };
        let end = llm.last_output_ms.max(llm.start_ms);
        if let Some(turn) = scope
            .open_turns
            .iter_mut()
            .find(|turn| turn.turn_id == llm.turn_id)
        {
            turn.last_child_end_ms = Some(turn.last_child_end_ms.map_or(end, |p| p.max(end)));
        }
        let output = if llm.output_preset {
            None
        } else {
            Some(llm_output(&llm.output))
        };
        ops.push(SpanOp::Merge(SpanRow {
            span_id: llm.span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: self.turn_parent_span_ids(&llm.turn_id),
            end_ms: Some(end),
            output,
            metadata: usage_metadata,
            metrics: metrics.map(Value::Object),
            ..Default::default()
        }));
        let _ = ts;
    }

    fn close_turn(&mut self, scope: &mut Scope, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let requested_turn_id = str_field(payload, "turn_id");
        let turn_index = requested_turn_id
            .as_deref()
            .and_then(|id| scope.open_turns.iter().position(|turn| turn.turn_id == id))
            .or_else(|| {
                if requested_turn_id.is_none() {
                    scope.open_turns.len().checked_sub(1)
                } else {
                    None
                }
            });
        let Some(turn_index) = turn_index else {
            return;
        };
        let turn_id = scope.open_turns[turn_index].turn_id.clone();
        let turn_span_id = scope.open_turns[turn_index].span_id.clone();

        if scope
            .open_llm
            .as_ref()
            .is_some_and(|llm| llm.turn_id == turn_id)
        {
            let llm = scope.open_llm.take().expect("checked above");
            let output = if llm.output_preset {
                None
            } else {
                Some(llm_output(&llm.output))
            };
            ops.push(SpanOp::Merge(SpanRow {
                span_id: llm.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![turn_span_id],
                end_ms: Some(llm.last_output_ms),
                output,
                metadata: Some(json!({
                    "usage_unavailable_reason": "codex_transcript_missing_token_count"
                })),
                ..Default::default()
            }));
        }
        self.close_tools_for_turn(scope, &turn_id, Some(ts), ops);
        let turn = scope.open_turns.remove(turn_index);
        scope.last_turn_end_ms = Some(ts);
        let output = str_field(payload, "last_agent_message")
            .or_else(|| str_field(payload, "last_assistant_message"))
            .map(|s| json!(s));
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn.span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![scope.turn_parent_span_id.clone()],
            end_ms: Some(ts),
            output,
            ..Default::default()
        }));
    }

    fn close_tools_for_turn(
        &mut self,
        scope: &mut Scope,
        turn_id: &str,
        end_ms: Option<i64>,
        ops: &mut Vec<SpanOp>,
    ) {
        let parent_span_ids = self.turn_parent_span_ids(turn_id);
        let call_ids: Vec<String> = scope
            .open_tools
            .iter()
            .filter(|(_, (_, owner))| owner == turn_id)
            .map(|(call_id, _)| call_id.clone())
            .collect();
        for call_id in call_ids {
            if let Some((span_id, _)) = scope.open_tools.remove(&call_id) {
                ops.push(SpanOp::Merge(SpanRow {
                    span_id,
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: parent_span_ids.clone(),
                    end_ms,
                    metadata: Some(tool_approval_metadata(Some(ToolApproval::Approved))),
                    error: Some(MISSING_TOOL_OUTPUT_ERROR.to_string()),
                    ..Default::default()
                }));
            }
        }
    }

    fn close_main_turn(&mut self, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let Some(path) = self.main_path.clone() else {
            return;
        };
        let Some(mut scope) = self.scopes.remove(&path) else {
            return;
        };
        self.close_turn(&mut scope, payload, ts, ops);
        self.scopes.insert(path, scope);
    }

    fn on_compacted(
        &mut self,
        scope: &mut Scope,
        _rec: &Value,
        payload: &Value,
        ts: i64,
        ops: &mut Vec<SpanOp>,
    ) {
        let Some(turn) = scope.open_turns.last() else {
            return;
        };
        let turn_id = turn.turn_id.clone();
        let turn_span = turn.span_id.clone();
        let turn_start = turn.start_ms;
        let turn_last_child = turn.last_child_end_ms;
        let replacement = payload
            .get("replacement_history")
            .and_then(Value::as_array)
            .cloned();
        let trigger = self.compaction_trigger_by_turn.remove(&turn_id);
        if trigger.is_none() {
            self.compaction_spans.insert(turn_id.clone());
        }

        // Relabel the turn as a compaction span.
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn_span.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![scope.turn_parent_span_id.clone()],
            name: "compaction".to_string(),
            span_type: SpanType::Task,
            metadata: Some(json!({ "compaction": {
                "trigger": trigger,
                "replaced_message_count": replacement.as_ref().map(|r| r.len()),
                "window_id": payload.get("window_id"),
            }})),
            tags: Some(vec!["compaction".to_string()]),
            ..Default::default()
        }));

        // Synthetic llm span for the compaction call: before/after context.
        let start = turn_last_child.unwrap_or(turn_start);
        let before = scope.conversation_history.clone();
        let span_id = ids::span_id(&self.session_id, &format!("llm:{turn_id}:compaction"));
        let name = scope
            .model
            .clone()
            .unwrap_or_else(|| "compaction".to_string());
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![turn_span.clone()],
            name: name.clone(),
            span_type: SpanType::Llm,
            start_ms: Some(start),
            input: Some(json!({ "messages_before_compaction": before.len(), "history": before })),
            output: Some(compaction_output(replacement.as_ref())),
            metadata: Some(json!({ "model": scope.model, "turn_id": turn_id, "compaction": true })),
            ..Default::default()
        }));
        if let Some(replacement) = replacement {
            // Native compaction is a semantic memory barrier: future requests
            // use only the replacement context, so the pre-compaction Values
            // can be dropped as soon as their one compaction span is emitted.
            scope.conversation_history = replacement;
        }
        let _ = (turn_span, name);
        scope.open_llm = Some(OpenLlm {
            span_id,
            turn_id,
            start_ms: start,
            last_output_ms: ts,
            output: Vec::new(),
            output_preset: true,
        });
    }

    fn end_main_root(&mut self, fallback_ts: i64, ops: &mut Vec<SpanOp>) {
        if self.root_ended || !self.root_opened {
            return;
        }
        self.root_ended = true;
        let end_ms = self
            .main_path
            .as_ref()
            .and_then(|path| self.scopes.get(path))
            .and_then(|scope| scope.last_turn_end_ms)
            .unwrap_or(fallback_ts);
        ops.push(SpanOp::Merge(SpanRow {
            span_id: self.root_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: self.external_parent_span_id.clone().into_iter().collect(),
            end_ms: Some(end_ms),
            ..Default::default()
        }));
    }

    fn close_compaction_turn(&mut self, payload: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let Some(path) = self.main_path.clone() else {
            return;
        };
        let Some(mut scope) = self.scopes.remove(&path) else {
            return;
        };
        // The compaction turn may not get a task_complete of its own.
        self.close_turn(&mut scope, payload, ts, ops);
        self.scopes.insert(path, scope);
    }

    fn close_subagent(&mut self, path: &str, ts: i64, ops: &mut Vec<SpanOp>) {
        let Some(mut scope) = self.scopes.remove(path) else {
            return;
        };
        if !scope.subagent_ended && scope.root_created {
            scope.subagent_ended = true;
            let end = scope.last_turn_end_ms.unwrap_or(ts);
            self.close_dangling(&mut scope, Some(end), ops);
            // End the subagent root span.
            ops.push(SpanOp::Merge(SpanRow {
                span_id: scope.turn_parent_span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: self.scope_root_parent_span_ids(&scope),
                end_ms: Some(end),
                ..Default::default()
            }));
        }
        // SubagentStop is terminal for this transcript. Its durable rollout is
        // still on disk and journal replay can rebuild it, so retaining the
        // closed scope would only pin its entire conversation history.
    }

    /// Close any open llm/tool/turn in `scope` (used on subagent stop + flush).
    fn close_dangling(&mut self, scope: &mut Scope, end: Option<i64>, ops: &mut Vec<SpanOp>) {
        let end_ms = end.or(scope.last_turn_end_ms);
        if let Some(llm) = scope.open_llm.take() {
            let output = if llm.output_preset {
                None
            } else {
                Some(llm_output(&llm.output))
            };
            ops.push(SpanOp::Merge(SpanRow {
                span_id: llm.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: self.turn_parent_span_ids(&llm.turn_id),
                end_ms: end_ms.or(Some(llm.last_output_ms)),
                output,
                metadata: Some(json!({
                    "usage_unavailable_reason": "codex_transcript_missing_token_count"
                })),
                ..Default::default()
            }));
        }
        let tools: Vec<(String, String, String)> = scope
            .open_tools
            .drain()
            .map(|(_, (span_id, turn_id))| {
                (span_id, turn_id, MISSING_TOOL_OUTPUT_ERROR.to_string())
            })
            .collect();
        for (sid, turn_id, error) in tools {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: sid,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: self.turn_parent_span_ids(&turn_id),
                end_ms,
                metadata: Some(tool_approval_metadata(Some(ToolApproval::Approved))),
                error: Some(error),
                ..Default::default()
            }));
        }
        for turn in scope.open_turns.drain(..) {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![scope.turn_parent_span_id.clone()],
                end_ms,
                ..Default::default()
            }));
        }
    }
}

fn effective_transcript_path(event: &Envelope) -> Option<String> {
    event
        .payload
        .pointer("/_bt_transcript_mirror/mirror")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            if event.event == "SubagentStop" {
                str_field(&event.payload, "agent_transcript_path")
            } else {
                str_field(&event.payload, "transcript_path")
                    .or_else(|| str_field(&event.payload, "agent_transcript_path"))
            }
        })
}

impl Scope {
    fn new(path: &str, kind: ScopeKind, turn_parent_span_id: String) -> Self {
        Scope {
            path: path.to_string(),
            kind,
            offset: 0,
            turn_parent_span_id,
            root_created: false,
            model: None,
            current_cwd: None,
            open_turns: Vec::new(),
            conversation_history: Vec::new(),
            open_llm: None,
            open_tools: HashMap::new(),
            last_turn_end_ms: None,
            turn_seq: 0,
            agent_id: None,
            agent_type: None,
            spawning_turn_span_id: None,
            subagent_ended: false,
        }
    }
}

// ---- helpers ---------------------------------------------------------------

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(|s| s.to_string())
}

fn basename(path: &str) -> String {
    let trimmed = path.trim_end_matches(['/', '\\']);
    trimmed
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(trimmed)
        .to_string()
}

fn message_text(payload: &Value) -> String {
    payload
        .get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| str_field(p, "text"))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn llm_output(items: &[Value]) -> Value {
    if items.len() == 1 {
        items[0].clone()
    } else {
        Value::Array(items.to_vec())
    }
}

fn push_tool_result(scope: &mut Scope, call_id: Option<&str>, payload: &Value) {
    let output = payload
        .get("output")
        .or_else(|| payload.get("result"))
        .cloned()
        .unwrap_or(Value::Null);
    let content = output
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| serde_json::to_string(&output).unwrap_or_else(|_| "null".to_string()));
    scope.conversation_history.push(json!({
        "role": "tool",
        "content": content,
        "tool_call_id": call_id.unwrap_or_default(),
    }));
}

fn args_object(args: Option<&Value>) -> Option<Map<String, Value>> {
    match args? {
        Value::Object(map) => Some(map.clone()),
        Value::String(raw) => serde_json::from_str::<Value>(raw)
            .ok()
            .and_then(|value| value.as_object().cloned()),
        _ => None,
    }
}

fn classify_tool_output(output: &Value) -> Option<String> {
    if let Some(object) = output.as_object() {
        if object.get("is_error").and_then(Value::as_bool) == Some(true)
            || object.get("isError").and_then(Value::as_bool) == Some(true)
            || matches!(
                object.get("status").and_then(Value::as_str),
                Some("error" | "failed")
            )
        {
            return Some(error_text(Some(output), "Tool execution failed"));
        }
        if let Some(error) = object.get("error") {
            return Some(error_text(Some(error), "Tool execution failed"));
        }
        if let Some(exit_code) = object
            .get("exit_code")
            .or_else(|| object.get("exitCode"))
            .and_then(Value::as_i64)
        {
            if exit_code != 0 {
                return Some(error_text(Some(output), &format!("Exit code {exit_code}")));
            }
        }
    }
    if let Some(text) = output.as_str() {
        let first = text.lines().next().unwrap_or(text);
        if first.to_ascii_lowercase().starts_with("error:") {
            return Some(first.to_string());
        }
        if let Some(code) = first
            .strip_prefix("Exit code ")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<i64>().ok())
        {
            if code != 0 {
                return Some(first.to_string());
            }
        }
    }
    None
}

#[derive(Default)]
struct SkillLoad {
    name: Option<String>,
    path: Option<String>,
}

fn string_candidates(args: Option<&Value>) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(raw) = args.and_then(Value::as_str) {
        candidates.push(raw.to_string());
    }
    if let Some(object) = args_object(args) {
        for key in [
            "path",
            "file_path",
            "filePath",
            "file",
            "command",
            "cmd",
            "resource",
        ] {
            if let Some(value) = object.get(key).and_then(Value::as_str) {
                candidates.push(value.to_string());
            }
        }
    }
    candidates
}

fn detect_skill(tool_name: &str, args: Option<&Value>) -> Option<SkillLoad> {
    if tool_name == "skills.read" {
        if let Some(object) = args_object(args) {
            return Some(SkillLoad {
                name: object
                    .get("name")
                    .or_else(|| object.get("package"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                path: None,
            });
        }
    }
    static SKILL_PATH: OnceLock<Regex> = OnceLock::new();
    static SCRIPT_PATH: OnceLock<Regex> = OnceLock::new();
    let skill_path =
        SKILL_PATH.get_or_init(|| Regex::new(r#"(?i)([^\s"']*SKILL\.md)"#).expect("regex"));
    let script_path = SCRIPT_PATH
        .get_or_init(|| Regex::new(r#"(?i)([^\s"']*[\\/]scripts[\\/][^\s"']+)"#).expect("regex"));
    for candidate in string_candidates(args) {
        if let Some(path) = skill_path
            .captures(&candidate)
            .and_then(|capture| capture.get(1))
            .map(|capture| capture.as_str().to_string())
        {
            let normalized = path.replace('\\', "/");
            let name = Path::new(&normalized)
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .map(str::to_string);
            return Some(SkillLoad {
                name,
                path: Some(path),
            });
        }
        if let Some(path) = script_path
            .captures(&candidate)
            .and_then(|capture| capture.get(1))
            .map(|capture| capture.as_str().to_string())
        {
            let normalized = path.replace('\\', "/");
            let name = Path::new(&normalized)
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .map(str::to_string);
            return Some(SkillLoad {
                name,
                path: Some(path),
            });
        }
    }
    None
}

fn permission_info(args: Option<&Value>) -> Option<Value> {
    let object = args_object(args)?;
    let sandbox_permissions = object.get("sandbox_permissions")?.as_str()?;
    if sandbox_permissions.is_empty() {
        return None;
    }
    let mut permission = Map::new();
    permission.insert("sandbox_permissions".into(), json!(sandbox_permissions));
    if let Some(justification) = object.get("justification").and_then(Value::as_str) {
        permission.insert("justification".into(), json!(justification));
    }
    if let Some(prefix_rule) = object.get("prefix_rule") {
        permission.insert("prefix_rule".into(), prefix_rule.clone());
    }
    Some(Value::Object(permission))
}

fn explicit_skill_names(text: &str) -> Vec<String> {
    static EXPLICIT_SKILLS: OnceLock<Vec<Regex>> = OnceLock::new();
    static SKILL_XML: OnceLock<Regex> = OnceLock::new();
    static SKILL_XML_NAME: OnceLock<Regex> = OnceLock::new();
    static SKILL_FRONTMATTER_NAME: OnceLock<Regex> = OnceLock::new();
    let patterns = EXPLICIT_SKILLS.get_or_init(|| {
        [
            r#"\$([A-Za-z0-9_.:-]+)"#,
            r#"(?:^|\s)/skills\s+([A-Za-z0-9_.:-]+)"#,
            r#"skill://([A-Za-z0-9_.:-]+)"#,
            r#"(?i)(?:^|[\s"'])([^\s"']*SKILL\.md)(?:$|[\s"'])"#,
            r#"UserInput::Skill\([^)]*(?:name|skill|id)\s*[:=]\s*["']?([A-Za-z0-9_.:-]+)"#,
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("regex"))
        .collect()
    });
    let mut names = Vec::new();
    for (index, pattern) in patterns.iter().enumerate() {
        for capture in pattern.captures_iter(text) {
            let Some(value) = capture.get(1).map(|capture| capture.as_str()) else {
                continue;
            };
            let name = if index == 3 {
                let normalized = value.replace('\\', "/");
                Path::new(&normalized)
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|value| value.to_str())
                    .unwrap_or(value)
                    .to_string()
            } else {
                value
                    .trim()
                    .trim_start_matches('$')
                    .trim_end_matches([',', ')', '.', ';'])
                    .to_string()
            };
            if !name.is_empty() && !names.contains(&name) {
                names.push(name);
            }
        }
    }
    let xml = SKILL_XML
        .get_or_init(|| Regex::new(r#"(?is)<skill\b([^>]*)>(.*?)</skill>"#).expect("regex"));
    let attr_name = SKILL_XML_NAME
        .get_or_init(|| Regex::new(r#"(?:name|id)=["']([^"']+)["']"#).expect("regex"));
    let frontmatter_name = SKILL_FRONTMATTER_NAME
        .get_or_init(|| Regex::new(r#"(?m)(?:^|\n)name:\s*([A-Za-z0-9_.:-]+)"#).expect("regex"));
    for capture in xml.captures_iter(text) {
        let candidate = capture
            .get(1)
            .and_then(|attrs| attr_name.captures(attrs.as_str()))
            .and_then(|capture| capture.get(1))
            .or_else(|| {
                capture
                    .get(2)
                    .and_then(|body| frontmatter_name.captures(body.as_str()))
                    .and_then(|capture| capture.get(1))
            })
            .map(|capture| capture.as_str().to_string());
        if let Some(name) = candidate {
            if !name.is_empty() && !names.contains(&name) {
                names.push(name);
            }
        }
    }
    names
}

fn explicit_skill_metadata(names: &[String]) -> Option<Value> {
    (!names.is_empty()).then(|| {
        json!({
            "loaded_skill_names": names,
            "loaded_skills": names.iter().map(|name| json!({ "name": name })).collect::<Vec<_>>(),
        })
    })
}

fn compaction_output(replacement: Option<&Vec<Value>>) -> Value {
    let Some(items) = replacement else {
        return json!({ "summary": "[unavailable]", "kept_messages": [] });
    };
    let mut kept = Vec::new();
    let mut summary_encrypted = false;
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("compaction") {
            if item
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_some()
            {
                summary_encrypted = true;
            }
            continue;
        }
        kept.push(item.clone());
    }
    json!({
        "summary": if summary_encrypted {
            "[summary unavailable — encrypted by Codex]"
        } else {
            "[no summary]"
        },
        "kept_messages": kept,
    })
}

fn parse_ts(rec: &Value) -> Option<i64> {
    let s = rec.get("timestamp").and_then(Value::as_str)?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

struct ReadLines {
    lines: Vec<String>,
    complete: bool,
}

fn read_new_lines(
    path: &str,
    offset: &mut u64,
    through_ms: Option<i64>,
    through_bytes: Option<u64>,
    byte_budget: usize,
) -> ReadLines {
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return ReadLines {
            lines: Vec::new(),
            complete: true,
        };
    };
    if let Ok(meta) = f.metadata() {
        if *offset > meta.len() {
            *offset = 0;
        }
    }
    if f.seek(SeekFrom::Start(*offset)).is_err() {
        return ReadLines {
            lines: Vec::new(),
            complete: true,
        };
    }
    let mut reader = BufReader::new(f);
    let mut lines = Vec::new();
    let mut consumed = 0usize;
    loop {
        if through_bytes.is_some_and(|limit| *offset >= limit) {
            break;
        }
        if consumed >= byte_budget && !lines.is_empty() {
            return ReadLines {
                lines,
                complete: false,
            };
        }
        let mut line = String::new();
        let Ok(bytes) = reader.read_line(&mut line) else {
            break;
        };
        if bytes == 0 || !line.ends_with('\n') {
            break;
        }
        if through_bytes.is_some_and(|limit| offset.saturating_add(bytes as u64) > limit) {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(limit) = through_ms {
            if serde_json::from_str::<Value>(trimmed)
                .ok()
                .and_then(|record| parse_ts(&record))
                .is_some_and(|timestamp| timestamp > limit)
            {
                break;
            }
        }
        *offset += bytes as u64;
        consumed += bytes;
        if !trimmed.trim().is_empty() {
            lines.push(trimmed.to_string());
        }
    }
    ReadLines {
        lines,
        complete: true,
    }
}

fn token_metrics(usage: &Value) -> Map<String, Value> {
    const MAP: &[(&str, &str)] = &[
        ("input_tokens", "prompt_tokens"),
        ("prompt_tokens", "prompt_tokens"),
        ("output_tokens", "completion_tokens"),
        ("completion_tokens", "completion_tokens"),
        ("total_tokens", "tokens"),
        ("tokens", "tokens"),
        ("cached_input_tokens", "prompt_cached_tokens"),
        ("prompt_cached_tokens", "prompt_cached_tokens"),
        ("input_tokens_details.cached_tokens", "prompt_cached_tokens"),
        (
            "prompt_tokens_details.cached_tokens",
            "prompt_cached_tokens",
        ),
        (
            "prompt_cache_creation_tokens",
            "prompt_cache_creation_tokens",
        ),
        (
            "input_tokens_details.cache_creation_tokens",
            "prompt_cache_creation_tokens",
        ),
        (
            "input_tokens_details.cache_write_tokens",
            "prompt_cache_creation_tokens",
        ),
        (
            "prompt_tokens_details.cache_creation_tokens",
            "prompt_cache_creation_tokens",
        ),
        (
            "prompt_tokens_details.cache_write_tokens",
            "prompt_cache_creation_tokens",
        ),
        ("reasoning_output_tokens", "completion_reasoning_tokens"),
        ("completion_reasoning_tokens", "completion_reasoning_tokens"),
        ("reasoning_tokens", "completion_reasoning_tokens"),
        (
            "output_tokens_details.reasoning_tokens",
            "completion_reasoning_tokens",
        ),
        (
            "completion_tokens_details.reasoning_tokens",
            "completion_reasoning_tokens",
        ),
        ("cost", "cost"),
        ("cost", "estimated_cost"),
        ("estimated_cost", "estimated_cost"),
        ("total_cost", "estimated_cost"),
        ("cost_usd", "estimated_cost"),
    ];
    let mut metrics = Map::new();
    for (from, to) in MAP {
        if metrics.contains_key(*to) {
            continue;
        }
        if let Some(v) = num_at(usage, from) {
            metrics.insert((*to).to_string(), json!(v));
        }
    }
    if !metrics.contains_key("tokens") {
        if let (Some(p), Some(c)) = (
            metrics.get("prompt_tokens").and_then(Value::as_f64),
            metrics.get("completion_tokens").and_then(Value::as_f64),
        ) {
            metrics.insert("tokens".to_string(), json!(p + c));
        }
    }
    metrics
}

fn num_at(v: &Value, path: &str) -> Option<f64> {
    let mut cur = v;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    cur.as_f64().filter(|n| n.is_finite())
}

fn username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::basename;

    #[test]
    fn basename_accepts_unix_and_windows_paths() {
        assert_eq!(basename("/tmp/project"), "project");
        assert_eq!(basename(r"C:\Users\agent\project"), "project");
        assert_eq!(basename(r"C:\Users\agent\project\\"), "project");
    }
}
