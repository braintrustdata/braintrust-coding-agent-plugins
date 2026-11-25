//! Claude Code hook/transcript translator.
//!
//! Hook events own lifecycle and timing. Transcript rows supply model calls,
//! conversation history, usage, and a recovery path for tool calls whose hook
//! event was missed. Transcript cursors advance on every hook, but never past
//! the hook timestamp; this is essential when replaying a journal against a
//! transcript that already contains the completed session.

mod activity_bounds;
mod history_message;
mod hook_context;
mod llm_call;
mod message_history;
mod native_field;
mod parsed_transcript;
mod subagent_hook;
mod timestamp_envelope;
mod tool_completion_hook;
mod transcript_envelope;
mod transcript_tool;
mod transcript_tool_outcome;
mod user_prompt_hook;

use activity_bounds::ActivityBounds;
use hook_context::HookContext;
use llm_call::LlmCall;
use message_history::MessageHistory;
use parsed_transcript::ParsedTranscript;
pub(crate) use subagent_hook::SubagentHook;
#[cfg(test)]
use timestamp_envelope::parse_rfc3339_timestamp;
use timestamp_envelope::parse_timestamp_ms;
use tool_completion_hook::ToolCompletionHook;
use transcript_envelope::TranscriptEnvelope;
pub(crate) use transcript_envelope::{
    cli_fork_history, is_background_agent_launch, CliForkHistory,
};
use transcript_tool::TranscriptTool;
use transcript_tool_outcome::{is_permission_denial, TranscriptToolOutcome};
pub(crate) use user_prompt_hook::UserPromptHook;

use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet, RECENT_ID_CAPACITY};
use super::tool::{
    add_tool_approval, explicit_skill_metadata, prompt_skill_names, push_unique, skill_file_load,
    skill_md_paths, SkillLoad, ToolApproval,
};
use super::{
    local_username, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
use crate::ids;
use crate::transcript_mirror::TranscriptPayload;
use crate::wire::Envelope;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, Seek, SeekFrom};
use std::sync::Arc;

fn decode<T: DeserializeOwned>(value: &Value) -> Option<T> {
    serde_json::from_value(value.clone()).ok()
}

impl TranslatorFactory for crate::agents::Claude {
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(ClaudeTranslator::new(
            session_id,
            GitMetadataCache::shared(),
        ))
    }
}
#[derive(Serialize, Deserialize)]
struct Turn {
    id: String,
    number: u32,
    cwd: Option<String>,
    parent_id: String,
    user_turn_id: Option<String>,
    effort: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct PendingPrompt {
    turn_id: Arc<String>,
    content: Value,
}

#[derive(Default, Serialize, Deserialize)]
struct TranscriptCursor {
    offset: u64,
    buffered: Vec<Value>,
}
#[derive(Serialize, Deserialize)]
struct Subagent {
    span_id: String,
    parent_span_id: String,
    transcript_path: Option<String>,
    #[serde(default)]
    deferred_insert: Option<SpanRow>,
}

#[derive(Clone, Serialize, Deserialize)]
struct AgentOrigin {
    parent_id: String,
    user_turn_id: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct PendingTool {
    span_id: String,
    parent_id: String,
    input: Value,
    metadata: Value,
}
#[derive(Serialize, Deserialize)]
enum PendingHistory {
    Main,
    Owned(MessageHistory),
}

#[derive(Serialize, Deserialize)]
struct PendingEmission {
    segments: VecDeque<Vec<Value>>,
    history: PendingHistory,
    scope: String,
    parent: String,
    cwd: Option<String>,
    clear_after: bool,
}
#[derive(Serialize, Deserialize)]
struct ClaudeTranslator {
    session_id: String,
    session_span_id: String,
    root_span_id: String,
    session_parent_span_ids: Vec<String>,
    root_open: bool,
    root_end_ms: Option<i64>,
    root_generation: u32,
    turn: Option<Turn>,
    last_turn_id: Option<String>,
    turn_count: u32,
    continuation_count: u32,
    last_user_turn_id: Option<String>,
    agent_origins: RecentMap<String, AgentOrigin>,
    /// User turn owning each turn or subagent span that can parent other spans.
    span_owners: RecentMap<String, String>,
    /// Hierarchy for enriching earlier turns when their native attachments arrive late.
    #[serde(default)]
    turn_identities: RecentMap<String, (String, String)>,
    user_turn_ends: RecentMap<String, i64>,
    #[serde(default)]
    activity_bounds: ActivityBounds,
    prompt_turns: RecentMap<String, Arc<String>>,
    pending_prompts: VecDeque<PendingPrompt>,
    record_turns: RecentMap<String, Arc<String>>,
    request_turns: RecentMap<String, Arc<String>>,
    tool_turns: RecentMap<String, Arc<String>>,
    main_record_turn: Option<Arc<String>>,
    tool_seq: u32,
    main_transcript: Option<String>,
    transcripts: HashMap<String, TranscriptCursor>,
    main_history: MessageHistory,
    emitted_requests: RecentSet<String>,
    emitted_tools: RecentSet<String>,
    pending_tools: HashMap<String, PendingTool>,
    #[serde(default)]
    pending_nested_spawns: HashSet<String>,
    subagents: HashMap<String, Subagent>,
    subagent_handbacks: HashMap<String, Value>,
    #[serde(default)]
    observe_stopped_subagents: bool,
    /// Skills the user asked for on the current (or next) turn: `/skill`
    /// expansions and `SKILL.md` paths named in the prompt. Cleared when a
    /// turn stops, so a turn that ends without a Stop hands its skills to the
    /// next one. Skill loads outside this list were the model's choice.
    requested_skills: Vec<String>,
    pending_emission: Option<PendingEmission>,
    claude_version: Option<String>,
    claude_version_logged: bool,
    session_source: Option<String>,
    session_model: Option<String>,
    permission_mode: Option<String>,
    effort: Option<String>,
    system_prompt: Option<String>,
    #[serde(skip, default = "GitMetadataCache::shared")]
    git: Arc<GitMetadataCache>,
    current_cwd: Option<String>,
    last_turn_cwd: Option<String>,
    last_turn_output: Option<Value>,
    session_status: Option<String>,
    last_ts_ms: i64,
}

impl ClaudeTranslator {
    fn new(session_id: &str, git: Arc<GitMetadataCache>) -> Self {
        let root = ids::span_id(session_id, "root");
        Self {
            session_id: session_id.to_string(),
            session_span_id: root.clone(),
            root_span_id: root,
            session_parent_span_ids: Vec::new(),
            root_open: false,
            root_end_ms: None,
            root_generation: 0,
            turn: None,
            last_turn_id: None,
            turn_count: 0,
            continuation_count: 0,
            last_user_turn_id: None,
            agent_origins: RecentMap::default(),
            span_owners: RecentMap::default(),
            turn_identities: RecentMap::default(),
            user_turn_ends: RecentMap::default(),
            activity_bounds: ActivityBounds::default(),
            tool_seq: 0,
            prompt_turns: RecentMap::default(),
            pending_prompts: VecDeque::new(),
            record_turns: RecentMap::default(),
            request_turns: RecentMap::default(),
            tool_turns: RecentMap::default(),
            main_record_turn: None,
            main_transcript: None,
            transcripts: HashMap::new(),
            main_history: MessageHistory::default(),
            emitted_requests: RecentSet::default(),
            emitted_tools: RecentSet::default(),
            pending_tools: HashMap::new(),
            pending_nested_spawns: HashSet::new(),
            subagents: HashMap::new(),
            subagent_handbacks: HashMap::new(),
            observe_stopped_subagents: false,
            requested_skills: Vec::new(),
            pending_emission: None,
            claude_version: None,
            claude_version_logged: false,
            session_source: None,
            session_model: None,
            permission_mode: None,
            effort: None,
            system_prompt: None,
            git,
            current_cwd: None,
            last_turn_cwd: None,
            last_turn_output: None,
            session_status: None,
            last_ts_ms: 0,
        }
    }

    fn ensure_root(
        &mut self,
        event: &Envelope,
        ctx: &SessionCtx,
        hook: &HookContext,
        ops: &mut Vec<SpanOp>,
    ) {
        if self.root_open {
            return;
        }
        if self.root_end_ms.is_some() {
            self.root_generation = self.root_generation.saturating_add(1);
            let root = ids::span_id(&self.session_id, &format!("root:{}", self.root_generation));
            self.session_span_id = root.clone();
            self.root_span_id = root;
            self.root_end_ms = None;
            self.activity_bounds = ActivityBounds::default();
            self.claude_version_logged = false;
        }
        self.root_open = true;
        let cwd = hook.cwd.clone().unwrap_or_default();
        let workspace = basename(&cwd);
        let mut metadata = serde_json::Map::new();
        // Internal routing settings must never appear as user metadata.
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        metadata.insert("session_id".into(), json!(ctx.session_id));
        metadata.insert("workspace".into(), json!(cwd));
        metadata.insert("source".into(), json!("claude-code"));
        metadata.insert("hostname".into(), json!(hostname()));
        metadata.insert("username".into(), json!(local_username()));
        metadata.insert(
            "os".into(),
            json!(command_output("", "uname", &["-s"])
                .unwrap_or_else(|| std::env::consts::OS.to_string())),
        );
        if let Some(version) = &event.source_version {
            metadata.insert("trace_claude_code_version".into(), json!(version));
        }
        if let Some(version) = &self.claude_version {
            metadata.insert("claude_code_version".into(), json!(version));
        }
        if let Some(source) = hook.source.clone().or_else(|| self.session_source.clone()) {
            metadata.insert("session_source".into(), json!(source));
        }
        if let Some(model) = hook.model.clone().or_else(|| self.session_model.clone()) {
            metadata.insert("model".into(), json!(model));
        }
        if let Some(system_prompt) = hook
            .system_prompt
            .clone()
            .or_else(|| self.system_prompt.clone())
        {
            metadata.insert("system_prompt".into(), json!(system_prompt));
        }
        ops.push(SpanOp::Insert(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: self.session_parent_span_ids.clone(),
            name: format!("Claude Code: {workspace}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            input: Some(json!(format!("Session: {workspace}"))),
            metadata: Some(Value::Object(metadata)),
            tags: None,
            ..Default::default()
        }));
    }

    fn tail_main(&mut self, event: &Envelope, hook: &HookContext) {
        if let Some(path) = &hook.transcript_path {
            self.main_transcript = Some(path.clone());
        }
        let Some(path) = self.main_transcript.clone() else {
            return;
        };
        let cursor = self.transcripts.entry(path.clone()).or_default();
        let Ok(fields) = TranscriptPayload::decode(&event.payload) else {
            return;
        };
        let rows = read_records_for_payload(event.ts_ms, &fields, &path, &mut cursor.offset);
        if self.claude_version.is_none() {
            self.claude_version = rows.iter().find_map(|row| string_field(row, "version"));
        }
        cursor.buffered.extend(rows);
        if fields.import_through_offset.is_none() && self.last_turn_id.is_none() {
            if let CliForkHistory::EndsAt(end) = cli_fork_history(&cursor.buffered) {
                let remainder = cursor.buffered.split_off(end);
                let history = std::mem::replace(&mut cursor.buffered, remainder);
                self.main_history = std::mem::take(&mut self.main_history)
                    .parse_records(&history)
                    .history;
            }
        }
    }

    fn observe_session_details(&mut self, hook: &HookContext) {
        if let Some(source) = &hook.source {
            self.session_source = Some(source.clone());
        }
        if let Some(model) = &hook.model {
            self.session_model = Some(model.clone());
        }
        if let Some(permission_mode) = &hook.permission_mode {
            self.permission_mode = Some(permission_mode.clone());
        }
        if let Some(effort) = &hook.effort {
            self.effort = Some(effort.clone());
        }
        if let Some(system_prompt) = &hook.system_prompt {
            self.system_prompt = Some(system_prompt.clone());
        }
    }

    /// Passive Claude hooks must not create a trace. In particular, Claude
    /// emits idle notifications after a completed turn, and it may start or
    /// resume a session long before the user submits a prompt.
    fn starts_trace(event: &Envelope) -> bool {
        matches!(
            event.event.as_str(),
            "UserPromptSubmit"
                | "PreToolUse"
                | "PostToolUse"
                | "PostToolUseFailure"
                | "PermissionDenied"
                | "PostCompact"
                | "SubagentStart"
                | "SubagentStop"
        )
    }

    fn open_turn(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        self.session_status = Some("running".into());
        if let Some(old) = self.turn.take() {
            self.close_pending_tools(
                &old.id,
                event.ts_ms,
                "Turn ended before tool completion",
                ops,
            );
            let end_ms = self.execution_end(&old, event.ts_ms, ops);
            ops.push(SpanOp::Merge(SpanRow {
                span_id: old.id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![old.parent_id],
                end_ms: Some(end_ms),
                late_merge_key: Some(format!("execution:stop:{}", event.ts_ms)),
                ..Default::default()
            }));
        }
        let continuation = continuation_origin(&event.payload);
        let (id, parent_id, user_turn_id, name) = if let Some((_, task_id)) = continuation {
            self.continuation_count += 1;
            let owner = self
                .agent_origins
                .get(task_id)
                .and_then(|origin| origin.user_turn_id.clone());
            (
                ids::span_id(
                    &self.session_id,
                    &format!("continuation:{}", self.continuation_count),
                ),
                owner
                    .clone()
                    .unwrap_or_else(|| self.session_span_id.clone()),
                owner,
                "Continuation: subagent result".to_owned(),
            )
        } else {
            self.turn_count += 1;
            let id = ids::span_id(&self.session_id, &format!("turn:{}", self.turn_count));
            (
                id.clone(),
                self.session_span_id.clone(),
                Some(id),
                format!("Turn {}", self.turn_count),
            )
        };
        if continuation.is_some() {
            self.activity_bounds.track(&id);
        }
        // A prompt that points the model at a skill file ("Read the skill
        // instructions at docs/.agents/skills/x/SKILL.md") requests that skill
        // as directly as a `/x` slash command. Continuations are runtime
        // notifications, not user requests.
        if continuation.is_none() {
            if let Some(prompt) = event.payload.get("prompt").and_then(Value::as_str) {
                for name in prompt_skill_names(prompt) {
                    push_unique(&mut self.requested_skills, name);
                }
            }
        }
        let mut metadata = explicit_skill_metadata(&self.requested_skills)
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        if let Some(permission_mode) = &self.permission_mode {
            metadata.insert("permission_mode".into(), json!(permission_mode));
        }
        if let Some(effort) = &self.effort {
            metadata.insert("effort".into(), json!(effort));
        }
        if let Some(kind) = turn_input_kind(&event.payload) {
            metadata.insert("input_kind".into(), json!(kind));
        }
        if let Some((trigger, task_id)) = continuation {
            metadata.insert("turn_trigger".into(), json!(trigger));
            metadata.insert("task_id".into(), json!(task_id));
        }
        let owner = user_turn_id.clone().unwrap_or_else(|| id.clone());
        ops.push(SpanOp::Insert(SpanRow {
            span_id: id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent_id.clone()],
            name,
            span_type: SpanType::Task,
            is_turn: true,
            start_ms: Some(event.ts_ms),
            input: event.payload.get("prompt").cloned(),
            metadata: (!metadata.is_empty()).then_some(Value::Object(metadata)),
            turn_span_id: Some(owner.clone()),
            ..Default::default()
        }));
        self.turn_identities
            .insert(id.clone(), (self.root_span_id.clone(), parent_id.clone()));
        self.span_owners.insert(id.clone(), owner);
        let turn_id = Arc::new(id.clone());
        if let Some(prompt_id) = event.payload.get("prompt_id").and_then(Value::as_str) {
            self.prompt_turns
                .insert(prompt_id.to_owned(), turn_id.clone());
        }
        if let Some(content) = event.payload.get("prompt").filter(|value| !value.is_null()) {
            if self.pending_prompts.len() == RECENT_ID_CAPACITY {
                self.pending_prompts.pop_front();
            }
            self.pending_prompts.push_back(PendingPrompt {
                turn_id,
                content: content.clone(),
            });
        }
        self.turn = Some(Turn {
            id,
            number: self.turn_count,
            cwd: self.current_cwd.clone(),
            parent_id,
            user_turn_id,
            effort: self.effort.clone(),
        });
    }

    /// Claude reports effort on later hooks, not reliably on UserPromptSubmit.
    /// Merge a newly observed level into the active turn.
    fn record_turn_effort(&mut self, ops: &mut Vec<SpanOp>) {
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        if self.effort.is_none() || turn.effort == self.effort {
            return;
        }
        turn.effort = self.effort.clone();
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn.id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![turn.parent_id.clone()],
            metadata: Some(json!({ "effort": turn.effort })),
            ..Default::default()
        }));
    }

    // A stopped foreground response can still own background work. Retain its
    // causal identity and extend its duration without replacing its response.
    fn extend_user_turn(&mut self, id: &String, end_ms: i64, ops: &mut Vec<SpanOp>) {
        if self
            .user_turn_ends
            .get(id)
            .is_some_and(|previous| *previous >= end_ms)
        {
            return;
        }
        self.user_turn_ends.insert(id.clone(), end_ms);
        if self.turn.as_ref().is_some_and(|turn| &turn.id == id) {
            return;
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id: id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![self.session_span_id.clone()],
            end_ms: Some(end_ms),
            late_merge_key: Some(format!("turn:activity:{end_ms}")),
            ..Default::default()
        }));
    }

    fn execution_end(&mut self, turn: &Turn, end_ms: i64, ops: &mut Vec<SpanOp>) -> i64 {
        if let Some(owner) = &turn.user_turn_id {
            if owner != &turn.id {
                self.extend_user_turn(owner, end_ms, ops);
            } else {
                let end_ms = self
                    .user_turn_ends
                    .get(owner)
                    .copied()
                    .unwrap_or(end_ms)
                    .max(end_ms);
                self.user_turn_ends.insert(owner.clone(), end_ms);
                return end_ms;
            }
        }
        end_ms
    }

    fn record_skill(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        if string_field(&event.payload, "expansion_type")
            .or_else(|| string_field(&event.payload, "type"))
            .is_some_and(|kind| kind != "slash_command")
        {
            return;
        }
        let direct = string_field(&event.payload, "skill_name")
            .or_else(|| string_field(&event.payload, "skillName"))
            .or_else(|| {
                event
                    .payload
                    .pointer("/skill/name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| string_field(&event.payload, "skill"));
        let command = string_field(&event.payload, "command_name")
            .or_else(|| string_field(&event.payload, "command"))
            .or_else(|| string_field(&event.payload, "slash_command"))
            .or_else(|| string_field(&event.payload, "name"))
            .map(|name| normalize_skill_name(&name));
        let name = direct.map(|name| normalize_skill_name(&name)).or_else(|| {
            let command = command?;
            let path = string_field(&event.payload, "transcript_path")?;
            skill_listing_contains(&path, &command).then_some(command)
        });
        let Some(name) = name.filter(|name| !name.is_empty()) else {
            return;
        };
        push_unique(&mut self.requested_skills, name);
        if let Some(turn) = &self.turn {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![turn.parent_id.clone()],
                metadata: explicit_skill_metadata(&self.requested_skills),
                ..Default::default()
            }));
        }
    }

    fn parent_for(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) -> Option<String> {
        if let Some(hook) = decode::<SubagentHook>(&event.payload) {
            return Some(self.ensure_subagent(&hook, event, ops));
        }
        self.turn.as_ref().map(|turn| turn.id.clone())
    }

    fn owner_of(&self, parent: &str) -> Option<String> {
        self.span_owners.get(parent).cloned()
    }

    fn ensure_subagent(
        &mut self,
        hook: &SubagentHook,
        event: &Envelope,
        ops: &mut Vec<SpanOp>,
    ) -> String {
        if let Some(agent) = self.subagents.get(&hook.agent_id) {
            return agent.span_id.clone();
        }
        let origin = self
            .agent_origins
            .get(&hook.agent_id)
            .cloned()
            .unwrap_or_else(|| AgentOrigin {
                parent_id: self
                    .turn
                    .as_ref()
                    .map(|turn| turn.id.clone())
                    .or_else(|| self.last_turn_id.clone())
                    .unwrap_or_else(|| self.session_span_id.clone()),
                user_turn_id: self
                    .turn
                    .as_ref()
                    .map(|turn| turn.user_turn_id.clone())
                    .unwrap_or_else(|| self.last_user_turn_id.clone()),
            });
        let tool_parent = string_field(&event.payload, "_bt_import_parent_tool_id")
            .map(|id| ids::span_id(&self.session_id, &format!("tool:{id}")));
        if let Some(parent) = &tool_parent {
            self.activity_bounds.track(parent);
        }
        let parent_id = string_field(&event.payload, "_bt_import_parent_agent_id")
            .and_then(|id| self.subagents.get(&id).map(|agent| agent.span_id.clone()))
            .or(tool_parent)
            .unwrap_or_else(|| origin.parent_id.clone());
        let owner = origin
            .user_turn_id
            .clone()
            .or_else(|| self.owner_of(&parent_id));
        self.agent_origins.insert(hook.agent_id.clone(), origin);
        let agent_type = hook.agent_type.clone().unwrap_or_else(|| "agent".into());
        let span_id = ids::span_id(&self.session_id, &format!("subagent:{}", hook.agent_id));
        self.activity_bounds.track(&span_id);
        let row = SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent_id.clone()],
            name: format!("subagent: {agent_type}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            metadata: Some(json!({ "agent_id": hook.agent_id, "agent_type": agent_type })),
            turn_span_id: owner.clone(),
            ..Default::default()
        };
        let defer = !self.pending_nested_spawns.is_empty();
        if !defer {
            ops.push(SpanOp::Insert(row.clone()));
        }
        if let Some(owner) = owner {
            self.span_owners.insert(span_id.clone(), owner);
        }
        self.subagents.insert(
            hook.agent_id.clone(),
            Subagent {
                span_id: span_id.clone(),
                parent_span_id: parent_id,
                transcript_path: None,
                deferred_insert: defer.then_some(row),
            },
        );
        span_id
    }

    fn pre_tool(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(parent_id) = self.parent_for(event, ops) else {
            return;
        };
        let Some(tool_name) = tool_name(&event.payload) else {
            return;
        };
        let call_id = self.call_id(event);
        if self.pending_tools.contains_key(&call_id) || self.emitted_tools.contains(&call_id) {
            return;
        }
        if tool_name == "Agent" && string_field(&event.payload, "agent_id").is_some() {
            self.pending_nested_spawns.insert(call_id.clone());
        }
        let input = tool_input(&event.payload);
        let span_id = ids::span_id(&self.session_id, &format!("tool:{call_id}"));
        // PreToolUse only proves that Claude proposed a call. Approval is
        // recorded once a terminal hook or transcript result proves whether
        // the call executed or was denied.
        let skill = skill_load(&tool_name, &input);
        let mut metadata = tool_metadata(
            event,
            &tool_name,
            &call_id,
            None,
            skill.as_ref(),
            &self.requested_skills,
        );
        let span_type = if tool_name == "AskUserQuestion" {
            if let Some(fields) = metadata.as_object_mut() {
                fields.insert("wait_kind".into(), json!("user_question"));
            }
            SpanType::Wait
        } else {
            SpanType::Tool
        };
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent_id.clone()],
            name: tool_span_name(&tool_name, &input, skill.as_ref()),
            span_type,
            start_ms: Some(event.ts_ms),
            input: Some(input.clone()),
            metadata: Some(metadata.clone()),
            turn_span_id: self.owner_of(&parent_id),
            ..Default::default()
        }));
        self.pending_tools.insert(
            call_id,
            PendingTool {
                span_id,
                parent_id,
                input,
                metadata,
            },
        );
    }

    fn finish_tool(&mut self, event: &Envelope, mut approval: ToolApproval, ops: &mut Vec<SpanOp>) {
        let Some(tool_name) = tool_name(&event.payload) else {
            return;
        };
        let call_id = self.call_id(event);
        self.pending_nested_spawns.remove(&call_id);
        let input = tool_input(&event.payload);
        let failure = ToolCompletionHook::deserialize(&event.payload).unwrap_or_default();
        let failure_hook = event.event == "PostToolUseFailure";
        let denied = failure_hook && failure.error.as_ref().is_some_and(is_permission_denial);
        if denied {
            approval = ToolApproval::Denied;
        }
        let error = if approval == ToolApproval::Denied {
            None
        } else if failure_hook {
            Some(
                failure
                    .summary()
                    .unwrap_or_else(|| "Tool execution failed".into()),
            )
        } else {
            failure.tool_error()
        };
        let output = failure.into_output(denied);
        if approval == ToolApproval::Approved && error.is_none() && tool_name == "SubagentHandback"
        {
            if let (Some(agent_id), Some(message)) = (
                string_field(&event.payload, "agent_id"),
                raw_tool_input(&event.payload).and_then(|input| input.get("message")),
            ) {
                self.subagent_handbacks.insert(agent_id, message.clone());
            }
        }
        let skill = skill_load(&tool_name, &input);
        let mut metadata = tool_metadata(
            event,
            &tool_name,
            &call_id,
            Some(approval),
            skill.as_ref(),
            &self.requested_skills,
        );
        let duration = event.payload.get("duration_ms").and_then(Value::as_i64);
        if let Some(duration) = duration {
            metadata["native_duration_ms"] = json!(duration);
        }
        if let Some(interrupted) = event.payload.get("is_interrupt").and_then(Value::as_bool) {
            metadata["is_interrupt"] = json!(interrupted);
        }
        if let Some(pending) = self.pending_tools.remove(&call_id) {
            // A PreToolUse hook may rewrite the call via `updatedInput`, so the
            // terminal hook's `tool_input` is what the call ran (or was denied)
            // with. Resend it only when it differs from the proposed input.
            let rewritten = raw_tool_input(&event.payload).is_some() && input != pending.input;
            ops.push(SpanOp::Merge(SpanRow {
                span_id: pending.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![pending.parent_id],
                name: if rewritten {
                    tool_span_name(&tool_name, &input, skill.as_ref())
                } else {
                    String::new()
                },
                input: rewritten.then_some(input),
                end_ms: Some(event.ts_ms),
                output,
                metadata: Some(metadata),
                error,
                ..Default::default()
            }));
        } else if !self.emitted_tools.contains(&call_id) {
            let Some(parent_id) = self.parent_for(event, ops) else {
                return;
            };
            let span_type = if tool_name == "AskUserQuestion" {
                if let Some(fields) = metadata.as_object_mut() {
                    fields.insert("wait_kind".into(), json!("user_question"));
                }
                SpanType::Wait
            } else {
                SpanType::Tool
            };
            ops.push(SpanOp::Insert(SpanRow {
                span_id: ids::span_id(&self.session_id, &format!("tool:{call_id}")),
                root_span_id: self.root_span_id.clone(),
                turn_span_id: self.owner_of(&parent_id),
                parent_span_ids: vec![parent_id],
                name: tool_span_name(&tool_name, &input, skill.as_ref()),
                span_type,
                start_ms: Some(event.ts_ms.saturating_sub(duration.unwrap_or(0))),
                end_ms: Some(event.ts_ms),
                input: Some(input),
                output,
                metadata: Some(metadata),
                error,
                ..Default::default()
            }));
        }
        self.emitted_tools.insert(call_id);
    }

    fn call_id(&mut self, event: &Envelope) -> String {
        string_field(&event.payload, "tool_use_id").unwrap_or_else(|| {
            self.tool_seq += 1;
            let turn = self.turn.as_ref().map(|t| t.number).unwrap_or(0);
            format!("{turn}:{}", self.tool_seq)
        })
    }

    fn stop_subagent(&mut self, event: &Envelope, hook: SubagentHook, ops: &mut Vec<SpanOp>) {
        if event.payload.get("_bt_import_through_offset").is_none()
            && hook.agent_transcript_path.is_some()
        {
            self.observe_stopped_subagents = true;
        }
        let agent_id = hook.agent_id.clone();
        let parent = self.ensure_subagent(&hook, event, ops);
        let native_parent = string_field(&event.payload, "_bt_claude_parent_agent_id")
            .filter(|id| self.agent_origins.get(id).is_some());
        if let Some(agent) = self.subagents.get_mut(&agent_id) {
            if let Some(mut row) = agent.deferred_insert.take() {
                if let Some(native_parent) = native_parent.as_ref() {
                    let parent_span_id =
                        ids::span_id(&self.session_id, &format!("subagent:{native_parent}"));
                    agent.parent_span_id = parent_span_id.clone();
                    row.parent_span_ids = vec![parent_span_id];
                }
                ops.push(SpanOp::Insert(row));
            }
        }
        let parent_span_ids = self
            .subagents
            .get(&agent_id)
            .map(|agent| vec![agent.parent_span_id.clone()])
            .unwrap_or_else(|| vec![self.session_span_id.clone()]);
        let path = hook.agent_transcript_path;
        if let Some(agent) = self.subagents.get_mut(&agent_id) {
            agent.transcript_path = path.clone();
        }
        let mut transcript_handback = None;
        if let Some(path) = path {
            let cursor = self.transcripts.entry(path.clone()).or_default();
            cursor
                .buffered
                .extend(read_event_records(event, &path, &mut cursor.offset));
            let records = std::mem::take(&mut cursor.buffered);
            transcript_handback = successful_subagent_handback_message(&records);
            self.queue_transcript(
                records,
                PendingHistory::Owned(MessageHistory::default()),
                format!("subagent:{agent_id}"),
                parent.clone(),
                self.current_cwd.clone(),
                ops,
            );
            self.transcripts.remove(&path);
        }
        self.close_pending_tools(
            &parent,
            event.ts_ms,
            "Subagent ended before tool completion",
            ops,
        );
        let output = self
            .subagent_handbacks
            .remove(&agent_id)
            .or(transcript_handback)
            .or_else(|| event.payload.get("last_assistant_message").cloned());
        ops.push(SpanOp::Merge(SpanRow {
            span_id: parent.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids,
            end_ms: Some(event.ts_ms),
            output,
            ..Default::default()
        }));
        self.subagents.remove(&agent_id);
        if let Some(owner) = self
            .agent_origins
            .get(&agent_id)
            .and_then(|origin| origin.user_turn_id.clone())
        {
            self.extend_user_turn(&owner, event.ts_ms, ops);
        }
    }

    fn emit_main(&mut self, parent: &str, ops: &mut Vec<SpanOp>) {
        let Some(path) = self.main_transcript.clone() else {
            return;
        };
        let records = self
            .transcripts
            .get_mut(&path)
            .map(|cursor| std::mem::take(&mut cursor.buffered))
            .unwrap_or_default();
        self.queue_transcript(
            records,
            PendingHistory::Main,
            "main".into(),
            parent.into(),
            self.current_cwd.clone(),
            ops,
        );
    }

    fn queue_transcript(
        &mut self,
        records: Vec<Value>,
        history: PendingHistory,
        scope: String,
        parent: String,
        cwd: Option<String>,
        ops: &mut Vec<SpanOp>,
    ) {
        debug_assert!(self.pending_emission.is_none());
        let pending = PendingEmission {
            segments: transcript_segments(records),
            history,
            scope,
            parent,
            cwd,
            clear_after: false,
        };
        self.pending_emission = self.emit_next_pending(pending, ops);
    }

    fn emit_next_pending(
        &mut self,
        mut pending: PendingEmission,
        ops: &mut Vec<SpanOp>,
    ) -> Option<PendingEmission> {
        let Some(records) = pending.segments.pop_front() else {
            if pending.clear_after {
                self.release_terminal_state();
            }
            return None;
        };
        if matches!(pending.history, PendingHistory::Main) {
            self.observe_record_turns(&records, ops);
        }
        let history = match &mut pending.history {
            PendingHistory::Main => std::mem::take(&mut self.main_history),
            PendingHistory::Owned(history) => std::mem::take(history),
        };
        let ParsedTranscript {
            calls,
            tools,
            history,
        } = history.parse_records(&records);
        match &mut pending.history {
            PendingHistory::Main => self.main_history = history,
            PendingHistory::Owned(pending_history) => *pending_history = history,
        }
        let op_start = ops.len();
        self.emit_parsed(calls, tools, &pending.scope, &pending.parent, ops);
        self.git
            .enrich_rows(pending.cwd.as_deref(), &mut ops[op_start..]);
        if pending.segments.is_empty() {
            if pending.clear_after {
                self.release_terminal_state();
            }
            None
        } else {
            Some(pending)
        }
    }

    fn release_terminal_state(&mut self) {
        self.observe_stopped_subagents = false;
        self.main_history = MessageHistory::default();
        self.transcripts.clear();
        self.subagents.clear();
        self.agent_origins = RecentMap::default();
        self.user_turn_ends = RecentMap::default();
        self.last_user_turn_id = None;
        self.pending_tools.clear();
        self.pending_nested_spawns.clear();
        self.requested_skills.clear();
        self.emitted_requests.clear();
        self.emitted_tools.clear();
        self.prompt_turns = RecentMap::default();
        self.pending_prompts.clear();
        self.record_turns = RecentMap::default();
        self.request_turns = RecentMap::default();
        self.tool_turns = RecentMap::default();
        self.main_record_turn = None;
    }

    // Hooks carry prompt_id only after the first user input, and older Claude
    // versions omit it. Without it, match prompts in submission order, then
    // retain transcript identities so repeats cannot consume another turn.
    fn user_record_turn(&mut self, record: &Value) -> Option<Arc<String>> {
        let prompt_id = record.get("promptId").and_then(Value::as_str);
        let mut owner = prompt_id
            .and_then(|id| self.prompt_turns.get(id))
            .or_else(|| {
                record
                    .get("uuid")
                    .and_then(Value::as_str)
                    .and_then(|id| self.record_turns.get(id))
            })
            .cloned();
        if let Some(index) = self.pending_prompts.iter().position(|pending| {
            if let Some(owner) = &owner {
                &pending.turn_id == owner
            } else {
                record
                    .pointer("/message/content")
                    .is_some_and(|content| prompt_matches(&pending.content, content))
            }
        }) {
            owner = self
                .pending_prompts
                .remove(index)
                .map(|pending| pending.turn_id);
        }
        if let (Some(prompt_id), Some(owner)) = (prompt_id, &owner) {
            self.prompt_turns
                .insert(prompt_id.to_owned(), owner.clone());
        }
        owner
    }

    // Hooks may precede their native records by an entire execution. Resolve
    // each user record before propagating its owner through record ancestry.
    fn observe_record_turns(&mut self, records: &[Value], ops: &mut Vec<SpanOp>) {
        for record in records {
            let user_prompt = is_real_user_record(record)
                && record.get("queueTranscriptOnly").and_then(Value::as_bool) != Some(true);
            if user_prompt {
                self.main_record_turn = self.user_record_turn(record);
            }
            let owner = if user_prompt {
                self.main_record_turn.clone()
            } else {
                record
                    .get("parentUuid")
                    .and_then(Value::as_str)
                    .and_then(|id| self.record_turns.get(id))
                    .cloned()
                    .or_else(|| self.main_record_turn.clone())
            };
            let Some(owner) = owner else { continue };
            if user_prompt {
                if let Some(content) = record
                    .pointer("/message/content")
                    .filter(|content| crate::attachments::contains(content))
                {
                    let identity =
                        self.turn_identities
                            .get(owner.as_str())
                            .cloned()
                            .or_else(|| {
                                self.turn
                                    .as_ref()
                                    .filter(|turn| turn.id == owner.as_str())
                                    .map(|turn| (self.root_span_id.clone(), turn.parent_id.clone()))
                            });
                    if let Some((root_span_id, parent_id)) = identity {
                        ops.push(SpanOp::Merge(SpanRow {
                            span_id: owner.to_string(),
                            root_span_id,
                            parent_span_ids: vec![parent_id],
                            input: Some(content.clone()),
                            turn_span_id: self.owner_of(owner.as_str()),
                            ..Default::default()
                        }));
                    }
                }
            }

            if let Some(uuid) = record.get("uuid").and_then(Value::as_str) {
                self.record_turns.insert(uuid.to_owned(), owner.clone());
            }
            if let Some(request_id) = assistant_request_id(record) {
                self.request_turns.insert(request_id, owner.clone());
                if let Some(blocks) = record.pointer("/message/content").and_then(Value::as_array) {
                    for block in blocks {
                        if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                            if let Some(id) = block.get("id").and_then(Value::as_str) {
                                self.tool_turns.insert(id.to_owned(), owner.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    fn emit_parsed(
        &mut self,
        calls: Vec<LlmCall>,
        tools: Vec<TranscriptTool>,
        scope: &str,
        parent: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        for call in calls {
            if self.emitted_requests.insert(call.request_id.clone()) {
                let span_key = format!("{scope}:llm:{}", call.request_id);
                let parent = if scope == "main" {
                    self.request_turns
                        .get(&call.request_id)
                        .map(|id| id.as_str())
                        .unwrap_or(parent)
                } else {
                    parent
                };
                let mut row = call.into_row(
                    ids::span_id(&self.session_id, &span_key),
                    self.root_span_id.clone(),
                    parent.to_string(),
                );
                row.turn_span_id = self.owner_of(parent);
                ops.push(SpanOp::Insert(row));
            }
        }
        for tool in tools {
            if self.emitted_tools.contains(&tool.call_id) {
                continue;
            }
            // A tool_use row is not a terminal outcome. Leave a corresponding
            // live hook span pending so the explicit boundary can cancel it.
            if !tool.terminal && self.pending_tools.contains_key(&tool.call_id) {
                continue;
            }
            self.emitted_tools.insert(tool.call_id.clone());
            if let Some(pending) = self.pending_tools.remove(&tool.call_id) {
                ops.push(SpanOp::Merge(tool.into_update(
                    pending.span_id,
                    self.root_span_id.clone(),
                    pending.parent_id,
                )));
            } else {
                let span_key = format!("tool:{}", tool.call_id);
                let parent = if scope == "main" {
                    self.tool_turns
                        .get(&tool.call_id)
                        .map(|id| id.as_str())
                        .unwrap_or(parent)
                } else {
                    parent
                };
                let mut row = tool.into_row(
                    ids::span_id(&self.session_id, &span_key),
                    self.root_span_id.clone(),
                    parent.to_string(),
                );
                row.turn_span_id = self.owner_of(parent);
                ops.push(SpanOp::Insert(row));
            }
        }
    }

    fn flush_previous_turn_rows(&mut self, ops: &mut Vec<SpanOp>) {
        let (Some(path), Some(parent)) = (self.main_transcript.clone(), self.last_turn_id.clone())
        else {
            return;
        };
        let Some(cursor) = self.transcripts.get_mut(&path) else {
            return;
        };
        let split = cursor
            .buffered
            .iter()
            .rposition(is_real_user_record)
            .unwrap_or(cursor.buffered.len());
        let current_turn_rows = cursor.buffered.split_off(split);
        let previous_rows = std::mem::replace(&mut cursor.buffered, current_turn_rows);
        self.queue_transcript(
            previous_rows,
            PendingHistory::Main,
            "main".into(),
            parent,
            self.last_turn_cwd.clone(),
            ops,
        );
    }

    fn stop_turn(&mut self, event: &Envelope, error: Option<String>, ops: &mut Vec<SpanOp>) {
        let Some(turn_id) = self.turn.as_ref().map(|turn| turn.id.clone()) else {
            return;
        };
        let interrupted = error.as_deref().is_some_and(is_interrupt_reason);
        let outcome = if interrupted {
            "interrupted"
        } else if error.is_some() {
            "errored"
        } else {
            "completed"
        };
        self.session_status = Some(outcome.to_string());
        let error = (!interrupted).then_some(error).flatten();
        self.recover_pending_denials(&turn_id, event.ts_ms, ops);
        self.emit_main(&turn_id, ops);
        self.close_pending_tools(
            &turn_id,
            event.ts_ms,
            "Turn ended before tool completion",
            ops,
        );
        let Some(turn) = self.turn.take() else { return };
        let end_ms = self.execution_end(&turn, event.ts_ms, ops);
        let output = event
            .payload
            .get("last_assistant_message")
            .cloned()
            .or_else(|| event.payload.get("output").cloned());
        if output.as_ref().is_some_and(|output| !output.is_null()) {
            self.last_turn_output = output.clone();
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![turn.parent_id],
            end_ms: Some(end_ms),
            late_merge_key: Some(format!("execution:stop:{}", event.ts_ms)),
            output,
            error,
            metadata: Some(json!({"outcome": outcome})),
            ..Default::default()
        }));
        self.last_turn_cwd = turn.cwd;
        self.last_user_turn_id = turn.user_turn_id;
        self.last_turn_id = Some(turn_id);
        self.requested_skills.clear();
    }

    fn recover_pending_denials(&mut self, turn_id: &str, end_ms: i64, ops: &mut Vec<SpanOp>) {
        if self.pending_tools.is_empty() {
            return;
        }
        let Some(cursor) = self
            .main_transcript
            .as_ref()
            .and_then(|path| self.transcripts.get(path))
        else {
            return;
        };
        for record in &cursor.buffered {
            let Ok(row) = TranscriptEnvelope::deserialize(record) else {
                continue;
            };
            for result in row.tool_results() {
                let Some(call_id) = result.tool_use_id.as_deref() else {
                    continue;
                };
                if !self
                    .pending_tools
                    .get(call_id)
                    .is_some_and(|tool| tool.parent_id == turn_id)
                    || !matches!(
                        TranscriptToolOutcome::from_result(result, row.tool_use_result.as_ref()),
                        TranscriptToolOutcome::Denied
                    )
                {
                    continue;
                }
                let Some(tool) = self.pending_tools.remove(call_id) else {
                    continue;
                };
                self.pending_nested_spawns.remove(call_id);
                self.emitted_tools.insert(call_id.to_owned());
                let mut metadata = tool.metadata.as_object().cloned().unwrap_or_default();
                add_tool_approval(&mut metadata, Some(ToolApproval::Denied));
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: tool.span_id,
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![tool.parent_id],
                    end_ms: Some(parse_timestamp_ms(record).unwrap_or(end_ms)),
                    output: result.content().cloned(),
                    metadata: Some(Value::Object(metadata)),
                    ..Default::default()
                }));
            }
        }
    }

    fn close_pending_tools(
        &mut self,
        parent_id: &str,
        end_ms: i64,
        cancellation_reason: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        let ids: Vec<String> = self
            .pending_tools
            .iter()
            .filter(|(_, tool)| tool.parent_id == parent_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.pending_nested_spawns.remove(&id);
            if let Some(tool) = self.pending_tools.remove(&id) {
                self.emitted_tools.insert(id);
                let mut metadata = tool.metadata.as_object().cloned().unwrap_or_default();
                metadata.insert("cancelled".into(), json!(true));
                metadata.insert("cancellation_reason".into(), json!(cancellation_reason));
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: tool.span_id,
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![tool.parent_id],
                    end_ms: Some(end_ms),
                    metadata: Some(Value::Object(metadata)),
                    ..Default::default()
                }));
            }
        }
    }

    fn end_root(&mut self, end_ms: i64, ops: &mut Vec<SpanOp>) {
        let end_ms = end_ms.max(self.last_ts_ms);
        if !self.root_open || self.root_end_ms.is_some_and(|previous| previous >= end_ms) {
            return;
        }
        self.root_end_ms = Some(end_ms);
        ops.push(SpanOp::Merge(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: self.session_parent_span_ids.clone(),
            end_ms: Some(end_ms),
            output: self.last_turn_output.clone(),
            metadata: self
                .session_status
                .as_ref()
                .map(|status| json!({"session_status": status})),
            late_merge_key: Some(format!("session:stop:{end_ms}")),
            ..Default::default()
        }));
    }

    fn end_session(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        for agent in self.subagents.values_mut() {
            if let Some(row) = agent.deferred_insert.take() {
                ops.push(SpanOp::Insert(row));
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: agent.span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![agent.parent_span_id.clone()],
                    end_ms: Some(event.ts_ms),
                    ..Default::default()
                }));
            }
        }
        if self.turn.is_some() {
            self.session_status = Some("interrupted".into());
        } else if self.session_status.is_none() {
            self.session_status = Some("completed".into());
        }
        if let Some(turn_id) = self
            .turn
            .as_ref()
            .map(|turn| turn.id.clone())
            .or_else(|| self.last_turn_id.clone())
        {
            self.emit_main(&turn_id, ops);
        }
        if let Some(turn) = self.turn.take() {
            self.close_pending_tools(
                &turn.id,
                event.ts_ms,
                "Session ended before tool completion",
                ops,
            );
            let end_ms = self.execution_end(&turn, event.ts_ms, ops);
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![turn.parent_id],
                end_ms: Some(end_ms),
                late_merge_key: Some(format!("execution:stop:{}", event.ts_ms)),
                metadata: Some(json!({"outcome":"interrupted"})),
                ..Default::default()
            }));
            self.last_turn_id = Some(turn.id);
            self.last_user_turn_id = turn.user_turn_id;
        }
        self.end_root(event.ts_ms, ops);
        self.root_open = false;
        // A stopped live subagent may append after session end. Keep its
        // deduplication state until the observer is finalized.
        if !self.observe_stopped_subagents {
            if let Some(pending) = &mut self.pending_emission {
                pending.clear_after = true;
            } else {
                self.release_terminal_state();
            }
        }
    }
}

impl AgentTranslator for ClaudeTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts_ms = self.last_ts_ms.max(event.ts_ms);
        anyhow::ensure!(
            self.pending_emission.is_none(),
            "Claude translator has pending catch-up work; drain it before handling another event"
        );
        let mut ops = Vec::new();
        let hook = decode::<HookContext>(&event.payload).unwrap_or_default();
        if let Some(cwd) = &hook.cwd {
            self.current_cwd = Some(cwd.clone());
        }
        self.tail_main(event, &hook);
        self.observe_session_details(&hook);
        let subagent_stop = if event.event == "SubagentStop" {
            decode::<SubagentHook>(&event.payload)
        } else {
            None
        };
        if let Some(hook) = &subagent_stop {
            // Claude's internal agents (such as prompt suggestions) emit
            // unpaired stops with an empty type. They are not delegated tasks.
            // Keep known agents even if their stop omits the type, and recover
            // named agents whose start hook was missed.
            if hook.agent_type.is_none() && !self.subagents.contains_key(&hook.agent_id) {
                return Ok(ops);
            }
        }
        if Self::starts_trace(event) {
            self.ensure_root(event, ctx, &hook, &mut ops);
        }
        if self.root_open && !self.claude_version_logged {
            if let Some(version) = &self.claude_version {
                self.claude_version_logged = true;
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: self.session_span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: self.session_parent_span_ids.clone(),
                    metadata: Some(json!({ "claude_code_version": version })),
                    ..Default::default()
                }));
            }
        }
        self.record_turn_effort(&mut ops);
        // Root creation moved below the passive-hook gate. Enrich it before
        // handling the event so a later prompt can retain the prior turn's
        // cwd while flushing its deferred transcript rows.
        self.git.enrich_rows(self.current_cwd.as_deref(), &mut ops);
        let mut event_op_start = ops.len();
        match event.event.as_str() {
            "SessionStart" => {
                if event
                    .payload
                    .get("_bt_import_inherited_history")
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    if let Some(cursor) = self
                        .main_transcript
                        .as_ref()
                        .and_then(|path| self.transcripts.get_mut(path))
                    {
                        let records = std::mem::take(&mut cursor.buffered);
                        self.main_history = std::mem::take(&mut self.main_history)
                            .parse_records(&records)
                            .history;
                    }
                }
            }
            "UserPromptSubmit" => {
                self.flush_previous_turn_rows(&mut ops);
                event_op_start = ops.len();
                self.open_turn(event, &mut ops);
            }
            "UserPromptExpansion" => self.record_skill(event, &mut ops),
            "PreToolUse" => self.pre_tool(event, &mut ops),
            "PostToolUse" | "PostToolUseFailure" => {
                self.finish_tool(event, ToolApproval::Approved, &mut ops);
            }
            "PermissionDenied" => self.finish_tool(event, ToolApproval::Denied, &mut ops),
            "PostCompact" => {
                let turn_span_id = self
                    .turn
                    .as_ref()
                    .map(|turn| turn.id.clone())
                    .or_else(|| self.last_turn_id.clone());
                if let Some(parent) = &turn_span_id {
                    // PostCompact is the durable point at which Claude's
                    // synthetic compact-summary transcript record becomes the
                    // first message in the active history window.
                    self.emit_main(parent, &mut ops);
                }
                ops.push(SpanOp::Insert(SpanRow {
                    span_id: ids::span_id(&self.session_id, &format!("compaction:{}", event.ts_ms)),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![turn_span_id
                        .clone()
                        .unwrap_or_else(|| self.session_span_id.clone())],
                    turn_span_id,
                    name: "Compaction".into(),
                    span_type: SpanType::Task,
                    start_ms: Some(event.ts_ms),
                    end_ms: Some(event.ts_ms),
                    metadata: Some(json!({"event":"compaction"})),
                    tags: Some(vec!["compaction".into()]),
                    ..Default::default()
                }));
            }
            "SubagentStart" => {
                if let Some(hook) = decode::<SubagentHook>(&event.payload) {
                    self.ensure_subagent(&hook, event, &mut ops);
                }
            }
            "SubagentStop" => {
                if let Some(hook) = subagent_stop {
                    self.stop_subagent(event, hook, &mut ops);
                }
            }
            "TranscriptUpdate" if event.payload.get("agent_transcript_path").is_some() => {
                if let (Some(agent_id), Some(path)) = (
                    string_field(&event.payload, "agent_id"),
                    string_field(&event.payload, "agent_transcript_path"),
                ) {
                    let mut offset = 0;
                    let records = read_event_records(event, &path, &mut offset);
                    self.queue_transcript(
                        records,
                        PendingHistory::Owned(MessageHistory::default()),
                        format!("subagent:{agent_id}"),
                        ids::span_id(&self.session_id, &format!("subagent:{agent_id}")),
                        self.current_cwd.clone(),
                        &mut ops,
                    );
                }
            }
            "TranscriptUpdate" if self.turn.is_none() => {
                if let Some(parent) = self.last_turn_id.clone() {
                    self.emit_main(&parent, &mut ops);
                }
            }
            "Stop" => self.stop_turn(event, None, &mut ops),
            "StopFailure" => {
                self.stop_turn(
                    event,
                    ToolCompletionHook::deserialize(&event.payload)
                        .ok()
                        .and_then(|failure| failure.summary())
                        .or_else(|| Some("Claude Code turn failed".into())),
                    &mut ops,
                );
            }
            "SessionEnd" => self.end_session(event, &mut ops),
            _ => {}
        }
        self.git
            .enrich_rows(self.current_cwd.as_deref(), &mut ops[event_op_start..]);
        self.activity_bounds.extend_ancestors(&mut ops);
        Ok(ops)
    }

    fn drain_pending(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some(pending) = self.pending_emission.take() else {
            return Ok(None);
        };
        let mut ops = Vec::new();
        self.pending_emission = self.emit_next_pending(pending, &mut ops);
        self.activity_bounds.extend_ancestors(&mut ops);
        Ok(Some(ops))
    }

    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let end_ms = self.last_ts_ms;
        if self.turn.is_some() && self.session_status.as_deref() == Some("running") {
            self.session_status = Some("interrupted".into());
        }
        let mut ops = Vec::new();
        if let Some(parent) = self
            .turn
            .as_ref()
            .map(|turn| turn.id.clone())
            .or_else(|| self.last_turn_id.clone())
        {
            self.emit_main(&parent, &mut ops);
        }
        for (_, tool) in self.pending_tools.drain() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: tool.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![tool.parent_id],
                end_ms: Some(end_ms),
                ..Default::default()
            }));
        }
        if let Some(turn) = self.turn.take() {
            let turn_end_ms = self.execution_end(&turn, end_ms, &mut ops);
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![turn.parent_id],
                end_ms: Some(turn_end_ms),
                late_merge_key: Some(format!("execution:stop:{end_ms}")),
                metadata: Some(json!({
                    "incomplete": true,
                    "outcome": "interrupted",
                    "termination_reason": "session_finalized_before_turn_completion"
                })),
                ..Default::default()
            }));
        }
        for (agent_id, subagent) in std::mem::take(&mut self.subagents) {
            if let Some(row) = subagent.deferred_insert.clone() {
                ops.push(SpanOp::Insert(row));
            }
            if let Some(owner) = self
                .agent_origins
                .get(&agent_id)
                .and_then(|origin| origin.user_turn_id.clone())
            {
                self.extend_user_turn(&owner, end_ms, &mut ops);
            }
            ops.push(SpanOp::Merge(SpanRow {
                span_id: subagent.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![subagent.parent_span_id],
                end_ms: Some(end_ms),
                ..Default::default()
            }));
        }
        self.end_root(end_ms, &mut ops);
        self.root_open = false;
        if let Some(pending) = &mut self.pending_emission {
            pending.clear_after = true;
        } else {
            self.release_terminal_state();
        }
        self.activity_bounds.extend_ancestors(&mut ops);
        Ok(ops)
    }
}

fn transcript_segments(records: Vec<Value>) -> VecDeque<Vec<Value>> {
    let mut segments = VecDeque::new();
    let mut current = Vec::new();
    let mut request_id: Option<String> = None;
    for record in records {
        let next_request_id = assistant_request_id(&record);
        if next_request_id
            .as_ref()
            .zip(request_id.as_ref())
            .is_some_and(|(next, current)| next != current)
            && !current.is_empty()
        {
            segments.push_back(std::mem::take(&mut current));
        }
        if next_request_id.is_some() {
            request_id = next_request_id;
        }
        current.push(record);
    }
    if !current.is_empty() {
        segments.push_back(current);
    }
    segments
}

fn assistant_request_id(record: &Value) -> Option<String> {
    (decode::<TranscriptEnvelope>(record)
        .and_then(|record| record.kind)
        .as_deref()
        == Some("assistant"))
    .then(|| {
        record
            .get("message")
            .and_then(|message| string_field(message, "id"))
            .or_else(|| string_field(record, "requestId"))
            .or_else(|| string_field(record, "uuid"))
    })
    .flatten()
    .filter(|id| !id.is_empty())
}
/// Returns the most recent SubagentHandback report whose matching transcript
/// result proves that the tool completed successfully.
pub(crate) fn successful_subagent_handback_message(records: &[Value]) -> Option<Value> {
    MessageHistory::default()
        .parse_records(records)
        .tools
        .into_iter()
        .rev()
        .find(TranscriptTool::is_successful_handback)
        .and_then(|tool| tool.handback_message())
}

fn continuation_origin(payload: &Value) -> Option<(&'static str, &str)> {
    if let Some(origin) = payload.get("origin") {
        if origin.get("kind").and_then(Value::as_str) == Some("peer")
            && origin.get("handback").and_then(Value::as_bool) == Some(true)
        {
            let id = origin
                .get("senderTaskId")
                .or_else(|| origin.get("from"))?
                .as_str()?;
            return (!id.is_empty()).then_some(("agent_message", id));
        }
    }
    let prompt = UserPromptHook::deserialize(payload).ok()?.text()?.trim();
    if let Some(id) = task_notification_id(prompt) {
        return Some(("task_notification", id));
    }
    let body = prompt.strip_prefix("<agent-message from=\"")?;
    let (id, body) = body.split_once("\">")?;
    if id.is_empty() || id.contains(['"', '<', '>']) || !body.ends_with("</agent-message>") {
        return None;
    }
    Some(("agent_message", id))
}

pub(crate) fn task_notification_id(prompt: &str) -> Option<&str> {
    let body = prompt
        .trim()
        .strip_prefix("<task-notification>")?
        .strip_suffix("</task-notification>")?;
    let (_, rest) = body.split_once("<task-id>")?;
    let (id, _) = rest.split_once("</task-id>")?;
    let id = id.trim();
    (!id.is_empty()).then_some(id)
}

fn prompt_matches(prompt: &Value, content: &Value) -> bool {
    if prompt == content {
        return true;
    }
    let (Some(mut remaining), Some(blocks)) = (prompt.as_str(), content.as_array()) else {
        return false;
    };
    // Native hooks use a string, while transcripts may wrap the same prompt
    // in text blocks alongside images. Compare without allocating a joined copy.
    let mut has_text = false;
    for text in blocks.iter().filter_map(|block| {
        (block.get("type").and_then(Value::as_str) == Some("text"))
            .then(|| block.get("text").and_then(Value::as_str))
            .flatten()
    }) {
        has_text = true;
        let Some(rest) = remaining.strip_prefix(text) else {
            return false;
        };
        remaining = rest;
    }
    remaining.is_empty() && (has_text || crate::attachments::contains(content))
}

fn is_real_user_record(record: &Value) -> bool {
    let Some(envelope) = decode::<TranscriptEnvelope>(record) else {
        return false;
    };
    if envelope.kind.as_deref() != Some("user") {
        return false;
    }
    if envelope.is_compact_summary {
        return false;
    }
    !record
        .pointer("/message/content")
        .and_then(Value::as_array)
        .is_some_and(|blocks| {
            blocks
                .iter()
                .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
        })
}
/// Read newline-delimited records forward from `offset`, advancing it past
/// everything consumed.
///
/// `cutoff_ms` stops before the first record stamped after the hook that is
/// being handled, so replaying against a transcript that already contains the
/// finished session does not pull in future turns. `through` stops at a byte
/// offset — the transcript mirror's high-water mark, which bounds a replayed
/// event to exactly the bytes the live run saw.
fn read_buffered_bounded<R: std::io::Read>(
    reader: &mut std::io::BufReader<R>,
    offset: &mut u64,
    cutoff_ms: Option<i64>,
    through: Option<u64>,
) -> Vec<Value> {
    let mut records = Vec::new();
    let mut line = String::new();
    loop {
        if through.is_some_and(|through| *offset >= through) {
            break;
        }
        line.clear();
        let start = *offset;
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        if through.is_some_and(|through| start + read as u64 > through) {
            break;
        }
        let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
            // A capture can end halfway through an asynchronous append. Keep
            // the fragment unread so the next capture can complete the record.
            if !line.ends_with('\n') {
                break;
            }
            *offset += read as u64;
            continue;
        };
        if cutoff_ms.is_some_and(|cutoff| {
            parse_timestamp_ms(&value).is_some_and(|timestamp| timestamp > cutoff)
        }) {
            *offset = start;
            break;
        }
        *offset += read as u64;
        records.push(value);
    }
    records
}

fn read_file_bounded(
    path: &str,
    offset: &mut u64,
    cutoff_ms: Option<i64>,
    through: Option<u64>,
) -> Vec<Value> {
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    if *offset > len {
        *offset = 0;
    }
    if file.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    let through = through.map(|through| through.min(len));
    read_buffered_bounded(
        &mut std::io::BufReader::new(file),
        offset,
        cutoff_ms,
        through,
    )
}

fn read_snapshot_bounded(
    contents: &str,
    offset: &mut u64,
    cutoff_ms: Option<i64>,
    through: Option<u64>,
) -> Vec<Value> {
    let len = contents.len() as u64;
    if *offset > len {
        *offset = 0;
    }
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(contents.as_bytes()));
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    let through = through.map(|through| through.min(len));
    read_buffered_bounded(&mut reader, offset, cutoff_ms, through)
}

/// Where this event's transcript records should be read from, in preference
/// order: the daemon's own mirror, then an inline snapshot (older journals),
/// then the live path.
fn read_event_records(event: &Envelope, path: &str, offset: &mut u64) -> Vec<Value> {
    let Ok(fields) = TranscriptPayload::decode(&event.payload) else {
        return Vec::new();
    };
    read_records_for_payload(event.ts_ms, &fields, path, offset)
}

fn read_records_for_payload(
    event_ts_ms: i64,
    fields: &TranscriptPayload,
    path: &str,
    offset: &mut u64,
) -> Vec<Value> {
    let import_through_offset = fields.import_through_offset;
    if let Some(mirrors) = &fields.claude_mirrors {
        // This marker records even an absent/unreadable file. Never replace a
        // captured absence with bytes appended later to the mutable native path.
        let Some(reference) = mirrors.get(path) else {
            return Vec::new();
        };
        let Some((mirror, through)) = reference.captured_prefix() else {
            return Vec::new();
        };
        let through = if fields.transcript_path.as_deref() == Some(path) {
            import_through_offset.map_or(through, |offset| offset.min(through))
        } else {
            through
        };
        return read_file_bounded(mirror, offset, None, Some(through));
    }

    let mirror = fields.matching_mirror(path);
    if let Some(mirror) = mirror {
        if let Some((mirror_path, through)) = mirror.captured_prefix() {
            // The mirror is byte-identical to the source prefix, so offsets
            // are interchangeable between them. An import offset is the
            // tighter bound when present.
            let through = import_through_offset.unwrap_or(through);
            let cutoff = import_through_offset.is_none().then_some(event_ts_ms);
            if std::fs::metadata(mirror_path).is_ok() {
                return read_file_bounded(mirror_path, offset, cutoff, Some(through));
            }
        }
    }

    // Journals written before transcript mirroring carry the whole file.
    let snapshot = fields.snapshot_for(path);
    match (snapshot, import_through_offset) {
        (Some(contents), through @ Some(_)) => {
            read_snapshot_bounded(contents, offset, None, through)
        }
        (Some(contents), None) => read_snapshot_bounded(contents, offset, Some(event_ts_ms), None),
        (None, through @ Some(_)) => read_file_bounded(path, offset, None, through),
        (None, None) if fields.replay == Some(true) => Vec::new(),
        (None, None) => read_file_bounded(path, offset, Some(event_ts_ms), None),
    }
}

fn tool_metadata(
    event: &Envelope,
    tool_name: &str,
    call_id: &str,
    approval: Option<ToolApproval>,
    skill: Option<&SkillLoad>,
    requested_skills: &[String],
) -> Value {
    let mut metadata = Map::new();
    metadata.insert("tool_name".into(), json!(tool_name));
    add_tool_approval(&mut metadata, approval);
    metadata.insert("tool_call_id".into(), json!(call_id));
    for (target, direct, nested) in [
        ("permission_id", "permission_id", "/permission/id"),
        ("permission_type", "permission_type", "/permission/type"),
        ("permission_title", "permission_title", "/permission/title"),
    ] {
        if let Some(value) = string_field(&event.payload, direct).or_else(|| {
            event
                .payload
                .pointer(nested)
                .and_then(Value::as_str)
                .map(str::to_owned)
        }) {
            metadata.insert(target.into(), json!(value));
        }
    }
    if let Some(skill) = skill {
        skill.insert_metadata(&mut metadata, requested_skills);
    }
    Value::Object(metadata)
}

/// Claude loads a skill through the `Skill` tool, or by reading its `SKILL.md`
/// directly when a prompt or instruction points at the file.
fn skill_load(tool_name: &str, input: &Value) -> Option<SkillLoad> {
    let text = |key: &str| input.get(key).and_then(Value::as_str);
    match tool_name {
        "Skill" => Some(SkillLoad {
            name: ["skill", "name", "skill_name", "skillName"]
                .iter()
                .find_map(|key| text(key).filter(|name| !name.is_empty()))
                .map(normalize_skill_name),
            path: None,
        }),
        "Read" => skill_file_load(text("file_path").or_else(|| text("path"))?),
        "Bash" => skill_md_paths(text("command")?).next(),
        _ => None,
    }
}

fn raw_tool_input(payload: &Value) -> Option<&Value> {
    payload.get("tool_input").or_else(|| payload.get("input"))
}

fn tool_input(payload: &Value) -> Value {
    raw_tool_input(payload)
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn tool_name(payload: &Value) -> Option<String> {
    string_field(payload, "tool_name").or_else(|| string_field(payload, "tool"))
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn basename(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("workspace")
        .to_string()
}

fn normalize_skill_name(name: &str) -> String {
    name.trim()
        .trim_start_matches('/')
        .trim_end_matches([',', ')', '.', ';', ':'])
        .trim()
        .to_string()
}

fn skill_listing_contains(path: &str, name: &str) -> bool {
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .any(|line| {
            serde_json::from_str::<Value>(&line)
                .ok()
                .and_then(|row| {
                    row.pointer("/attachment/names")
                        .and_then(Value::as_array)
                        .cloned()
                })
                .is_some_and(|names| {
                    names
                        .iter()
                        .any(|candidate| candidate.as_str() == Some(name))
                })
        })
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| command_output("", "hostname", &[]))
        .unwrap_or_default()
}

fn command_output(cwd: &str, command: &str, args: &[&str]) -> Option<String> {
    let mut process = crate::subprocess::background_command(command);
    if !cwd.is_empty() {
        process.current_dir(cwd);
    }
    let output = process
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn tool_span_name(tool: &str, input: &Value, skill: Option<&SkillLoad>) -> String {
    if let Some(skill) = skill {
        return skill.display_name();
    }
    match tool {
        "Read" | "Write" | "Edit" | "MultiEdit" => string_field(input, "file_path")
            .or_else(|| string_field(input, "path"))
            .map(|path| format!("{tool}: {}", basename(&path)))
            .unwrap_or_else(|| tool.to_string()),
        "Bash" | "Terminal" => {
            let command = string_field(input, "command").unwrap_or_else(|| "command".into());
            format!("Terminal: {}", command.chars().take(50).collect::<String>())
        }
        name if name.starts_with("mcp__") => {
            format!(
                "MCP: {}",
                name.trim_start_matches("mcp__").replace("__", " - ")
            )
        }
        _ => tool.to_string(),
    }
}

fn is_interrupt_reason(reason: &str) -> bool {
    let reason = reason
        .trim()
        .trim_end_matches(['.', '!', '?'])
        .to_ascii_lowercase();
    matches!(
        reason.as_str(),
        "request interrupted by user"
            | "interrupted by user"
            | "user interrupted"
            | "request cancelled by user"
            | "request canceled by user"
            | "cancelled by user"
            | "canceled by user"
            | "user cancelled"
            | "user canceled"
            | "user aborted"
            | "aborted by user"
    )
}

fn turn_input_kind(payload: &Value) -> Option<&'static str> {
    let source = payload
        .get("prompt_source")
        .or_else(|| payload.get("promptSource"))
        .or_else(|| payload.get("input_kind"))
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    match source.as_deref() {
        Some("human" | "typed" | "user") => Some("human"),
        Some("quick_reply" | "quick-reply") => Some("quick_reply"),
        Some("task_notification" | "task-notification") => Some("task_notification"),
        Some("scheduled" | "scheduled_wake" | "scheduled-wake") => Some("scheduled_wake"),
        _ if payload
            .get("prompt")
            .and_then(Value::as_str)
            .is_some_and(|prompt| prompt.trim_start().starts_with("<task-notification")) =>
        {
            Some("task_notification")
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode, parse_rfc3339_timestamp, parse_timestamp_ms, read_snapshot_bounded,
        successful_subagent_handback_message, TranscriptEnvelope,
    };

    #[test]
    fn transcript_envelope_decodes_native_fixture_rows() {
        let contents = include_str!("../../tests/fixtures/claude/test-fixture/transcripts/4381b0d7-d67e-4187-bb2d-86a101d3b955.jsonl");
        let rows = contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            decode::<TranscriptEnvelope>(&rows[2]).and_then(|row| row.kind),
            Some("user".into())
        );
        assert_eq!(parse_timestamp_ms(&rows[6]), Some(1_779_843_127_934));
        let mut future_row = rows[6].clone();
        future_row["future_field"] = serde_json::json!({ "nested": true });
        assert_eq!(
            decode::<TranscriptEnvelope>(&future_row).and_then(|row| row.kind),
            Some("assistant".into())
        );
    }

    #[test]
    fn transcript_cutoff_is_independent_of_routing_schema_drift() {
        let future_record = serde_json::json!({
            "type": "user",
            "timestamp": "2026-01-01T00:00:01Z",
            "subtype": { "future": "object shape" },
            "isCompactSummary": null,
        });
        let contents = format!("{future_record}\n");
        let mut offset = 0;
        let records = read_snapshot_bounded(
            &contents,
            &mut offset,
            Some(parse_rfc3339_timestamp("2026-01-01T00:00:00Z").unwrap()),
            None,
        );

        assert!(records.is_empty());
        assert_eq!(offset, 0, "the future record remains unread");
    }

    #[test]
    fn transcript_partial_record_survives_the_next_capture() {
        let first = serde_json::json!({"type":"user","message":{"content":"hello"}});
        let final_row = serde_json::json!({
            "type":"assistant",
            "message":{"id":"final-request","content":[{"type":"text","text":"done"}]}
        });
        let prefix = format!("{first}\r\nmalformed complete row\r\n");
        let contents = format!("{prefix}{final_row}\r\n");
        let partial = &contents[..prefix.len() + 20];
        let mut offset = 0;
        assert_eq!(
            read_snapshot_bounded(partial, &mut offset, None, None),
            vec![first]
        );
        assert_eq!(
            read_snapshot_bounded(&contents, &mut offset, None, None),
            vec![final_row],
            "a row split across captures must not be permanently skipped"
        );
        assert!(read_snapshot_bounded(&contents, &mut offset, None, None).is_empty());
    }

    #[test]
    fn handback_requires_a_successful_matching_tool_result() {
        let assistant = serde_json::json!({
            "type": "assistant",
            "message": {"id": "request", "content": [{
                "type": "tool_use", "id": "handback", "name": "SubagentHandback",
                "input": {"message": "report"}
            }]}
        });
        let successful = serde_json::json!({
            "type": "user",
            "message": {"content": [{
                "type": "tool_result", "tool_use_id": "handback", "content": "Delivered"
            }]}
        });
        let failed = serde_json::json!({
            "type": "user",
            "message": {"content": [{
                "type": "tool_result", "tool_use_id": "handback", "content": "Failed",
                "is_error": true
            }]}
        });

        assert_eq!(
            successful_subagent_handback_message(&[assistant.clone(), successful]),
            Some(serde_json::json!("report"))
        );
        assert_eq!(
            successful_subagent_handback_message(&[assistant.clone(), failed]),
            None,
            "a failed handback proposal is not output delivered to the caller"
        );
        assert_eq!(
            successful_subagent_handback_message(&[assistant]),
            None,
            "an incomplete handback proposal is not output delivered to the caller"
        );
    }
}
