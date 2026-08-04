//! Claude Code hook/transcript translator.
//!
//! Hook events own lifecycle and timing. Transcript rows supply model calls,
//! conversation history, usage, and a recovery path for tool calls whose hook
//! event was missed. Transcript cursors advance on every hook, but never past
//! the hook timestamp; this is essential when replaying a journal against a
//! transcript that already contains the completed session.

use super::git::GitMetadataCache;
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Seek, SeekFrom};
use std::process::Command;
use std::sync::Arc;

pub struct ClaudeTranslatorFactory {
    git: Arc<GitMetadataCache>,
}

impl ClaudeTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}

impl TranslatorFactory for ClaudeTranslatorFactory {
    fn source(&self) -> &str {
        "claude-code"
    }

    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(ClaudeTranslator::new(session_id, self.git.clone()))
    }
}

struct Turn {
    id: String,
    number: u32,
    cwd: Option<String>,
}

#[derive(Default)]
struct TranscriptCursor {
    offset: u64,
    buffered: Vec<Value>,
}

struct Subagent {
    span_id: String,
    transcript_path: Option<String>,
}

struct PendingTool {
    span_id: String,
    parent_id: String,
}

struct ClaudeTranslator {
    session_id: String,
    session_span_id: String,
    root_span_id: String,
    root_open: bool,
    root_ended: bool,
    turn: Option<Turn>,
    last_turn_id: Option<String>,
    turn_count: u32,
    tool_seq: u32,
    main_transcript: Option<String>,
    transcripts: HashMap<String, TranscriptCursor>,
    main_history: Vec<Value>,
    emitted_requests: HashSet<String>,
    emitted_tools: HashSet<String>,
    pending_tools: HashMap<String, PendingTool>,
    subagents: HashMap<String, Subagent>,
    pending_skills: Vec<String>,
    claude_version: Option<String>,
    claude_version_logged: bool,
    git: Arc<GitMetadataCache>,
    current_cwd: Option<String>,
    last_turn_cwd: Option<String>,
}

impl ClaudeTranslator {
    fn new(session_id: &str, git: Arc<GitMetadataCache>) -> Self {
        let root = ids::span_id(session_id, "root");
        Self {
            session_id: session_id.to_string(),
            session_span_id: root.clone(),
            root_span_id: root,
            root_open: false,
            root_ended: false,
            turn: None,
            last_turn_id: None,
            turn_count: 0,
            tool_seq: 0,
            main_transcript: None,
            transcripts: HashMap::new(),
            main_history: Vec::new(),
            emitted_requests: HashSet::new(),
            emitted_tools: HashSet::new(),
            pending_tools: HashMap::new(),
            subagents: HashMap::new(),
            pending_skills: Vec::new(),
            claude_version: None,
            claude_version_logged: false,
            git,
            current_cwd: None,
            last_turn_cwd: None,
        }
    }

    fn ensure_root(&mut self, event: &Envelope, ctx: &SessionCtx, ops: &mut Vec<SpanOp>) {
        if self.root_open {
            return;
        }
        self.root_open = true;
        let (parent_span_id, root_span_id) = ctx
            .config
            .as_ref()
            .map(|config| config.attached_span_ids())
            .unwrap_or_default();
        if let Some(root) = root_span_id {
            self.root_span_id = root.clone();
        }
        let cwd = string_field(&event.payload, "cwd").unwrap_or_default();
        let workspace = basename(&cwd);
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|c| c.additional_metadata.clone())
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        // Internal routing settings must never appear as user metadata.
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        metadata.insert("session_id".into(), json!(self.session_id));
        metadata.insert("workspace".into(), json!(cwd));
        metadata.insert("source".into(), json!("claude-code"));
        metadata.insert("hostname".into(), json!(hostname()));
        metadata.insert("username".into(), json!(username()));
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
        if let Some(source) = string_field(&event.payload, "source") {
            metadata.insert("session_source".into(), json!(source));
        }
        if let Some(model) = string_field(&event.payload, "model") {
            metadata.insert("model".into(), json!(model));
        }
        ops.push(SpanOp::Insert(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: parent_span_id.into_iter().collect(),
            name: format!("Claude Code: {workspace}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            input: Some(json!(format!("Session: {workspace}"))),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        }));
    }

    fn tail_main(&mut self, event: &Envelope) {
        if let Some(path) = string_field(&event.payload, "transcript_path") {
            self.main_transcript = Some(path);
        }
        let Some(path) = self.main_transcript.clone() else {
            return;
        };
        let cursor = self.transcripts.entry(path.clone()).or_default();
        let rows = read_event_records(event, &path, &mut cursor.offset);
        if self.claude_version.is_none() {
            self.claude_version = rows.iter().find_map(|row| string_field(row, "version"));
        }
        cursor.buffered.extend(rows);
    }

    fn open_turn(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        if let Some(old) = self.turn.take() {
            self.close_pending_tools(
                &old.id,
                event.ts_ms,
                "Turn ended before tool completion",
                ops,
            );
            ops.push(SpanOp::Merge(SpanRow {
                span_id: old.id,
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(event.ts_ms),
                ..Default::default()
            }));
        }
        self.turn_count += 1;
        let id = ids::span_id(&self.session_id, &format!("turn:{}", self.turn_count));
        let skill_metadata = explicit_skill_metadata(&self.pending_skills);
        self.pending_skills.clear();
        ops.push(SpanOp::Insert(SpanRow {
            span_id: id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![self.session_span_id.clone()],
            name: format!("Turn {}", self.turn_count),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            input: event.payload.get("prompt").cloned(),
            metadata: skill_metadata,
            ..Default::default()
        }));
        self.turn = Some(Turn {
            id,
            number: self.turn_count,
            cwd: self.current_cwd.clone(),
        });
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
        if !self.pending_skills.contains(&name) {
            self.pending_skills.push(name);
        }
        if let Some(turn) = &self.turn {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.id.clone(),
                root_span_id: self.root_span_id.clone(),
                metadata: explicit_skill_metadata(&self.pending_skills),
                ..Default::default()
            }));
        }
    }

    fn parent_for(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) -> Option<String> {
        if let Some(agent_id) = string_field(&event.payload, "agent_id") {
            return Some(self.ensure_subagent(&agent_id, event, ops));
        }
        self.turn.as_ref().map(|turn| turn.id.clone())
    }

    fn ensure_subagent(
        &mut self,
        agent_id: &str,
        event: &Envelope,
        ops: &mut Vec<SpanOp>,
    ) -> String {
        if let Some(agent) = self.subagents.get(agent_id) {
            return agent.span_id.clone();
        }
        let parent_id = self
            .turn
            .as_ref()
            .map(|turn| turn.id.clone())
            .or_else(|| self.last_turn_id.clone())
            .unwrap_or_else(|| self.session_span_id.clone());
        let agent_type =
            string_field(&event.payload, "agent_type").unwrap_or_else(|| "agent".into());
        let span_id = ids::span_id(&self.session_id, &format!("subagent:{agent_id}"));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent_id.clone()],
            name: format!("subagent: {agent_type}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            metadata: Some(json!({ "agent_id": agent_id, "agent_type": agent_type })),
            ..Default::default()
        }));
        self.subagents.insert(
            agent_id.to_string(),
            Subagent {
                span_id: span_id.clone(),
                transcript_path: None,
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
        let input = tool_input(&event.payload);
        let span_id = ids::span_id(&self.session_id, &format!("tool:{call_id}"));
        let metadata = tool_metadata(event, &tool_name, &call_id, "approved", &input);
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent_id.clone()],
            name: tool_span_name(&tool_name, &input),
            span_type: SpanType::Tool,
            start_ms: Some(event.ts_ms),
            input: Some(input.clone()),
            metadata: Some(metadata),
            ..Default::default()
        }));
        self.pending_tools
            .insert(call_id, PendingTool { span_id, parent_id });
    }

    fn finish_tool(
        &mut self,
        event: &Envelope,
        approval: &str,
        forced_error: Option<String>,
        ops: &mut Vec<SpanOp>,
    ) {
        let Some(tool_name) = tool_name(&event.payload) else {
            return;
        };
        let call_id = self.call_id(event);
        let input = tool_input(&event.payload);
        let output = tool_output(&event.payload);
        let error = forced_error.or_else(|| tool_error(&event.payload));
        let metadata = tool_metadata(event, &tool_name, &call_id, approval, &input);
        if let Some(pending) = self.pending_tools.remove(&call_id) {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: pending.span_id,
                root_span_id: self.root_span_id.clone(),
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
            let duration = event
                .payload
                .get("duration_ms")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            ops.push(SpanOp::Insert(SpanRow {
                span_id: ids::span_id(&self.session_id, &format!("tool:{call_id}")),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![parent_id],
                name: tool_span_name(&tool_name, &input),
                span_type: SpanType::Tool,
                start_ms: Some(event.ts_ms.saturating_sub(duration)),
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

    fn stop_subagent(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(agent_id) = string_field(&event.payload, "agent_id") else {
            return;
        };
        let parent = self.ensure_subagent(&agent_id, event, ops);
        let path = string_field(&event.payload, "agent_transcript_path");
        if let Some(agent) = self.subagents.get_mut(&agent_id) {
            agent.transcript_path = path.clone();
        }
        if let Some(path) = path {
            let cursor = self.transcripts.entry(path.clone()).or_default();
            cursor
                .buffered
                .extend(read_event_records(event, &path, &mut cursor.offset));
            let records = std::mem::take(&mut cursor.buffered);
            self.emit_transcript(&records, &format!("subagent:{agent_id}"), &parent, ops);
        }
        self.close_pending_tools(
            &parent,
            event.ts_ms,
            "Subagent ended before tool completion",
            ops,
        );
        ops.push(SpanOp::Merge(SpanRow {
            span_id: parent,
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(event.ts_ms),
            output: event.payload.get("last_assistant_message").cloned(),
            ..Default::default()
        }));
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
        let parsed = parse_transcript(&records, std::mem::take(&mut self.main_history));
        self.main_history = parsed.history.clone();
        self.emit_parsed(parsed, "main", parent, ops);
    }

    fn emit_transcript(
        &mut self,
        records: &[Value],
        scope: &str,
        parent: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        let parsed = parse_transcript(records, Vec::new());
        self.emit_parsed(parsed, scope, parent, ops);
    }

    fn emit_parsed(
        &mut self,
        parsed: ParsedTranscript,
        scope: &str,
        parent: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        for call in parsed.calls {
            let request_key = format!("{scope}:{}", call.request_id);
            if self.emitted_requests.insert(request_key.clone()) {
                let span_key = format!("{scope}:llm:{}", call.request_id);
                ops.push(SpanOp::Insert(call.into_row(
                    ids::span_id(&self.session_id, &span_key),
                    self.root_span_id.clone(),
                    parent.to_string(),
                )));
            }
        }
        for tool in parsed.tools {
            if self.emitted_tools.insert(tool.call_id.clone()) {
                let span_key = format!("tool:{}", tool.call_id);
                ops.push(SpanOp::Insert(tool.into_row(
                    ids::span_id(&self.session_id, &span_key),
                    self.root_span_id.clone(),
                    parent.to_string(),
                )));
            }
        }
    }

    fn flush_previous_turn_rows(&mut self, ops: &mut Vec<SpanOp>) {
        let op_start = ops.len();
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
        let parsed = parse_transcript(&previous_rows, std::mem::take(&mut self.main_history));
        self.main_history = parsed.history.clone();
        self.emit_parsed(parsed, "main", &parent, ops);
        self.git
            .enrich_rows(self.last_turn_cwd.as_deref(), &mut ops[op_start..]);
    }

    fn stop_turn(&mut self, event: &Envelope, error: Option<String>, ops: &mut Vec<SpanOp>) {
        let Some(turn_id) = self.turn.as_ref().map(|turn| turn.id.clone()) else {
            return;
        };
        self.emit_main(&turn_id, ops);
        self.close_pending_tools(
            &turn_id,
            event.ts_ms,
            "Turn ended before tool completion",
            ops,
        );
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn_id.clone(),
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(event.ts_ms),
            output: event
                .payload
                .get("last_assistant_message")
                .cloned()
                .or_else(|| event.payload.get("output").cloned()),
            error,
            ..Default::default()
        }));
        self.last_turn_cwd = self.turn.as_ref().and_then(|turn| turn.cwd.clone());
        self.last_turn_id = Some(turn_id);
        self.turn = None;
        self.pending_skills.clear();
    }

    fn close_pending_tools(
        &mut self,
        parent_id: &str,
        end_ms: i64,
        error: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        let ids: Vec<String> = self
            .pending_tools
            .iter()
            .filter(|(_, tool)| tool.parent_id == parent_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(tool) = self.pending_tools.remove(&id) {
                self.emitted_tools.insert(id);
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: tool.span_id,
                    root_span_id: self.root_span_id.clone(),
                    end_ms: Some(end_ms),
                    error: Some(error.to_string()),
                    ..Default::default()
                }));
            }
        }
    }

    fn end_session(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
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
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.id.clone(),
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(event.ts_ms),
                ..Default::default()
            }));
            self.last_turn_id = Some(turn.id);
        }
        if self.root_open && !self.root_ended {
            self.root_ended = true;
            ops.push(SpanOp::Merge(SpanRow {
                span_id: self.session_span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(event.ts_ms),
                ..Default::default()
            }));
        }
    }
}

impl AgentTranslator for ClaudeTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let mut ops = Vec::new();
        if let Some(cwd) = string_field(&event.payload, "cwd") {
            self.current_cwd = Some(cwd);
        }
        self.tail_main(event);
        self.ensure_root(event, ctx, &mut ops);
        if !self.claude_version_logged {
            if let Some(version) = &self.claude_version {
                self.claude_version_logged = true;
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: self.session_span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    metadata: Some(json!({ "claude_code_version": version })),
                    ..Default::default()
                }));
            }
        }
        self.git.enrich_rows(self.current_cwd.as_deref(), &mut ops);
        let mut event_op_start = ops.len();
        match event.event.as_str() {
            "SessionStart" => {}
            "UserPromptSubmit" => {
                self.flush_previous_turn_rows(&mut ops);
                event_op_start = ops.len();
                self.open_turn(event, &mut ops);
            }
            "UserPromptExpansion" => self.record_skill(event, &mut ops),
            "PreToolUse" => self.pre_tool(event, &mut ops),
            "PostToolUse" => self.finish_tool(event, "approved", None, &mut ops),
            "PostToolUseFailure" => self.finish_tool(
                event,
                "approved",
                tool_error(&event.payload)
                    .or_else(|| {
                        event
                            .payload
                            .pointer("/tool_response/output")
                            .and_then(Value::as_str)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned)
                    })
                    .or_else(|| Some("Tool execution failed".into())),
                &mut ops,
            ),
            "PermissionDenied" => self.finish_tool(event, "denied", None, &mut ops),
            "SubagentStart" => {
                if let Some(agent_id) = string_field(&event.payload, "agent_id") {
                    self.ensure_subagent(&agent_id, event, &mut ops);
                }
            }
            "SubagentStop" => self.stop_subagent(event, &mut ops),
            "Stop" => self.stop_turn(event, None, &mut ops),
            "StopFailure" => self.stop_turn(
                event,
                tool_error(&event.payload).or_else(|| Some("Claude Code turn failed".into())),
                &mut ops,
            ),
            "SessionEnd" => self.end_session(event, &mut ops),
            _ => {}
        }
        self.git
            .enrich_rows(self.current_cwd.as_deref(), &mut ops[event_op_start..]);
        Ok(ops)
    }

    fn flush(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(Vec::new())
    }
}

struct ParsedTranscript {
    calls: Vec<LlmCall>,
    tools: Vec<TranscriptTool>,
    history: Vec<Value>,
}

fn parse_transcript(records: &[Value], mut history: Vec<Value>) -> ParsedTranscript {
    let mut calls = Vec::<LlmCall>::new();
    let mut call_indexes = HashMap::<String, usize>::new();
    let mut assistant_history_indexes = HashMap::<String, usize>::new();
    let mut tools = HashMap::<String, TranscriptTool>::new();
    let mut tool_order = Vec::<String>::new();

    for record in records {
        match record.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let request_id = record
                    .get("message")
                    .and_then(|message| string_field(message, "id"))
                    .or_else(|| string_field(record, "requestId"))
                    .or_else(|| string_field(record, "uuid"))
                    .unwrap_or_default();
                if request_id.is_empty() {
                    continue;
                }
                let index = *call_indexes.entry(request_id.clone()).or_insert_with(|| {
                    let index = calls.len();
                    calls.push(LlmCall::new(
                        request_id.clone(),
                        parse_timestamp_ms(record).unwrap_or(0),
                        history.clone(),
                    ));
                    index
                });
                calls[index].observe(record);
                let output = calls[index].output_message();
                if let Some(history_index) = assistant_history_indexes.get(&request_id) {
                    history[*history_index] = output;
                } else {
                    assistant_history_indexes.insert(request_id.clone(), history.len());
                    history.push(output);
                }
                if let Some(content) = record.pointer("/message/content").and_then(Value::as_array)
                {
                    for block in content {
                        if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                            let Some(call_id) = string_field(block, "id") else {
                                continue;
                            };
                            if !tools.contains_key(&call_id) {
                                tool_order.push(call_id.clone());
                                tools.insert(
                                    call_id.clone(),
                                    TranscriptTool {
                                        call_id,
                                        tool_name: string_field(block, "name")
                                            .unwrap_or_else(|| "Tool".into()),
                                        input: block
                                            .get("input")
                                            .cloned()
                                            .unwrap_or_else(|| json!({})),
                                        output: None,
                                        error: None,
                                        start_ms: parse_timestamp_ms(record).unwrap_or(0),
                                        end_ms: parse_timestamp_ms(record).unwrap_or(0),
                                    },
                                );
                            }
                        }
                    }
                }
            }
            Some("user") => {
                let content = record
                    .pointer("/message/content")
                    .cloned()
                    .unwrap_or(Value::Null);
                if let Some(blocks) = content.as_array() {
                    let mut had_tool_result = false;
                    for block in blocks {
                        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                            continue;
                        }
                        had_tool_result = true;
                        let call_id = string_field(block, "tool_use_id").unwrap_or_default();
                        let result = block.get("content").cloned().unwrap_or(Value::Null);
                        history.push(json!({
                            "role": "tool",
                            "tool_call_id": call_id,
                            "content": result
                        }));
                        if let Some(tool) = tools.get_mut(&call_id) {
                            tool.output = Some(result);
                            tool.end_ms = parse_timestamp_ms(record).unwrap_or(tool.start_ms);
                            if block
                                .get("is_error")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                            {
                                tool.error = Some("Tool execution failed".into());
                            }
                        }
                    }
                    if !had_tool_result {
                        history.push(json!({ "role": "user", "content": content }));
                    }
                } else if !content.is_null() {
                    history.push(json!({ "role": "user", "content": content }));
                }
            }
            _ => {}
        }
    }
    ParsedTranscript {
        calls,
        tools: tool_order
            .into_iter()
            .filter_map(|id| tools.remove(&id))
            .collect(),
        history,
    }
}

fn is_real_user_record(record: &Value) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("user") {
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

struct LlmCall {
    request_id: String,
    model: String,
    start_ms: i64,
    end_ms: i64,
    input: Vec<Value>,
    text: Vec<String>,
    tool_calls: Vec<Value>,
    prompt_tokens: u64,
    completion_tokens: u64,
    cache_creation_tokens: u64,
    cache_creation_5m_tokens: u64,
    cache_creation_1h_tokens: u64,
    cache_read_tokens: u64,
    error: Option<String>,
    api_error_status: Option<u64>,
}

impl LlmCall {
    fn new(request_id: String, start_ms: i64, input: Vec<Value>) -> Self {
        Self {
            request_id,
            model: "claude".into(),
            start_ms,
            end_ms: start_ms,
            input,
            text: Vec::new(),
            tool_calls: Vec::new(),
            prompt_tokens: 0,
            completion_tokens: 0,
            cache_creation_tokens: 0,
            cache_creation_5m_tokens: 0,
            cache_creation_1h_tokens: 0,
            cache_read_tokens: 0,
            error: None,
            api_error_status: None,
        }
    }

    fn observe(&mut self, record: &Value) {
        self.end_ms = parse_timestamp_ms(record).unwrap_or(self.end_ms);
        if record
            .get("isApiErrorMessage")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            self.error = string_field(record, "error")
                .or_else(|| {
                    record
                        .pointer("/message/content")
                        .and_then(Value::as_array)
                        .and_then(|content| {
                            content.iter().find_map(|block| {
                                (block.get("type").and_then(Value::as_str) == Some("text"))
                                    .then(|| block.get("text").and_then(Value::as_str))
                                    .flatten()
                            })
                        })
                        .map(str::to_owned)
                })
                .or_else(|| Some("Claude API error".into()));
            self.api_error_status = record.get("apiErrorStatus").and_then(Value::as_u64);
        }
        let Some(message) = record.get("message") else {
            return;
        };
        if let Some(model) = string_field(message, "model") {
            self.model = model;
        }
        if let Some(content) = message.get("content").and_then(Value::as_array) {
            for block in content {
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            if !self.text.iter().any(|seen| seen == text) {
                                self.text.push(text.to_string());
                            }
                        }
                    }
                    Some("tool_use") => {
                        let arguments =
                            serde_json::to_string(block.get("input").unwrap_or(&Value::Null))
                                .unwrap_or_else(|_| "{}".into());
                        let call = json!({
                            "id": block.get("id").cloned().unwrap_or(Value::Null),
                            "type": "function",
                            "function": {
                                "name": block.get("name").cloned().unwrap_or(Value::Null),
                                "arguments": arguments
                            }
                        });
                        if !self.tool_calls.contains(&call) {
                            self.tool_calls.push(call);
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(usage) = message.get("usage") {
            self.prompt_tokens = self.prompt_tokens.max(u64_field(usage, "input_tokens"));
            self.completion_tokens = self
                .completion_tokens
                .max(u64_field(usage, "output_tokens"));
            self.cache_creation_tokens = self
                .cache_creation_tokens
                .max(u64_field(usage, "cache_creation_input_tokens"));
            self.cache_read_tokens = self
                .cache_read_tokens
                .max(u64_field(usage, "cache_read_input_tokens"));
            if let Some(cache) = usage.get("cache_creation") {
                self.cache_creation_5m_tokens = self
                    .cache_creation_5m_tokens
                    .max(u64_field(cache, "ephemeral_5m_input_tokens"));
                self.cache_creation_1h_tokens = self
                    .cache_creation_1h_tokens
                    .max(u64_field(cache, "ephemeral_1h_input_tokens"));
            }
        }
    }

    fn output_message(&self) -> Value {
        let content = self.text.join("\n");
        if self.tool_calls.is_empty() {
            json!({ "role": "assistant", "content": content })
        } else {
            json!({ "role": "assistant", "content": content, "tool_calls": self.tool_calls })
        }
    }

    fn into_row(self, span_id: String, root_span_id: String, parent: String) -> SpanRow {
        let has_split = self.cache_creation_5m_tokens > 0 || self.cache_creation_1h_tokens > 0;
        let creation = if has_split {
            self.cache_creation_5m_tokens + self.cache_creation_1h_tokens
        } else {
            self.cache_creation_tokens
        };
        let prompt = self.prompt_tokens + self.cache_read_tokens + creation;
        let mut metrics = Map::new();
        metrics.insert("prompt_tokens".into(), json!(prompt));
        metrics.insert("completion_tokens".into(), json!(self.completion_tokens));
        metrics.insert("tokens".into(), json!(prompt + self.completion_tokens));
        metrics.insert("prompt_cached_tokens".into(), json!(self.cache_read_tokens));
        if has_split {
            metrics.insert(
                "prompt_cache_creation_5m_tokens".into(),
                json!(self.cache_creation_5m_tokens),
            );
            metrics.insert(
                "prompt_cache_creation_1h_tokens".into(),
                json!(self.cache_creation_1h_tokens),
            );
        } else {
            metrics.insert(
                "prompt_cache_creation_tokens".into(),
                json!(self.cache_creation_tokens),
            );
        }
        let output = self.output_message();
        let mut metadata = Map::new();
        metadata.insert("model".into(), json!(self.model));
        metadata.insert("request_id".into(), json!(self.request_id));
        if let Some(status) = self.api_error_status {
            metadata.insert("api_error_status".into(), json!(status));
        }
        SpanRow {
            span_id,
            root_span_id,
            parent_span_ids: vec![parent],
            name: self.model.clone(),
            span_type: SpanType::Llm,
            start_ms: Some(self.start_ms),
            end_ms: Some(self.end_ms),
            input: Some(Value::Array(self.input)),
            output: Some(output),
            metadata: Some(Value::Object(metadata)),
            metrics: Some(Value::Object(metrics)),
            error: self.error,
            ..Default::default()
        }
    }
}

struct TranscriptTool {
    call_id: String,
    tool_name: String,
    input: Value,
    output: Option<Value>,
    error: Option<String>,
    start_ms: i64,
    end_ms: i64,
}

impl TranscriptTool {
    fn into_row(self, span_id: String, root_span_id: String, parent: String) -> SpanRow {
        SpanRow {
            span_id,
            root_span_id,
            parent_span_ids: vec![parent],
            name: tool_span_name(&self.tool_name, &self.input),
            span_type: SpanType::Tool,
            start_ms: Some(self.start_ms),
            end_ms: Some(self.end_ms),
            input: Some(self.input),
            output: self.output,
            metadata: Some(json!({
                "tool_name": self.tool_name,
                "tool_approval": "approved",
                "tool_call_id": self.call_id,
                "recovered_from_transcript": true
            })),
            error: self.error,
            ..Default::default()
        }
    }
}

fn read_records_until(path: &str, offset: &mut u64, cutoff_ms: i64) -> Vec<Value> {
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if *offset > len {
        *offset = 0;
    }
    if file.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    let mut reader = std::io::BufReader::new(file);
    read_buffered_until(&mut reader, offset, cutoff_ms)
}

fn read_buffered_until<R: std::io::Read>(
    reader: &mut std::io::BufReader<R>,
    offset: &mut u64,
    cutoff_ms: i64,
) -> Vec<Value> {
    let mut records = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        let start = *offset;
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
            *offset += read as u64;
            continue;
        };
        if parse_timestamp_ms(&value).is_some_and(|timestamp| timestamp > cutoff_ms) {
            *offset = start;
            break;
        }
        *offset += read as u64;
        records.push(value);
    }
    records
}

fn read_event_records(event: &Envelope, path: &str, offset: &mut u64) -> Vec<Value> {
    let import_through_offset = event
        .payload
        .get("_bt_import_through_offset")
        .and_then(Value::as_u64);
    let snapshot = event
        .payload
        .get("_bt_transcript_snapshot")
        .filter(|snapshot| snapshot.get("path").and_then(Value::as_str) == Some(path))
        .and_then(|snapshot| snapshot.get("contents"))
        .and_then(Value::as_str);
    match (snapshot, import_through_offset) {
        (Some(contents), Some(through)) => read_snapshot_through_offset(contents, offset, through),
        (None, Some(through)) => read_records_through_offset(path, offset, through),
        (Some(contents), None) => read_snapshot_until(contents, offset, event.ts_ms),
        (None, None) => read_records_until(path, offset, event.ts_ms),
    }
}

fn read_records_through_offset(path: &str, offset: &mut u64, through: u64) -> Vec<Value> {
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
    read_buffered_through_offset(&mut std::io::BufReader::new(file), offset, through.min(len))
}

fn read_snapshot_through_offset(contents: &str, offset: &mut u64, through: u64) -> Vec<Value> {
    if *offset > contents.len() as u64 {
        *offset = 0;
    }
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(contents.as_bytes()));
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    read_buffered_through_offset(&mut reader, offset, through.min(contents.len() as u64))
}

fn read_buffered_through_offset<R: std::io::Read>(
    reader: &mut std::io::BufReader<R>,
    offset: &mut u64,
    through: u64,
) -> Vec<Value> {
    let mut records = Vec::new();
    let mut line = String::new();
    while *offset < through {
        line.clear();
        let start = *offset;
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 || start + read as u64 > through {
            break;
        }
        *offset += read as u64;
        if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
            records.push(value);
        }
    }
    records
}

fn read_snapshot_until(contents: &str, offset: &mut u64, cutoff_ms: i64) -> Vec<Value> {
    if *offset > contents.len() as u64 {
        *offset = 0;
    }
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(contents.as_bytes()));
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    read_buffered_until(&mut reader, offset, cutoff_ms)
}

fn tool_metadata(
    event: &Envelope,
    tool_name: &str,
    call_id: &str,
    approval: &str,
    input: &Value,
) -> Value {
    let mut metadata = Map::new();
    metadata.insert("tool_name".into(), json!(tool_name));
    metadata.insert("tool_approval".into(), json!(approval));
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
    if tool_name == "Skill" {
        let skill_name = ["name", "skill", "skill_name", "skillName"]
            .iter()
            .find_map(|key| string_field(input, key));
        metadata.insert("tool_kind".into(), json!("skill"));
        metadata.insert("skill_name".into(), json!(skill_name));
        metadata.insert("skill_load_trigger".into(), json!("explicit"));
    }
    Value::Object(metadata)
}

fn tool_input(payload: &Value) -> Value {
    payload
        .get("tool_input")
        .or_else(|| payload.get("input"))
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn tool_output(payload: &Value) -> Option<Value> {
    payload
        .get("tool_response")
        .or_else(|| payload.get("output"))
        .cloned()
        .or_else(|| payload.pointer("/tool_response/output").cloned())
}

fn tool_name(payload: &Value) -> Option<String> {
    string_field(payload, "tool_name").or_else(|| string_field(payload, "tool"))
}

fn parse_timestamp_ms(value: &Value) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value.get("timestamp")?.as_str()?)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn u64_field(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
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

fn explicit_skill_metadata(names: &[String]) -> Option<Value> {
    (!names.is_empty()).then(|| {
        json!({
            "loaded_skill_names": names,
            "loaded_skills": names.iter().map(|name| json!({ "name": name })).collect::<Vec<_>>()
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

fn username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default()
}

fn command_output(cwd: &str, command: &str, args: &[&str]) -> Option<String> {
    let mut process = Command::new(command);
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

fn tool_span_name(tool: &str, input: &Value) -> String {
    match tool {
        "Skill" => string_field(input, "name")
            .or_else(|| string_field(input, "skill"))
            .map(|name| format!("skill: {name}"))
            .unwrap_or_else(|| "skill".into()),
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

fn tool_error(payload: &Value) -> Option<String> {
    for value in [
        payload.get("error"),
        payload.get("message"),
        payload.pointer("/tool_response/error"),
        payload.pointer("/tool_response/stderr"),
        payload.pointer("/tool_response/message"),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(text) = value.as_str().filter(|value| !value.is_empty()) {
            return Some(text.lines().next().unwrap_or(text).to_string());
        }
    }
    let response = payload.get("tool_response")?;
    let failed = response
        .get("interrupted")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || response
            .get("is_error")
            .or_else(|| response.get("isError"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || response
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "error" | "failed"));
    failed.then(|| "Tool execution failed".to_string())
}
