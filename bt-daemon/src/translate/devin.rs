mod hook_event;

use self::hook_event::{HookEvent, HookKind, ToolCall, ToolResponse};
use super::decode::decode;
use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::{agents::Devin, ids, wire::Envelope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

impl TranslatorFactory for Devin {
    fn create(&self, session_namespace: &str) -> Box<dyn AgentTranslator> {
        Box::new(DevinTranslator {
            namespace: session_namespace.to_owned(),
            root: ids::span_id(session_namespace, "root"),
            root_emitted: false,
            root_end_ms: None,
            turns: RecentMap::default(),
            started_spans: RecentSet::default(),
            finished_tools: RecentSet::default(),
            open: BTreeMap::new(),
            incomplete: RecentSet::default(),
            last_ts_ms: None,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct DevinTranslator {
    namespace: String,
    root: String,
    root_emitted: bool,
    #[serde(default)]
    root_end_ms: Option<i64>,
    turns: RecentMap<String, bool>,
    started_spans: RecentSet<String>,
    finished_tools: RecentSet<String>,
    #[serde(default, with = "super::state::rows")]
    open: BTreeMap<String, SpanRow>,
    #[serde(default)]
    incomplete: RecentSet<String>,
    #[serde(default)]
    last_ts_ms: Option<i64>,
}

impl DevinTranslator {
    fn row(&self, span_id: String, name: String) -> SpanRow {
        SpanRow {
            span_id,
            root_span_id: self.root.clone(),
            name,
            ..SpanRow::default()
        }
    }

    fn extend_root(&mut self, end_ms: i64) -> SpanOp {
        self.root_end_ms = Some(end_ms);
        let mut row = self.row(self.root.clone(), "Devin session".into());
        row.end_ms = Some(end_ms);
        SpanOp::Merge(row)
    }

    fn turn(&self, span_id: String) -> SpanRow {
        SpanRow {
            parent_span_ids: vec![self.root.clone()],
            turn_span_id: Some(span_id.clone()),
            is_turn: true,
            ..self.row(span_id, "turn".into())
        }
    }

    fn child(&self, span_id: String, name: String, turn: Option<&str>) -> SpanRow {
        SpanRow {
            parent_span_ids: vec![turn.unwrap_or(&self.root).to_owned()],
            turn_span_id: turn.map(str::to_owned),
            ..self.row(span_id, name)
        }
    }

    fn record_tool(
        &mut self,
        tool: ToolCall,
        response: Option<ToolResponse>,
        turn: Option<&str>,
        ts: i64,
    ) -> Option<SpanOp> {
        let id = tool.tool_use_id.filter(|id| !id.is_empty())?;
        let span_id = ids::span_id(&self.namespace, &format!("tool:{id}"));
        let mut row = self.child(span_id.clone(), tool.tool_name, turn);
        row.span_type = SpanType::Tool;
        row.input = tool.tool_input;

        match response {
            None if self.started_spans.insert(span_id.clone()) => {
                row.start_ms = Some(ts);
                Some(if self.finished_tools.contains(&span_id) {
                    SpanOp::Merge(row)
                } else {
                    self.track_open(&row);
                    SpanOp::Insert(row)
                })
            }
            Some(response) if self.finished_tools.insert(span_id.clone()) => {
                row.end_ms = Some(ts);
                row.output = response.output.map(Value::String);
                // A successful exec hook result may still contain a nonzero shell
                // exit code. Only report an error when Devin marks the tool failed.
                if response.success == Some(false) {
                    row.error = response.error.or_else(|| Some("Devin tool failed".into()));
                }
                self.record_completion(&mut row);
                Some(if self.started_spans.contains(&span_id) {
                    SpanOp::Merge(row)
                } else {
                    SpanOp::Insert(row)
                })
            }
            _ => None,
        }
    }

    fn track_open(&mut self, row: &SpanRow) {
        self.open.insert(
            row.span_id.clone(),
            SpanRow {
                span_id: row.span_id.clone(),
                root_span_id: row.root_span_id.clone(),
                parent_span_ids: row.parent_span_ids.clone(),
                span_type: row.span_type,
                turn_span_id: row.turn_span_id.clone(),
                ..SpanRow::default()
            },
        );
    }

    fn record_completion(&mut self, row: &mut SpanRow) {
        self.open.remove(&row.span_id);
        if self.incomplete.remove(&row.span_id) {
            row.metadata = Some(json!({"incomplete": false}));
        }
    }

    fn close_unfinished(&mut self, ts: i64) -> Vec<SpanOp> {
        std::mem::take(&mut self.open)
            .into_values()
            .map(|mut row| {
                self.incomplete.insert(row.span_id.clone());
                row.end_ms = Some(ts);
                row.metadata = Some(json!({"incomplete": true}));
                SpanOp::Merge(row)
            })
            .collect()
    }

    fn permission_request(
        &mut self,
        tool: ToolCall,
        turn: Option<&str>,
        ts: i64,
    ) -> Option<SpanOp> {
        let id = tool.tool_use_id.filter(|id| !id.is_empty())?;
        let span_id = ids::span_id(&self.namespace, &format!("permission:{id}"));
        if !self.started_spans.insert(span_id.clone()) {
            return None;
        }
        let mut row = self.child(
            span_id,
            format!("Permission request: {}", tool.tool_name),
            turn,
        );
        row.start_ms = Some(ts);
        row.end_ms = Some(ts);
        row.input = tool.tool_input;
        row.tags = Some(vec!["permission-request".into()]);
        Some(SpanOp::Insert(row))
    }
}

impl AgentTranslator for DevinTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let HookEvent {
            prompt_id,
            cwd,
            kind,
        } = decode("devin", &event.event, &event.payload)?;
        if matches!(kind, HookKind::Unknown) {
            return Ok(Vec::new());
        }

        let mut ops = Vec::new();
        let ts = event.ts_ms;
        self.last_ts_ms = Some(self.last_ts_ms.map_or(ts, |last| last.max(ts)));
        if !self.root_emitted {
            let mut row = self.row(self.root.clone(), "Devin session".into());
            row.start_ms = Some(ts);
            row.metadata = Some(match &cwd {
                Some(cwd) => json!({"session_id": ctx.session_id, "cwd": cwd}),
                None => json!({"session_id": ctx.session_id}),
            });
            ops.push(SpanOp::Insert(row));
            self.root_emitted = true;
        } else if self.root_end_ms.is_some_and(|end| ts > end)
            && !matches!(kind, HookKind::SessionEnd)
        {
            // Keep one root across resumes and extend its observed time range.
            ops.push(self.extend_root(ts));
        }

        let turn = prompt_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .map(|id| ids::span_id(&self.namespace, &format!("turn:{id}")));
        if let Some(span_id) = &turn {
            let submitted = self.turns.get(span_id).copied();
            let prompt = match &kind {
                HookKind::UserPromptSubmit { prompt } => Some(prompt),
                _ => None,
            };
            if submitted.is_none() || (submitted == Some(false) && prompt.is_some()) {
                self.turns.insert(span_id.clone(), prompt.is_some());
                let mut row = self.turn(span_id.clone());
                if let Some(prompt) = prompt {
                    row.start_ms = Some(ts);
                    row.input = Some(json!([{"role": "user", "content": prompt}]));
                }
                if submitted.is_none() {
                    self.track_open(&row);
                    ops.push(SpanOp::Insert(row));
                } else {
                    row.is_turn = false;
                    ops.push(SpanOp::Merge(row));
                }
            }
        }

        let op = match kind {
            HookKind::PreToolUse(tool) => self.record_tool(tool, None, turn.as_deref(), ts),
            HookKind::PostToolUse {
                tool,
                tool_response,
            } => self.record_tool(tool, Some(tool_response), turn.as_deref(), ts),
            HookKind::PermissionRequest(tool) => self.permission_request(tool, turn.as_deref(), ts),
            HookKind::Stop {
                last_assistant_message,
            } => turn.map(|id| {
                let mut row = self.turn(id);
                row.is_turn = false;
                row.end_ms = Some(ts);
                row.output = last_assistant_message
                    .map(|content| json!([{"role": "assistant", "content": content}]));
                self.record_completion(&mut row);
                SpanOp::Merge(row)
            }),
            HookKind::PostCompaction { summary } => {
                let id = ids::span_id(&self.namespace, &format!("compaction:{ts}"));
                let mut row = self.child(id, "Context compaction".into(), turn.as_deref());
                row.end_ms = Some(ts);
                row.output = summary.map(Value::String);
                row.tags = Some(vec!["compaction".into()]);
                Some(SpanOp::Insert(row))
            }
            HookKind::SessionEnd => {
                let end = self.last_ts_ms.unwrap_or(ts);
                ops.extend(self.close_unfinished(end));
                Some(self.extend_root(end))
            }
            HookKind::SessionStart | HookKind::UserPromptSubmit { .. } | HookKind::Unknown => None,
        };
        ops.extend(op);
        GitMetadataCache::shared().enrich_rows(cwd.as_deref(), &mut ops);
        Ok(ops)
    }

    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(self
            .last_ts_ms
            .map_or_else(Vec::new, |ts| self.close_unfinished(ts)))
    }
}

#[cfg(test)]
mod tests;
