mod hook_event;

use self::hook_event::{HookEvent, HookKind, ToolCall, ToolResponse};
use super::decode::decode;
use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::{agents::Devin, ids, wire::Envelope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

impl TranslatorFactory for Devin {
    fn create(&self, session_namespace: &str) -> Box<dyn AgentTranslator> {
        Box::new(DevinTranslator {
            namespace: session_namespace.to_owned(),
            root: ids::span_id(session_namespace, "root"),
            root_emitted: false,
            turns: RecentMap::default(),
            started_spans: RecentSet::default(),
            finished_tools: RecentSet::default(),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct DevinTranslator {
    namespace: String,
    root: String,
    root_emitted: bool,
    turns: RecentMap<String, bool>,
    started_spans: RecentSet<String>,
    finished_tools: RecentSet<String>,
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
                    SpanOp::Insert(row)
                })
            }
            Some(response) if self.finished_tools.insert(span_id.clone()) => {
                row.end_ms = Some(ts);
                row.output = response.output.map(Value::String);
                // A successful exec hook result may still contain a nonzero shell
                // exit code. Only report an error when Devin marks the tool failed.
                if !response.success {
                    row.error = response.error.or_else(|| Some("Devin tool failed".into()));
                }
                Some(if self.started_spans.contains(&span_id) {
                    SpanOp::Merge(row)
                } else {
                    SpanOp::Insert(row)
                })
            }
            _ => None,
        }
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
        if !self.root_emitted {
            let mut row = self.row(self.root.clone(), "Devin session".into());
            row.start_ms = Some(ts);
            row.metadata = Some(match &cwd {
                Some(cwd) => json!({"session_id": ctx.session_id, "cwd": cwd}),
                None => json!({"session_id": ctx.session_id}),
            });
            ops.push(SpanOp::Insert(row));
            self.root_emitted = true;
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
                let mut row = self.row(self.root.clone(), "Devin session".into());
                row.end_ms = Some(ts);
                Some(SpanOp::Merge(row))
            }
            HookKind::SessionStart | HookKind::UserPromptSubmit { .. } | HookKind::Unknown => None,
        };
        ops.extend(op);
        GitMetadataCache::shared().enrich_rows(cwd.as_deref(), &mut ops);
        Ok(ops)
    }
}

#[cfg(test)]
mod tests;
