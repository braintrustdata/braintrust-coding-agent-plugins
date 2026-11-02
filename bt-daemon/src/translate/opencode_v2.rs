//! OpenCode 2 event translation. The v2 event stream has a different schema
//! from v1; this adapter keeps its correlation state separate while sharing the
//! existing span construction and history logic.

use super::{opencode, recent::RecentSet, AgentTranslator, SessionCtx, SpanOp};
use crate::wire::Envelope;
use serde_json::{json, Value};
use std::collections::HashMap;

pub(super) struct OpenCodeV2Translator {
    spans: Box<dyn AgentTranslator>,
    messages: HashMap<(String, String), (i64, Value)>,
    pending_inputs: HashMap<(String, String), Value>,
    tool_names: HashMap<(String, String), String>,
    seen: RecentSet<String>,
}

impl OpenCodeV2Translator {
    pub(super) fn new(session_id: &str) -> Self {
        Self {
            spans: opencode::new_translator(session_id),
            messages: HashMap::new(),
            pending_inputs: HashMap::new(),
            tool_names: HashMap::new(),
            seen: RecentSet::default(),
        }
    }

    fn emit(
        &mut self,
        original: &Envelope,
        ctx: &SessionCtx,
        name: &str,
        mut payload: Value,
    ) -> anyhow::Result<Vec<SpanOp>> {
        let mut event = original.clone();
        event.event = name.to_string();
        if let (Some(directory), Some(object)) = (
            original.payload.pointer("/location/directory"),
            payload.as_object_mut(),
        ) {
            object.insert("directory".into(), directory.clone());
        }
        event.payload = payload;
        self.spans.handle(&event, ctx)
    }
}

impl AgentTranslator for OpenCodeV2Translator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let Some(data) = event.payload.get("data") else {
            return Ok(Vec::new());
        };
        let Some(sid) = data.get("sessionID").and_then(Value::as_str) else {
            return Ok(Vec::new());
        };
        if let Some(id) = event.payload.get("id").and_then(Value::as_str) {
            if !self.seen.insert(id.to_string()) {
                return Ok(Vec::new());
            }
        }
        let mid = data
            .get("assistantMessageID")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match event.event.as_str() {
            "session.created" => self.emit(event, ctx, "session.created", json!({"properties":{"info":{"id":sid,"parentID":data.get("parentID"),"title":data.get("title")}}})),
            "session.inbox.enqueued" if data.pointer("/item/type").and_then(Value::as_str) == Some("user") => {
                if let Some(id) = data.get("inboxID").and_then(Value::as_str) {
                    self.pending_inputs.insert((sid.into(), id.into()), data.pointer("/item/payload").cloned().unwrap_or(Value::Null));
                }
                Ok(Vec::new())
            }
            "session.inbox.delivered" => {
                let Some(id) = data.get("inboxID").and_then(Value::as_str) else { return Ok(Vec::new()); };
                let Some(input) = self.pending_inputs.remove(&(sid.into(), id.into())) else { return Ok(Vec::new()); };
                let text = input.get("text").and_then(Value::as_str).unwrap_or_default();
                self.emit(event, ctx, "chat.message", json!({"input":{"sessionID":sid},"output":{"message":{"id":id},"parts":[{"type":"text","text":text}]}}))
            }
            "session.inbox.cancelled" => {
                if let Some(id) = data.get("inboxID").and_then(Value::as_str) { self.pending_inputs.remove(&(sid.into(), id.into())); }
                Ok(Vec::new())
            }
            "session.step.started" => {
                self.messages.insert((sid.into(), mid.into()), (data.get("started").and_then(Value::as_i64).unwrap_or(event.ts_ms), data.get("model").cloned().unwrap_or(Value::Null)));
                Ok(Vec::new())
            }
            "session.tool.input.started" => {
                if let (Some(id), Some(name)) = (data.get("id").and_then(Value::as_str), data.get("name").and_then(Value::as_str)) {
                    self.tool_names.insert((sid.into(), id.into()), name.into());
                }
                Ok(Vec::new())
            }
            "session.tool.called" => {
                let Some(id) = data.get("id").and_then(Value::as_str) else { return Ok(Vec::new()); };
                let name = self.tool_names.get(&(sid.into(), id.into())).cloned().unwrap_or_else(|| "tool".into());
                let input = data.get("input").cloned().unwrap_or(Value::Null);
                let mut ops = self.emit(event, ctx, "message.part.updated", json!({"properties":{"part":{"sessionID":sid,"messageID":mid,"type":"tool","callID":id,"tool":name,"state":{"status":"running","input":input}}}}))?;
                ops.extend(self.emit(event, ctx, "tool.execute.before", json!({"input":{"sessionID":sid,"callID":id,"tool":name},"output":{"args":input}}))?);
                Ok(ops)
            }
            "session.tool.success" | "session.tool.failed" => {
                let Some(id) = data.get("id").and_then(Value::as_str) else { return Ok(Vec::new()); };
                let name = self.tool_names.remove(&(sid.into(), id.into())).unwrap_or_else(|| "tool".into());
                let failed = event.event == "session.tool.failed";
                let output = if failed { data.get("error") } else { data.get("content") }.cloned().unwrap_or(Value::Null);
                let state = if failed { json!({"status":"error","error":output}) } else { json!({"status":"completed","output":output}) };
                let mut ops = self.emit(event, ctx, "message.part.updated", json!({"properties":{"part":{"sessionID":sid,"messageID":mid,"type":"tool","callID":id,"tool":name,"state":state}}}))?;
                ops.extend(self.emit(event, ctx, "tool.execute.after", json!({"input":{"sessionID":sid,"callID":id,"tool":name},"result":{"output":output}}))?);
                Ok(ops)
            }
            "session.text.ended" | "session.reasoning.ended" => {
                let kind = if event.event == "session.text.ended" { "text" } else { "reasoning" };
                self.emit(event, ctx, "message.part.updated", json!({"properties":{"part":{"sessionID":sid,"messageID":mid,"id":data.get("ordinal").map(Value::to_string),"type":kind,"text":data.get("text"),"time":{"end":event.ts_ms}}}}))
            }
            "session.step.ended" | "session.step.failed" => {
                let (started, model) = self.messages.remove(&(sid.into(), mid.into())).unwrap_or((event.ts_ms, Value::Null));
                self.emit(event, ctx, "message.updated", json!({"properties":{"info":{"sessionID":sid,"id":mid,"role":"assistant","providerID":model.get("providerID"),"modelID":model.get("id"),"time":{"created":started,"completed":event.ts_ms},"tokens":data.get("tokens"),"error":data.get("error")}}}))
            }
            "session.execution.succeeded" => self.emit(event, ctx, "session.idle", json!({"properties":{"sessionID":sid}})),
            "session.execution.failed" | "session.execution.interrupted" => self.emit(event, ctx, "session.error", json!({"properties":{"sessionID":sid,"error":data.get("error")}})),
            "session.deleted" => self.emit(event, ctx, "session.deleted", json!({"properties":{"sessionID":sid}})),
            _ => Ok(Vec::new()),
        }
    }
    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.spans.checkpoint(ctx)
    }
    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.spans.finalize(ctx)
    }
}
