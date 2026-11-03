//! OpenCode 2 event translation. Native event DTOs are decoded before they
//! reach the reducer; open-ended tool values remain JSON.
mod event;

use self::event::*;
use super::{opencode, recent::RecentSet, AgentTranslator, SessionCtx, SpanOp};
use crate::wire::Envelope;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

pub(super) struct OpenCodeV2Translator {
    spans: Box<dyn AgentTranslator>,
    messages: HashMap<(String, String), (i64, Option<Model>)>,
    pending_inputs: HashMap<(String, String), Prompt>,
    tool_names: HashMap<(String, String), String>,
    compactions: HashMap<String, (i64, String)>,
    pending_context: HashMap<String, Context>,
    message_context: HashMap<(String, String), Context>,
    text_parts: HashMap<(String, String, bool), BTreeMap<u64, String>>,
    seen: RecentSet<String>,
}

struct ToolCompletion<'a> {
    session_id: &'a str,
    message_id: &'a str,
    id: &'a str,
    output: Value,
    failed: bool,
}

impl OpenCodeV2Translator {
    pub(super) fn new(session_id: &str) -> Self {
        Self {
            spans: opencode::new_translator(session_id),
            messages: HashMap::new(),
            pending_inputs: HashMap::new(),
            tool_names: HashMap::new(),
            compactions: HashMap::new(),
            pending_context: HashMap::new(),
            message_context: HashMap::new(),
            text_parts: HashMap::new(),
            seen: RecentSet::default(),
        }
    }

    fn emit(
        &mut self,
        original: &Envelope,
        ctx: &SessionCtx,
        directory: Option<&str>,
        name: &str,
        mut payload: Value,
    ) -> anyhow::Result<Vec<SpanOp>> {
        let mut event = original.clone();
        event.event = name.to_string();
        if let (Some(directory), Some(object)) = (directory, payload.as_object_mut()) {
            object.insert("directory".into(), directory.into());
        }
        event.payload = payload;
        self.spans.handle(&event, ctx)
    }

    fn tool_finished(
        &mut self,
        event: &Envelope,
        ctx: &SessionCtx,
        directory: Option<&str>,
        completion: ToolCompletion<'_>,
    ) -> anyhow::Result<Vec<SpanOp>> {
        let ToolCompletion {
            session_id: sid,
            message_id: mid,
            id,
            output,
            failed,
        } = completion;
        let name = self
            .tool_names
            .remove(&(sid.into(), id.into()))
            .unwrap_or_else(|| "tool".into());
        let state = if failed {
            json!({"status":"error","error":output})
        } else {
            json!({"status":"completed","output":output})
        };
        let mut ops = self.emit(event, ctx, directory, "message.part.updated", json!({"properties":{"part":{"sessionID":sid,"messageID":mid,"type":"tool","callID":id,"tool":name,"state":state}}}))?;
        ops.extend(self.emit(
            event,
            ctx,
            directory,
            "tool.execute.after",
            json!({"input":{"sessionID":sid,"callID":id,"tool":name},"result":{"output":output}}),
        )?);
        Ok(ops)
    }
}

impl AgentTranslator for OpenCodeV2Translator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        // The event name is authoritative on the daemon envelope. The
        // synthetic model-context payload has no native `type` field.
        let mut payload = event.payload.clone();
        let Some(object) = payload.as_object_mut() else {
            return Ok(Vec::new());
        };
        object.insert("type".into(), event.event.clone().into());
        let Ok(native) = serde_json::from_value::<NativeEnvelope>(payload) else {
            return Ok(Vec::new());
        };
        if let Some(id) = &native.id {
            if !self.seen.insert(id.clone()) {
                return Ok(Vec::new());
            }
        }
        let directory = native
            .location
            .as_ref()
            .and_then(|location| location.directory.as_deref());
        match native.event {
            NativeEvent::Created(data) => self.emit(event, ctx, directory, "session.created", json!({"properties":{"info":{"id":data.session_id,"parentID":data.parent_id,"title":data.title}}})),
            NativeEvent::Enqueued(data) => {
                if let InboxItem::User { payload } = data.item { self.pending_inputs.insert((data.session_id, data.inbox_id), payload); }
                Ok(Vec::new())
            }
            NativeEvent::Delivered(data) => {
                let Some(input) = self.pending_inputs.remove(&(data.session_id.clone(), data.inbox_id.clone())) else { return Ok(Vec::new()); };
                let mut parts = vec![json!({"type":"text","text":input.text})];
                for file in input.files { if file.mime.starts_with("image/") { parts.push(json!({"type":"image_url","image_url":{"url":format!("data:{};base64,{}",file.mime,file.data)}})); } }
                self.emit(event, ctx, directory, "chat.message", json!({"input":{"sessionID":data.session_id,"turnID":data.inbox_id},"output":{"message":{"id":data.inbox_id},"parts":parts}}))
            }
            NativeEvent::Cancelled(data) => { self.pending_inputs.remove(&(data.session_id, data.inbox_id)); Ok(Vec::new()) }
            NativeEvent::Context(data) => { let sid = data.session_id.clone(); self.pending_context.insert(sid, data); Ok(Vec::new()) }
            NativeEvent::StepStarted(data) => {
                let key = (data.session_id.clone(), data.message_id);
                if let Some(context) = self.pending_context.remove(&data.session_id) { self.message_context.insert(key.clone(), context); }
                self.messages.insert(key, (data.started.unwrap_or(event.ts_ms), data.model)); Ok(Vec::new())
            }
            NativeEvent::ToolInputStarted(data) => { self.tool_names.insert((data.session_id, data.id), data.name); Ok(Vec::new()) }
            NativeEvent::PermissionAsked(data) => {
                let call = data.source.map(|source| source.id);
                let name = call.as_ref().and_then(|id| self.tool_names.get(&(data.session_id.clone(), id.clone()))).cloned().unwrap_or_else(|| data.action.clone());
                self.emit(event, ctx, directory, "permission.asked", json!({"properties":{"id":data.id,"sessionID":data.session_id,"tool":{"callID":call,"name":name},"permissionType":data.action}}))
            }
            NativeEvent::PermissionReplied(data) => self.emit(event, ctx, directory, "permission.replied", json!({"properties":{"requestID":data.request_id,"sessionID":data.session_id,"reply":data.reply}})),
            NativeEvent::CompactionStarted(data) => { self.compactions.insert(data.session_id, (event.ts_ms, data.input_id.unwrap_or_else(|| "compaction".into()))); Ok(Vec::new()) }
            NativeEvent::CompactionEnded(data) => {
                let (started, trigger) = self.compactions.remove(&data.session_id).unwrap_or((event.ts_ms, "compaction".into()));
                if data.native.as_ref().and_then(|native| native.messages.as_ref()).is_some() { return Ok(Vec::new()); }
                let summary = native.id.as_deref().unwrap_or("compaction-summary");
                let keep_id = native.metadata.and_then(|metadata| metadata.keep_id);
                let mut ops = self.emit(event, ctx, directory, "message.part.updated", json!({"properties":{"part":{"sessionID":data.session_id,"messageID":trigger,"type":"compaction","tail_start_id":keep_id}}}))?;
                ops.extend(self.emit(event, ctx, directory, "message.part.updated", json!({"properties":{"part":{"sessionID":data.session_id,"messageID":summary,"type":"text","text":data.text,"time":{"end":event.ts_ms}}}}))?);
                ops.extend(self.emit(event, ctx, directory, "message.updated", json!({"properties":{"info":{"sessionID":data.session_id,"id":summary,"role":"assistant","summary":true,"providerID":data.model.as_ref().and_then(|m| m.provider_id.as_ref()),"modelID":data.model.as_ref().and_then(|m| m.id.as_ref()),"time":{"created":started,"completed":event.ts_ms},"tokens":data.tokens}}}))?);
                Ok(ops)
            }
            NativeEvent::CompactionFailed(data) => {
                let (started, _) = self.compactions.remove(&data.session_id).unwrap_or((event.ts_ms, "compaction".into()));
                let id = native.id.as_deref().unwrap_or("compaction-failed");
                self.emit(event, ctx, directory, "message.updated", json!({"properties":{"info":{"sessionID":data.session_id,"id":id,"role":"assistant","compaction":true,"time":{"created":started,"completed":event.ts_ms},"tokens":data.tokens,"error":data.error}}}))
            }
            NativeEvent::ToolCalled(data) => {
                let name = self.tool_names.get(&(data.session_id.clone(), data.id.clone())).cloned().unwrap_or_else(|| "tool".into());
                let mut ops = self.emit(event, ctx, directory, "message.part.updated", json!({"properties":{"part":{"sessionID":data.session_id,"messageID":data.message_id,"type":"tool","callID":data.id,"tool":name,"state":{"status":"running","input":data.input}}}}))?;
                ops.extend(self.emit(event, ctx, directory, "tool.execute.before", json!({"input":{"sessionID":data.session_id,"callID":data.id,"tool":name},"output":{"args":data.input}}))?);
                Ok(ops)
            }
            NativeEvent::ToolSuccess(data) => self.tool_finished(event, ctx, directory, ToolCompletion { session_id: &data.session_id, message_id: &data.message_id, id: &data.id, output: normalize_tool_content(data.content.unwrap_or_default()), failed: false }),
            NativeEvent::ToolFailed(data) => self.tool_finished(event, ctx, directory, ToolCompletion { session_id: &data.session_id, message_id: &data.message_id, id: &data.id, output: json!(data.error), failed: true }),
            NativeEvent::TextEnded(data) | NativeEvent::ReasoningEnded(data) => {
                let reasoning = event.event == "session.reasoning.ended";
                let kind = if reasoning { "reasoning" } else { "text" };
                let parts = self.text_parts.entry((data.session_id.clone(), data.message_id.clone(), reasoning)).or_default();
                parts.insert(data.ordinal.unwrap_or(0), data.text);
                let text = parts.values().map(String::as_str).collect::<String>();
                self.emit(event, ctx, directory, "message.part.updated", json!({"properties":{"part":{"sessionID":data.session_id,"messageID":data.message_id,"type":kind,"text":text,"time":{"end":event.ts_ms}}}}))
            }
            NativeEvent::StepEnded(data) | NativeEvent::StepFailed(data) => {
                let key = (data.session_id.clone(), data.message_id.clone());
                self.text_parts.remove(&(data.session_id.clone(), data.message_id.clone(), false));
                self.text_parts.remove(&(data.session_id.clone(), data.message_id.clone(), true));
                let (started, model) = self.messages.remove(&key).unwrap_or((event.ts_ms, None));
                let mut ops = Vec::new();
                if let Some(context) = self.message_context.remove(&key) { ops.extend(self.emit(event, ctx, directory, "model.context", json!({"sessionID":data.session_id,"messages":normalize_context(context)}))?); }
                ops.extend(self.emit(event, ctx, directory, "message.updated", json!({"properties":{"info":{"sessionID":data.session_id,"id":data.message_id,"role":"assistant","providerID":model.as_ref().and_then(|m| m.provider_id.as_ref()),"modelID":model.as_ref().and_then(|m| m.id.as_ref()),"time":{"created":started,"completed":event.ts_ms},"tokens":data.tokens,"error":data.error}}}))?);
                Ok(ops)
            }
            NativeEvent::Succeeded(data) => self.emit(event, ctx, directory, "session.idle", json!({"properties":{"sessionID":data.session_id}})),
            NativeEvent::Failed(data) => self.emit(event, ctx, directory, "session.error", json!({"properties":{"sessionID":data.session_id,"error":data.error}})),
            NativeEvent::Interrupted(data) => self.emit(event, ctx, directory, "session.error", json!({"properties":{"sessionID":data.session_id,"error":format!("OpenCode session interrupted ({})", data.reason.as_deref().unwrap_or("unknown"))}})),
            NativeEvent::Deleted(data) => self.emit(event, ctx, directory, "session.deleted", json!({"properties":{"sessionID":data.session_id}})),
            NativeEvent::Other => Ok(Vec::new()),
        }
    }
    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.spans.checkpoint(ctx)
    }
    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.spans.finalize(ctx)
    }
}

fn normalize_context(context: Context) -> Vec<Value> {
    let mut messages = Vec::new();
    let system = context.system.join("\n\n");
    if !system.is_empty() {
        messages.push(json!({"role":"system","content":system}));
    }
    for message in context.messages {
        match message.content {
            MessageContent::Text(text) => {
                messages.push(json!({"role":message.role,"content":text}))
            }
            MessageContent::Other(content) => {
                messages.push(json!({"role":message.role,"content":content}))
            }
            MessageContent::Parts(parts) => {
                let mut content = Vec::new();
                let mut calls = Vec::new();
                let mut single_text = None;
                for part in parts {
                    match part {
                        ContextPart::Known(KnownContextPart::Text { text }) => { single_text = Some(text.clone()); content.push(json!({"type":"text","text":text})); },
                        ContextPart::Known(KnownContextPart::Media { media_type, data, media, extra }) => {
                            let source = media.as_ref().and_then(|media| media.source.as_ref());
                            let mime = source.as_ref().and_then(|source| source.media_type.as_deref()).or(media_type.as_deref());
                            let bytes = source.as_ref().and_then(|source| source.data.as_deref()).or(data.as_deref());
                            let image = mime.zip(bytes).filter(|(mime, _)| mime.starts_with("image/"));
                            if let Some((mime, bytes)) = image { content.push(json!({"type":"image_url","image_url":{"url":format!("data:{mime};base64,{bytes}")}})); }
                            else if let Ok(value) = serde_json::to_value(KnownContextPart::Media { media_type, data, media, extra }) { content.push(value); }
                        }
                        ContextPart::Known(KnownContextPart::ToolCall { id, name, input }) => calls.push(json!({"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}})),
                        ContextPart::Known(KnownContextPart::ToolResult { id, result }) => {
                            let value = match serde_json::from_value::<Vec<ToolContent>>(result.value.clone()) { Ok(parts) => normalize_tool_content(parts), Err(_) => result.value };
                            messages.push(json!({"role":"tool","tool_call_id":id,"content":value}));
                        }
                        ContextPart::Other(value) => content.push(value),
                    }
                }
                if message.role == "tool" {
                    continue;
                }
                let value = match content.as_slice() {
                    [] => json!(""),
                    [_] if single_text.is_some() => json!(single_text),
                    _ => Value::Array(content),
                };
                if calls.is_empty() {
                    messages.push(json!({"role":message.role,"content":value}));
                } else {
                    messages.push(json!({"role":message.role,"content":value,"tool_calls":calls}));
                }
            }
        }
    }
    messages
}

fn normalize_tool_content(parts: Vec<ToolContent>) -> Value {
    Value::Array(
        parts
            .into_iter()
            .map(|part| match part {
                ToolContent::Known(KnownToolContent::File { mime, uri, .. })
                    if mime.starts_with("image/") =>
                {
                    json!({"type":"image_url","image_url":{"url":uri}})
                }
                other => serde_json::to_value(other).unwrap_or(Value::Null),
            })
            .collect(),
    )
}
