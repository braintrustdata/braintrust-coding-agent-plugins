//! Active model-visible context, fork inheritance and compaction preservation.
use super::history_message::HistoryMessage;
use super::llm_call::LlmCall;
use super::parsed_transcript::ParsedTranscript;
use super::transcript_envelope::TranscriptEnvelope;
use super::transcript_tool::TranscriptTool;
use super::{decode, parse_timestamp_ms, string_field};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Default, Serialize, Deserialize)]
pub(super) struct MessageHistory {
    messages: Vec<HistoryMessage>,
    preserved_after_compaction: Vec<String>,
    fork_context_prefix: bool,
}
impl MessageHistory {
    fn active(&self) -> Vec<Value> {
        self.messages
            .iter()
            .map(|message| message.value().clone())
            .collect()
    }

    fn push(&mut self, value: Value, source_uuid: Option<String>) -> usize {
        let index = self.messages.len();
        self.messages.push(HistoryMessage::new(value, source_uuid));
        index
    }

    fn update(&mut self, index: usize, value: Value, source_uuid: Option<String>) {
        let Some(message) = self.messages.get_mut(index) else {
            return;
        };
        message.update(value, source_uuid);
    }

    fn observe_compact_boundary(&mut self, record: &Value) {
        self.preserved_after_compaction = record
            .pointer("/compactMetadata/preservedMessages/uuids")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
    }

    fn begin_compacted(&mut self, summary: Value, source_uuid: Option<String>) {
        let preserved = std::mem::take(&mut self.preserved_after_compaction);
        let kept = self
            .messages
            .drain(..)
            .filter(|message| message.is_preserved_by(&preserved))
            .collect::<Vec<_>>();
        self.messages
            .push(HistoryMessage::new(summary, source_uuid));
        self.messages.extend(kept);
    }
}
impl MessageHistory {
    pub(super) fn parse_records(mut self, records: &[Value]) -> ParsedTranscript {
        let mut calls = Vec::<LlmCall>::new();
        let mut call_indexes = HashMap::<String, usize>::new();
        let mut assistant_history_indexes = HashMap::<String, usize>::new();
        let mut tools = HashMap::<String, TranscriptTool>::new();
        let mut tool_order = Vec::<String>::new();

        for record in records {
            let Some(envelope) = decode::<TranscriptEnvelope>(record) else {
                continue;
            };
            match envelope.kind.as_deref() {
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
                            if self.fork_context_prefix {
                                Vec::new()
                            } else {
                                self.active()
                            },
                            self.fork_context_prefix,
                        ));
                        index
                    });
                    calls[index].observe(record);
                    let output = calls[index].output_message();
                    let source_uuid = string_field(record, "uuid");
                    if let Some(history_index) = assistant_history_indexes.get(&request_id) {
                        self.update(*history_index, output, source_uuid);
                    } else {
                        let history_index = self.push(output, source_uuid);
                        assistant_history_indexes.insert(request_id.clone(), history_index);
                    }
                    // Fork transcripts copy the launching response before their
                    // first user row. It is input context, not child work or usage.
                    if calls[index].inherited {
                        continue;
                    }
                    if let Some(content) =
                        record.pointer("/message/content").and_then(Value::as_array)
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
                                        TranscriptTool::from_call(call_id, block, record),
                                    );
                                }
                            }
                        }
                    }
                }
                Some("user") => {
                    self.fork_context_prefix = false;
                    // Claude persists a synthetic user message containing the new
                    // active context immediately after a compact_boundary record.
                    // It is the first model-visible message after the boundary.
                    if envelope.is_compact_summary {
                        let content = record
                            .pointer("/message/content")
                            .cloned()
                            .unwrap_or(Value::Null);
                        self.begin_compacted(
                            json!({
                                "role": "user",
                                "content": if content.is_null() {
                                    json!("[compaction summary unavailable]")
                                } else {
                                    content
                                },
                                "message_type": "compaction_summary"
                            }),
                            string_field(record, "uuid"),
                        );
                        continue;
                    }
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
                            self.push(
                                json!({
                                    "role": "tool",
                                    "tool_call_id": call_id,
                                    "content": result
                                }),
                                string_field(record, "uuid"),
                            );
                            if let Some(tool) = tools.get_mut(&call_id) {
                                tool.observe_result(result, block, record);
                            }
                        }
                        if !had_tool_result {
                            self.push(
                                json!({ "role": "user", "content": content }),
                                string_field(record, "uuid"),
                            );
                        }
                    } else if !content.is_null() {
                        self.push(
                            json!({ "role": "user", "content": content }),
                            string_field(record, "uuid"),
                        );
                    }
                }
                Some("system") if envelope.subtype.as_deref() == Some("compact_boundary") => {
                    self.observe_compact_boundary(record);
                }
                Some("fork-context-ref") => self.fork_context_prefix = true,
                _ => {}
            }
        }
        calls.retain(|call| !call.inherited);
        ParsedTranscript {
            calls,
            tools: tool_order
                .into_iter()
                .filter_map(|id| tools.remove(&id))
                .collect(),
            history: self,
        }
    }
}
