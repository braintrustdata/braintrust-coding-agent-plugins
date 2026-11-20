//! One native model request, including streamed output and monotonic usage.
use super::{decode, parse_timestamp_ms, string_field};
use crate::agents::usage::SnakeCaseUsage;
use crate::translate::{SpanRow, SpanType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Serialize, Deserialize)]
pub(super) struct LlmCall {
    pub(super) request_id: String,
    pub(super) inherited: bool,
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
    pub(super) fn new(
        request_id: String,
        start_ms: i64,
        input: Vec<Value>,
        inherited: bool,
    ) -> Self {
        Self {
            request_id,
            inherited,
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

    pub(super) fn observe(&mut self, record: &Value) {
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
        #[derive(Deserialize)]
        struct UsageRecord {
            message: Option<UsageMessage>,
        }
        #[derive(Deserialize)]
        struct UsageMessage {
            usage: Option<SnakeCaseUsage>,
        }
        if let Some(usage) = decode::<UsageRecord>(record)
            .and_then(|record| record.message)
            .and_then(|message| message.usage)
        {
            self.prompt_tokens = self.prompt_tokens.max(usage.input_tokens.unwrap_or(0));
            self.completion_tokens = self.completion_tokens.max(usage.output_tokens.unwrap_or(0));
            self.cache_creation_tokens = self
                .cache_creation_tokens
                .max(usage.cache_creation_input_tokens.unwrap_or(0));
            self.cache_read_tokens = self
                .cache_read_tokens
                .max(usage.cache_read_input_tokens.unwrap_or(0));
            if let Some(cache) = usage.cache_creation {
                self.cache_creation_5m_tokens = self
                    .cache_creation_5m_tokens
                    .max(cache.ephemeral_5m_input_tokens.unwrap_or(0));
                self.cache_creation_1h_tokens = self
                    .cache_creation_1h_tokens
                    .max(cache.ephemeral_1h_input_tokens.unwrap_or(0));
            }
        }
    }

    pub(super) fn output_message(&self) -> Value {
        let content = self.text.join("\n");
        if self.tool_calls.is_empty() {
            json!({ "role": "assistant", "content": content })
        } else {
            json!({ "role": "assistant", "content": content, "tool_calls": self.tool_calls })
        }
    }

    pub(super) fn into_row(self, span_id: String, root_span_id: String, parent: String) -> SpanRow {
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
