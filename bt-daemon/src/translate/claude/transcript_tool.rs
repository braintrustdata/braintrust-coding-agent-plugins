//! One transcript-recovered tool call and its terminal transitions.
use super::transcript_envelope::{TranscriptEnvelope, TranscriptToolResult};
use super::transcript_tool_outcome::TranscriptToolOutcome;
use super::{parse_timestamp_ms, skill_load, string_field, tool_span_name};
use crate::translate::tool::{add_tool_approval, ToolApproval};
use crate::translate::{SpanRow, SpanType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Serialize, Deserialize)]
pub(super) struct TranscriptTool {
    pub(super) call_id: String,
    tool_name: String,
    input: Value,
    output: Option<Value>,
    approval: Option<ToolApproval>,
    cancelled: bool,
    pub(super) terminal: bool,
    error: Option<String>,
    start_ms: i64,
    end_ms: i64,
}

impl TranscriptTool {
    pub(super) fn from_call(call_id: String, block: &Value, record: &Value) -> Self {
        Self {
            call_id,
            tool_name: string_field(block, "name").unwrap_or_else(|| "Tool".into()),
            input: block.get("input").cloned().unwrap_or_else(|| json!({})),
            output: None,
            approval: None,
            cancelled: false,
            terminal: false,
            error: None,
            start_ms: parse_timestamp_ms(record).unwrap_or(0),
            end_ms: parse_timestamp_ms(record).unwrap_or(0),
        }
    }

    pub(super) fn observe_result(
        &mut self,
        output: Value,
        result: &TranscriptToolResult,
        row: &TranscriptEnvelope,
        timestamp_ms: Option<i64>,
    ) {
        self.output = Some(output);
        self.end_ms = timestamp_ms.unwrap_or(self.start_ms);
        self.terminal = true;
        match TranscriptToolOutcome::from_result(result, row.tool_use_result.as_ref()) {
            TranscriptToolOutcome::Succeeded => self.approval = Some(ToolApproval::Approved),
            TranscriptToolOutcome::Failed(error) => {
                self.approval = Some(ToolApproval::Approved);
                self.error = Some(error);
            }
            TranscriptToolOutcome::Denied => self.approval = Some(ToolApproval::Denied),
            TranscriptToolOutcome::Cancelled => self.cancelled = true,
        }
    }

    pub(super) fn into_structured_output(self) -> Option<Value> {
        (self.tool_name == "StructuredOutput"
            && self.approval == Some(ToolApproval::Approved)
            && self.error.is_none()
            && !self.cancelled)
            .then_some(self.input)
    }

    pub(super) fn is_successful_handback(&self) -> bool {
        self.tool_name == "SubagentHandback"
            && self.approval == Some(ToolApproval::Approved)
            && self.error.is_none()
            && !self.cancelled
    }

    pub(super) fn handback_message(&self) -> Option<Value> {
        self.input.get("message").cloned()
    }

    fn metadata(&self) -> Value {
        let mut metadata = Map::new();
        metadata.insert("tool_name".into(), json!(self.tool_name));
        add_tool_approval(&mut metadata, self.approval);
        metadata.insert("tool_call_id".into(), json!(self.call_id));
        metadata.insert("recovered_from_transcript".into(), json!(true));
        if self.cancelled || !self.terminal {
            metadata.insert("cancelled".into(), json!(true));
        }
        Value::Object(metadata)
    }

    pub(super) fn into_row(self, span_id: String, root_span_id: String, parent: String) -> SpanRow {
        SpanRow {
            span_id,
            root_span_id,
            parent_span_ids: vec![parent],
            name: tool_span_name(
                &self.tool_name,
                &self.input,
                skill_load(&self.tool_name, &self.input).as_ref(),
            ),
            span_type: SpanType::Tool,
            start_ms: Some(self.start_ms),
            end_ms: Some(self.end_ms),
            input: Some(self.input.clone()),
            output: self.output.clone(),
            metadata: Some(self.metadata()),
            error: self.error.clone(),
            ..Default::default()
        }
    }

    pub(super) fn into_update(
        self,
        span_id: String,
        root_span_id: String,
        parent_id: String,
    ) -> SpanRow {
        SpanRow {
            span_id,
            root_span_id,
            parent_span_ids: vec![parent_id],
            end_ms: Some(self.end_ms),
            output: self.output.clone(),
            metadata: Some(self.metadata()),
            error: self.error.clone(),
            ..Default::default()
        }
    }
}
