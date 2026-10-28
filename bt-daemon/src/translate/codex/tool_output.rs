//! Interpret native result envelopes without interpreting command stdout.
//!
//! Code-mode `exec` can complete while one or more commands fail or keep
//! running. Keep those facts separate, and never turn missing output into a
//! confirmed interruption. The metadata allowlist here is wrapper_outcome,
//! command_outcomes (outcome, exit_code, session_id), and outcome for an explicit
//! interruption. Raw tool output is retained by the caller.

use super::super::tool::{error_text, nonempty_error_text};
use serde_json::{json, Map, Value};

#[derive(Default)]
pub(super) struct ToolOutcome {
    pub error: Option<String>,
    pub metadata: Map<String, Value>,
    commands: Vec<Value>,
}

pub(super) fn classify(tool_name: &str, output: &Value) -> ToolOutcome {
    let mut outcome = ToolOutcome::default();
    if matches!(
        tool_name.rsplit('.').next(),
        Some("exec" | "wait" | "exec_command" | "write_stdin" | "shell" | "shell_command")
    ) {
        outcome.observe(output, 0);
        if outcome.commands.is_empty() && outcome.error.is_none() {
            outcome.error = native_error(output);
        }
    } else {
        // Preserve explicit failure signals on other tools, but do not search
        // their JSON documents or text blocks for execution telemetry.
        outcome.error = native_error(output);
        if let Some(code) = exit_code(output) {
            outcome.command(Some(code), None, false);
        }
    }
    if !outcome.commands.is_empty() {
        if outcome.error.is_none()
            && outcome
                .commands
                .iter()
                .any(|command| command["outcome"] == "interrupted")
        {
            outcome
                .metadata
                .insert("outcome".into(), json!("interrupted"));
        }
        outcome.metadata.insert(
            "command_outcomes".into(),
            Value::Array(std::mem::take(&mut outcome.commands)),
        );
    }
    outcome
}

impl ToolOutcome {
    fn command(&mut self, code: Option<i64>, session_id: Option<&Value>, interrupted: bool) {
        let status = if interrupted {
            "interrupted"
        } else if let Some(code) = code {
            if code == 0 {
                "success"
            } else {
                self.error
                    .get_or_insert_with(|| format!("Command exited with code {code}"));
                "nonzero_exit"
            }
        } else {
            "running"
        };
        let mut fields = Map::new();
        fields.insert("outcome".into(), json!(status));
        if let Some(code) = code {
            fields.insert("exit_code".into(), json!(code));
        }
        if let Some(id) = session_id {
            fields.insert("session_id".into(), id.clone());
        }
        self.commands.push(Value::Object(fields));
    }

    fn observe(&mut self, value: &Value, depth: usize) {
        // Bound recursion through JSON-encoded and nested code-mode results.
        if depth > 16 {
            return;
        }
        match value {
            Value::Object(object) => {
                let interrupted = matches!(
                    object.get("status").and_then(Value::as_str),
                    Some("cancelled" | "canceled" | "interrupted" | "aborted")
                );
                if !interrupted && self.error.is_none() {
                    self.error = native_error(value);
                }
                let code = exit_code(value);
                let session_id = object
                    .get("session_id")
                    .filter(|id| id.is_i64() || id.is_string());
                if code.is_some()
                    || interrupted
                    || (session_id.is_some() && object.contains_key("output"))
                {
                    self.command(code, session_id, interrupted);
                    // stdout/stderr/output can themselves contain JSON or
                    // error-like text. They are diagnostics, not child results.
                    return;
                }
                if matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("input_text" | "output_text" | "text")
                ) {
                    if let Some(text) = object.get("text").and_then(Value::as_str) {
                        self.observe_text(text, depth + 1);
                    }
                } else {
                    // Known container shapes, including Promise.allSettled.
                    // Do not recursively scan arbitrary API payload fields.
                    for key in ["result", "results", "value", "content"] {
                        if let Some(nested) = object.get(key) {
                            self.observe(nested, depth + 1);
                        }
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.observe(value, depth + 1);
                }
            }
            Value::String(text) => {
                self.observe_text(text, depth + 1);
            }
            _ => {}
        }
    }

    fn observe_text(&mut self, text: &str, depth: usize) {
        if depth > 16 {
            return;
        }
        let first = text.lines().next().unwrap_or(text);
        let wrapper = if first == "Script completed" {
            Some("completed")
        } else if first.starts_with("Script running with cell ID ") {
            Some("running")
        } else {
            None
        };
        if let Some(wrapper) = wrapper {
            self.metadata
                .insert("wrapper_outcome".into(), json!(wrapper));
            if let Some((_, body)) = text.split_once("\nOutput:\n") {
                self.observe_text(body, depth + 1);
            }
            return;
        }
        if self.observe_command_header(text) {
            return;
        }
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            self.observe(&value, depth + 1);
        } else {
            // Each text() call can add a JSON result to one output block. A
            // truncation notice may precede these independently valid records.
            for line in text.lines() {
                if let Ok(value) = serde_json::from_str::<Value>(line) {
                    self.observe(&value, depth + 1);
                }
            }
        }
    }

    fn observe_command_header(&mut self, text: &str) -> bool {
        let first = text.lines().next().unwrap_or(text);
        if ![
            "Chunk ID:",
            "Wall time:",
            "Process exited with code ",
            "Process running with session ID ",
            "Exit code ",
        ]
        .iter()
        .any(|prefix| first.starts_with(prefix))
        {
            return false;
        }
        // Native result metadata ends before Final output. Never search the
        // command's printed output for an exit status, including on success.
        let header = text
            .split_once("\nFinal output:")
            .map_or(text, |(header, _)| header);
        for line in header.lines() {
            if let Some(code) = line
                .strip_prefix("Process exited with code ")
                .or_else(|| line.strip_prefix("Exit code "))
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.parse::<i64>().ok())
            {
                self.command(Some(code), None, false);
                return true;
            }
            if let Some(id) = line.strip_prefix("Process running with session ID ") {
                let id = id.trim();
                let id = id.parse::<i64>().map_or_else(|_| json!(id), |id| json!(id));
                self.command(None, Some(&id), false);
                return true;
            }
        }
        // A recognized header still separates stdout from telemetry when its
        // terminal status was truncated or omitted. Do not parse the body.
        true
    }
}

fn exit_code(value: &Value) -> Option<i64> {
    value
        .get("exit_code")
        .or_else(|| value.get("exitCode"))
        .and_then(Value::as_i64)
}

fn has_error_marker(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::String(_) => nonempty_error_text(value).is_some(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        Value::Number(_) => true,
    }
}

fn native_error(output: &Value) -> Option<String> {
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
        if let Some(error) = object.get("error").filter(|error| has_error_marker(error)) {
            return Some(error_text(Some(error), "Tool execution failed"));
        }
    }
    if let Some(text) = output.as_str() {
        let first = text.lines().next().unwrap_or(text);
        if first.to_ascii_lowercase().starts_with("error:") {
            return Some(first.to_owned());
        }
        if first
            .strip_prefix("Exit code ")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|code| code != 0)
        {
            return Some(first.to_owned());
        }
    }
    None
}
