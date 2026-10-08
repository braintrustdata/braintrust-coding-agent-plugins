//! Shared terminal-state conventions for coding-agent tool spans.
//!
//! Translators decide which native events prove execution, failure, or denial.
//! This module only encodes the common Braintrust representation once that
//! source-specific interpretation has been made.

use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ToolApproval {
    Approved,
    Denied,
}

impl ToolApproval {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }
}

/// Adds approval metadata only when a translator has a reliable native signal.
pub fn add_tool_approval(metadata: &mut Map<String, Value>, approval: Option<ToolApproval>) {
    if let Some(approval) = approval {
        metadata.insert("tool_approval".into(), json!(approval.as_str()));
    }
}

pub fn tool_approval_metadata(approval: Option<ToolApproval>) -> Value {
    let mut metadata = Map::new();
    add_tool_approval(&mut metadata, approval);
    Value::Object(metadata)
}

/// Explicit `/skill`-style requests on the message or turn that made them.
pub fn explicit_skill_metadata(names: &[String]) -> Option<Value> {
    (!names.is_empty()).then(|| {
        json!({
            "loaded_skill_names": names,
            "loaded_skills": names.iter().map(|name| json!({ "name": name })).collect::<Vec<_>>(),
        })
    })
}

/// Omits fields the source did not report instead of recording them as null.
pub fn without_nulls(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
    value
}

pub fn with_tool_approval(mut metadata: Value, approval: Option<ToolApproval>) -> Value {
    if let Some(object) = metadata.as_object_mut() {
        add_tool_approval(object, approval);
    }
    metadata
}

/// Returns a concise, source-provided error message without serializing an
/// arbitrary native payload into the regular span error field.
pub fn error_text(value: Option<&Value>, fallback: &str) -> String {
    value
        .and_then(nonempty_error_text)
        .unwrap_or_else(|| fallback.to_string())
}

/// Extracts a concise message when the source has already identified a value
/// as an error. Callers must not use this to infer failure from normal output.
pub fn nonempty_error_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return text
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string);
    }
    // MCP and ACP tool results carry their message in `content` text blocks.
    if let Some(values) = value.as_array() {
        return values.iter().find_map(nonempty_error_text);
    }
    let object = value.as_object()?;
    for key in [
        "error", "message", "stderr", "output", "result", "content", "text",
    ] {
        if let Some(text) = object.get(key).and_then(nonempty_error_text) {
            return Some(text);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_is_omitted_when_the_host_cannot_prove_it() {
        let mut metadata = Map::new();
        add_tool_approval(&mut metadata, None);
        assert!(!metadata.contains_key("tool_approval"));
        add_tool_approval(&mut metadata, Some(ToolApproval::Denied));
        assert_eq!(metadata["tool_approval"], "denied");
    }

    #[test]
    fn error_text_keeps_the_useful_nested_message() {
        assert_eq!(
            error_text(
                Some(&json!({"result":{"message":"disk full\ntrace"}})),
                "fallback"
            ),
            "disk full"
        );
        assert_eq!(error_text(None, "fallback"), "fallback");
        assert_eq!(
            error_text(
                Some(&json!({
                    "content":[
                        {"type":"image","data":"aGk="},
                        {"type":"text","text":"ENOENT: missing\ntrace"}
                    ],
                    "details":{}
                })),
                "fallback"
            ),
            "ENOENT: missing"
        );
        assert_eq!(
            error_text(
                Some(&json!({"content":[{"type":"text","text":""}]})),
                "fallback"
            ),
            "fallback"
        );
        assert_eq!(
            nonempty_error_text(&json!("\n  disk full\ntrace")),
            Some("disk full".into())
        );
    }
}
