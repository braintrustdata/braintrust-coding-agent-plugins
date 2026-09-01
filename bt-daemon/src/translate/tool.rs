//! Shared terminal-state conventions for coding-agent tool spans.
//!
//! Translators decide which native events prove execution, failure, or denial.
//! This module only encodes the common Braintrust representation once that
//! source-specific interpretation has been made.

use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
        return Some(text.lines().next().unwrap_or(text).to_string());
    }
    let object = value.as_object()?;
    for key in ["error", "message", "stderr", "output", "result"] {
        let Some(candidate) = object.get(key) else {
            continue;
        };
        if let Some(text) = candidate.as_str().filter(|text| !text.is_empty()) {
            return Some(text.lines().next().unwrap_or(text).to_string());
        }
        if let Some(nested) = candidate.as_object() {
            for key in ["error", "message"] {
                if let Some(text) = nested
                    .get(key)
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    return Some(text.lines().next().unwrap_or(text).to_string());
                }
            }
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
    }
}
