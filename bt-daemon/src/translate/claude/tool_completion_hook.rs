use crate::json::Field;
use crate::translate::tool::nonempty_error_text;
use serde::Deserialize;
use serde_json::Value;

#[derive(Default, Deserialize)]
pub(super) struct ToolCompletionHook {
    // The hook may carry a plain message or a tool-specific structured error.
    pub(super) error: Option<Value>,
    message: Option<Value>,
    // Tools supply their own result fields.
    #[serde(default)]
    tool_response: Field<Value>,
    #[serde(default)]
    output: Field<Value>,
}

impl ToolCompletionHook {
    pub(super) fn into_output(self, preserve_denial_reason: bool) -> Option<Value> {
        let output = if self.tool_response.is_missing() {
            self.output
        } else {
            self.tool_response
        };
        match output {
            Field::Value(value) => Some(value),
            Field::Null => Some(Value::Null),
            Field::Missing if preserve_denial_reason => self.error,
            Field::Missing => None,
        }
    }

    pub(super) fn summary(&self) -> Option<String> {
        let response_field = |name| {
            self.tool_response
                .as_ref()
                .and_then(|result| result.get(name))
        };
        [
            self.error.as_ref(),
            self.message.as_ref(),
            response_field("error"),
            response_field("stderr"),
            response_field("message"),
            response_field("output"),
            self.tool_response.as_ref(),
        ]
        .into_iter()
        .flatten()
        .find_map(nonempty_error_text)
    }

    pub(super) fn tool_error(&self) -> Option<String> {
        let response = self.tool_response.as_ref();
        if let Some(error) = [
            self.error.as_ref(),
            response.and_then(|result| result.get("error")),
        ]
        .into_iter()
        .flatten()
        .find_map(nonempty_error_text)
        {
            return Some(error);
        }
        let failed = response.is_some_and(|response| {
            response
                .get("interrupted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || response
                    .get("is_error")
                    .or_else(|| response.get("isError"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                || response
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|value| matches!(value, "error" | "failed"))
        });
        failed.then(|| {
            self.summary()
                .unwrap_or_else(|| "Tool execution failed".into())
        })
    }
}
