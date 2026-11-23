use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub(crate) enum MessageRole {
    System,
    Developer,
    User,
    Assistant,
    Tool,
    Other(String),
}

impl From<String> for MessageRole {
    fn from(s: String) -> Self {
        match s.as_str() {
            "system" => Self::System,
            "developer" => Self::Developer,
            "user" => Self::User,
            "assistant" => Self::Assistant,
            "tool" => Self::Tool,
            _ => Self::Other(s),
        }
    }
}

impl From<MessageRole> for String {
    fn from(role: MessageRole) -> Self {
        match role {
            MessageRole::System => "system",
            MessageRole::Developer => "developer",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
            MessageRole::Other(s) => return s,
        }
        .into()
    }
}
