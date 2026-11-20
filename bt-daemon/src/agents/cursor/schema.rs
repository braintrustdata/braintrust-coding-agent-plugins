//! Native Cursor transcript schema shared by live translation and import.
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum TranscriptRecord {
    Message {
        role: Role,
        message: TranscriptMessage,
    },
    /// Marks the end of a turn. The translator reads turn status from hooks,
    /// so only the marker's shape is checked.
    TurnEnded {
        #[serde(rename = "type")]
        _kind: TurnEndedKind,
        #[serde(rename = "status")]
        status: String,
        #[serde(rename = "error", default)]
        error: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Role {
    User,
    Assistant,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct TranscriptMessage {
    pub content: MessageContent,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TurnEndedKind {
    TurnEnded,
}

/// Message content: plain text, or content blocks.
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContentBlock {
    Text {
        text: String,
    },
    /// Tool requests are reported by hooks; transcripts add nothing to them.
    ToolUse,
}

impl MessageContent {
    /// The concatenated text of the content.
    pub fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Blocks(blocks) => blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    ContentBlock::ToolUse => None,
                })
                .collect(),
        }
    }
}

impl TranscriptRecord {
    pub fn role(&self) -> Option<Role> {
        match self {
            Self::Message { role, .. } => Some(*role),
            Self::TurnEnded { .. } => None,
        }
    }

    pub fn text(&self) -> Option<String> {
        match self {
            Self::Message { message, .. } => Some(message.content.text()),
            Self::TurnEnded { .. } => None,
        }
    }
}
