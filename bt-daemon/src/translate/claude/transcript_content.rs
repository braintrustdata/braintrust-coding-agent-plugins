use super::transcript_envelope::TranscriptToolResult;
use crate::json::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Content shared by the prompt hook and native transcript messages.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub(super) enum TranscriptContent {
    Text(String),
    Blocks(Vec<TranscriptBlock>),
    // Keep unfamiliar content shapes readable in checkpoints and span input.
    Unknown(Value),
}

#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub(super) struct TranscriptBlock {
    #[serde(rename = "type", default, skip_serializing_if = "Field::is_missing")]
    pub(super) kind: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub(super) text: Field<String>,
    #[serde(flatten)]
    pub(super) result: TranscriptToolResult,
    // Preserve tool data and future native block fields when emitting content.
    #[serde(flatten)]
    extensions: BTreeMap<String, Value>,
}

impl TranscriptContent {
    pub(super) fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Blocks(_) | Self::Unknown(_) => None,
        }
    }

    pub(super) fn has_tool_result(&self) -> bool {
        matches!(self, Self::Blocks(blocks) if blocks.iter().any(|block| block.kind.as_deref() == Some("tool_result")))
    }

    pub(super) fn single_text(&self) -> Option<&str> {
        match self {
            Self::Blocks(blocks) => match blocks.as_slice() {
                [block] if block.kind.as_deref() == Some("text") => block.text.as_deref(),
                _ => None,
            },
            _ => self.as_text(),
        }
    }

    pub(super) fn matches_prompt(&self, prompt: &Self) -> bool {
        if self == prompt {
            return true;
        }
        let (Some(mut remaining), Self::Blocks(blocks)) = (prompt.as_text(), self) else {
            return false;
        };
        let mut has_text = false;
        for text in blocks.iter().filter_map(|block| {
            (block.kind.as_deref() == Some("text"))
                .then(|| block.text.as_deref())
                .flatten()
        }) {
            has_text = true;
            let Some(rest) = remaining.strip_prefix(text) else {
                return false;
            };
            remaining = rest;
        }
        remaining.is_empty() && (has_text || crate::attachments::contains(&serde_json::json!(self)))
    }
}
