use super::transcript_content::TranscriptContent;
use crate::json::Field;
use serde::Deserialize;

#[derive(Default, Deserialize)]
pub(crate) struct UserPromptHook {
    #[serde(default)]
    pub(super) prompt: Field<TranscriptContent>,
}

impl UserPromptHook {
    pub(crate) fn text(&self) -> Option<&str> {
        self.prompt.as_ref()?.single_text()
    }
}
