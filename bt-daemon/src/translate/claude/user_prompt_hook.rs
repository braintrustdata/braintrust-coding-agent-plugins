use serde::Deserialize;

/// Text projection of a prompt hook. Imported prompts may also contain blocks.
#[derive(Deserialize)]
pub(crate) struct UserPromptHook<'a> {
    #[serde(default, borrow)]
    prompt: Option<PromptText<'a>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PromptText<'a> {
    Text(&'a str),
    Blocks(#[serde(borrow)] Vec<PromptBlock<'a>>),
    Other(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PromptBlock<'a> {
    Text {
        text: &'a str,
    },
    #[serde(other)]
    Other,
}

impl<'a> UserPromptHook<'a> {
    pub(crate) fn text(&self) -> Option<&'a str> {
        match self.prompt.as_ref()? {
            PromptText::Text(text) => Some(text),
            PromptText::Blocks(blocks) => match blocks.as_slice() {
                [PromptBlock::Text { text }] => Some(text),
                _ => None,
            },
            PromptText::Other(_) => None,
        }
    }
}
