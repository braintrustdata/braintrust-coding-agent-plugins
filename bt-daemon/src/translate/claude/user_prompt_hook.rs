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
    Other(serde::de::IgnoredAny),
}

impl<'a> UserPromptHook<'a> {
    pub(crate) fn text(&self) -> Option<&'a str> {
        match self.prompt.as_ref()? {
            PromptText::Text(text) => Some(text),
            PromptText::Other(_) => None,
        }
    }
}
