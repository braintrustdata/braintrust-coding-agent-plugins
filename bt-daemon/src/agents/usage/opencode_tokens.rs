use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct OpenCodeTokens {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    #[serde(default)]
    pub reasoning: i64,
    #[serde(default)]
    pub cache: OpenCodeCacheTokens,
}

impl OpenCodeTokens {
    /// OpenCode reports uncached input separately from cache reads and writes.
    pub fn prompt_tokens(&self) -> i64 {
        self.input + self.cache.read + self.cache.write
    }

    pub fn total_tokens(&self) -> i64 {
        self.prompt_tokens() + self.output + self.reasoning
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct OpenCodeCacheTokens {
    #[serde(default)]
    pub read: i64,
    #[serde(default)]
    pub write: i64,
}
