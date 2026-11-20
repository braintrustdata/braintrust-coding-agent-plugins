use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(crate) struct SessionEnd {
    pub exit_reason: Option<String>,
}
