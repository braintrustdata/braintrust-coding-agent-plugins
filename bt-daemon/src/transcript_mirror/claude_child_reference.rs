#[derive(serde::Deserialize)]
pub(crate) struct ClaudeChildReference {
    pub path: String,
    pub through: Option<u64>,
}
