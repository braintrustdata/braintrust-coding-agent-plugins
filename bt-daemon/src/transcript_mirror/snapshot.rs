#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct Snapshot {
    pub path: String,
    pub contents: String,
}
