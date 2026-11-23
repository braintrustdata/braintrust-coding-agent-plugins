#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Observation {
    pub path: Option<String>,
    pub observed_bytes: Option<u64>,
    pub full_path: Option<String>,
    pub full_observed_bytes: Option<u64>,
}
