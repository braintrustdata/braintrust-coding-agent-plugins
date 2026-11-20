use serde::Deserialize;

#[derive(Clone, Default, Deserialize)]
pub(crate) struct ExportMetadata {
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub workspace_root: Option<String>,
}
