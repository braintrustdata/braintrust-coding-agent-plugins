use super::{ExportMetadata, RunStream};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(crate) struct RunModelRecord {
    pub run_stream: RunStream,
    #[serde(flatten)]
    pub metadata: ExportMetadata,
}
