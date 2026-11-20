use super::{NativeEnvelope, UnixMicros};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct ExportRecord {
    pub recorded_at: Option<UnixMicros>,
    pub envelope: Option<NativeEnvelope>,
}
