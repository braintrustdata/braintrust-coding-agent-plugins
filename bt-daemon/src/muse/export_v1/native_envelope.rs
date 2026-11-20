use super::{NativePayload, UnixMicros};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct NativeEnvelope {
    pub recorded_at: Option<UnixMicros>,
    pub payload: Option<NativePayload>,
}
