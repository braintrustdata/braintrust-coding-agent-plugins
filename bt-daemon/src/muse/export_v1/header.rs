use super::{Session, Version};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Header {
    pub export_schema_version: u64,
    pub exporter_version: Option<Version>,
    pub sessions: Vec<Session>,
    #[serde(default)]
    pub session_terminated_abnormally: bool,
}
