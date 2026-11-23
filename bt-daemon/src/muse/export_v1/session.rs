use super::SessionEnd;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(crate) struct Session {
    pub session_id: String,
    pub trajectory_id: Option<String>,
    pub root_session_id: Option<String>,
    pub session_end: Option<SessionEnd>,
}
