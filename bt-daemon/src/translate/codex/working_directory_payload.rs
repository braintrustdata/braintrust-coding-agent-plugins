use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct WorkingDirectoryPayload {
    #[serde(default)]
    pub(super) cwd: Option<String>,
}
