use super::PermissionObserved;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct NotificationObserved {
    pub notification_type: Option<String>,
    pub title: Option<String>,
    pub message: Option<String>,
    #[serde(flatten)]
    pub tool: PermissionObserved,
}
