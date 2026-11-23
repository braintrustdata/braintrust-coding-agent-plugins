use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Hook {
    pub(super) plugin_id: Option<String>,
    pub(super) event_name: Option<String>,
    pub(super) handler_type: Option<String>,
    pub(super) command: Option<String>,
    pub(super) enabled: Option<bool>,
    pub(super) is_managed: Option<bool>,
    pub(super) source: Option<String>,
    pub(super) source_path: Option<String>,
    pub(super) trust_status: Option<String>,
    pub(super) key: Option<String>,
    pub(super) current_hash: Option<String>,
}
impl Hook {
    /// Both installed and invocation-local trust require an enabled, unmanaged
    /// command matching the exact definition the caller intended to approve.
    pub(super) fn is_enabled_command(&self, command: &str) -> bool {
        self.handler_type.as_deref() == Some("command")
            && self.command.as_deref() == Some(command)
            && self.enabled == Some(true)
            && self.is_managed == Some(false)
    }
}
