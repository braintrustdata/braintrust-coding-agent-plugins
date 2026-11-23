use crate::json::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SessionMetadata {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub cwd: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub model: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub provider: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub permission_mode: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub trajectory_id: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub root_session_id: Field<String>,
}

impl SessionMetadata {
    pub fn update(&mut self, incoming: &Self) {
        macro_rules! update { ($($field:ident),+) => { $(if !incoming.$field.is_missing() { self.$field.clone_from(&incoming.$field); })+ }; }
        update!(
            cwd,
            model,
            provider,
            permission_mode,
            trajectory_id,
            root_session_id
        );
    }
}
