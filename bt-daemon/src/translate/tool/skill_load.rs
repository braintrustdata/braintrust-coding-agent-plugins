//! The identity and common span representation of a skill load.
use super::skill_was_requested;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// A tool call that loaded a skill, as far as the source reveals it.
#[derive(Default, Serialize, Deserialize)]
pub struct SkillLoad {
    pub name: Option<String>,
    pub path: Option<String>,
}

impl SkillLoad {
    pub fn display_name(&self) -> String {
        self.name
            .as_ref()
            .map_or_else(|| "skill".into(), |name| format!("skill: {name}"))
    }

    pub fn was_requested(&self, requested: &[String]) -> bool {
        self.name
            .as_deref()
            .is_some_and(|name| skill_was_requested(requested, name))
    }

    /// Only a load the user asked for carries an explicit trigger.
    pub fn insert_metadata(&self, metadata: &mut Map<String, Value>, requested: &[String]) {
        metadata.insert("tool_kind".into(), json!("skill"));
        if let Some(name) = &self.name {
            metadata.insert("skill_name".into(), json!(name));
            if self.was_requested(requested) {
                metadata.insert("skill_load_trigger".into(), json!("explicit"));
            }
        }
        if let Some(path) = &self.path {
            metadata.insert("skill_path".into(), json!(path));
        }
    }
}
