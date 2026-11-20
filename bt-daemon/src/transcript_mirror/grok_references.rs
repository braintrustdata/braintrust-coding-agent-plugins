use super::Reference;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct GrokReferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updates: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<Reference>,
}

impl GrokReferences {
    pub fn is_empty(&self) -> bool {
        self.updates.is_none() && self.events.is_none() && self.system_prompt.is_none()
    }
}
