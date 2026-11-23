use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct Plugin {
    pub(super) id: String,
    pub(super) installed: bool,
    pub(super) enabled: bool,
}
impl Plugin {
    pub(super) fn is_active(&self, id: &str) -> bool {
        self.id == id && self.installed && self.enabled
    }
}
