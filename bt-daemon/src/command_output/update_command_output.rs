use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct UpdateCommandOutput {
    pub source: String,
    pub display_name: String,
    pub restart_required: bool,
}

impl UpdateCommandOutput {
    pub(super) fn render_human(&self) -> String {
        format!("The Braintrust tracing plugin was updated for {}.\nRestart the coding agent to load the update.", self.display_name)
    }
}
