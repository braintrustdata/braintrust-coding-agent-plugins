use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct StopCommandOutput {
    pub running: bool,
    pub stopped: bool,
}

impl StopCommandOutput {
    pub(super) fn render_human(&self) -> String {
        if self.stopped {
            "Tracing daemon stopped."
        } else {
            "No tracing daemon is running."
        }
        .into()
    }
}
