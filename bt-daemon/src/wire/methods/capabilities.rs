use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Capabilities {
    #[serde(default)]
    pub sources: Vec<String>,
    /// A failed event closes the connection before any later event is accepted.
    #[serde(default)]
    pub ordered_event_pipeline: bool,
}
