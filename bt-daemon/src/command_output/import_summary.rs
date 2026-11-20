use crate::wire::TraceDestination;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ImportSummary {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<TraceDestination>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_span_id: Option<String>,
    pub span_count: usize,
    pub finalized: bool,
}

impl ImportSummary {
    pub(super) fn render_human(&self) -> String {
        let destination = self
            .destination
            .as_ref()
            .map(render_destination)
            .unwrap_or_else(|| "the configured destination".into());
        let root = self
            .root_span_id
            .as_deref()
            .map(|id| format!(", root span {id}"))
            .unwrap_or_default();
        format!(
            "Imported session {} to {}: {} spans{}.",
            self.session_id, destination, self.span_count, root
        )
    }
}

fn render_destination(destination: &TraceDestination) -> String {
    match destination {
        TraceDestination::ProjectLogs {
            project_id,
            project_name,
        } => project_name
            .as_ref()
            .map(|name| format!("project {name}"))
            .or_else(|| project_id.as_ref().map(|id| format!("project {id}")))
            .unwrap_or_else(|| "project logs".into()),
        TraceDestination::Experiment { experiment_id } => {
            format!("experiment {experiment_id}")
        }
        TraceDestination::ParentSpan { components } => components
            .span_id
            .as_ref()
            .map(|id| format!("parent span {id}"))
            .unwrap_or_else(|| "a parent span".into()),
    }
}
