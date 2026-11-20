use braintrust_sdk_rust::SpanComponents;
use serde::{Deserialize, Serialize};

/// Where a session's root span should be logged.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TraceDestination {
    /// Project logs selected by stable id, display name, or both.
    ProjectLogs {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_name: Option<String>,
    },
    /// An existing experiment.
    Experiment { experiment_id: String },
    /// A child of an exported Braintrust span.
    ParentSpan { components: SpanComponents },
}

impl std::str::FromStr for TraceDestination {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, id) = value.split_once(':').ok_or_else(|| {
            "destination must be project_logs:<project-id> or experiment:<experiment-id>"
                .to_string()
        })?;
        if id.is_empty() {
            return Err("destination id must not be empty".to_string());
        }
        match kind {
            "project_logs" => Ok(Self::ProjectLogs {
                project_id: Some(id.to_string()),
                project_name: None,
            }),
            "experiment" => Ok(Self::Experiment {
                experiment_id: id.to_string(),
            }),
            _ => Err(format!(
                "unsupported destination {kind:?}; expected project_logs or experiment"
            )),
        }
    }
}

impl TraceDestination {
    pub fn project_name(&self) -> Option<&str> {
        match self {
            Self::ProjectLogs { project_name, .. } => project_name.as_deref(),
            _ => None,
        }
    }
}
