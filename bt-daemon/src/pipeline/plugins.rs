use super::Stage;
use crate::{span_processor, translate::SpanOp};
use std::path::PathBuf;

/// The outcome says what happened; the caller chooses the next stage.
pub(crate) enum PluginOutcome {
    Prepared(Box<SpanOp>),
    Filtered,
    Failed(span_processor::PluginFailure),
    Unavailable(anyhow::Error),
}

pub(crate) struct Plugins {
    pub paths: Vec<PathBuf>,
    pub source: String,
    pub session_id: String,
}

#[async_trait::async_trait]
impl Stage for Plugins {
    type Input = SpanOp;
    type Outcome = PluginOutcome;

    async fn process(&mut self, input: &SpanOp) -> PluginOutcome {
        match span_processor::process(&self.paths, input, &self.source, &self.session_id) {
            Ok(result) => match (result.op, result.failure) {
                (_, Some(failure)) => PluginOutcome::Failed(failure),
                (Some(op), None) => PluginOutcome::Prepared(Box::new(op)),
                (None, None) => PluginOutcome::Filtered,
            },
            Err(error) => PluginOutcome::Unavailable(error),
        }
    }
}
