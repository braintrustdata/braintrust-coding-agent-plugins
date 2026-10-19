use super::Stage;
use crate::{span_processor, translate::SpanOp};
use std::path::PathBuf;

/// The outcome says what happened; the caller chooses the next stage.
pub(crate) enum PluginOutcome {
    Prepared(Box<SpanOp>),
    Filtered,
    Failed(PluginRejected),
    Unavailable(anyhow::Error),
}

pub(crate) struct Plugins {
    paths: Vec<PathBuf>,
    pub source: String,
    pub session_id: String,
    blocked: Option<(Vec<PathBuf>, PluginRejected)>,
}

#[derive(Debug, Clone)]
pub(crate) struct PluginRejected {
    pub failure: crate::span_processor::PluginFailure,
    pub digest: Option<[u8; 32]>,
    pub processors: Vec<Option<[u8; 32]>>,
}
impl std::fmt::Display for PluginRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "span plugin {} failed; delivery is held: {}",
            self.failure.path.display(),
            self.failure.message
        )
    }
}
impl std::error::Error for PluginRejected {}

impl Plugins {
    pub fn new(source: &str, session_id: &str) -> Self {
        Self {
            paths: Vec::new(),
            source: source.into(),
            session_id: session_id.into(),
            blocked: None,
        }
    }

    pub fn configure(&mut self, paths: &[PathBuf]) {
        self.paths.clear();
        self.paths.extend_from_slice(paths);
    }
}

#[async_trait::async_trait]
impl Stage for Plugins {
    type Input = SpanOp;
    type Outcome = PluginOutcome;

    async fn process(&mut self, input: &SpanOp) -> PluginOutcome {
        if let Some((paths, failure)) = &self.blocked {
            if paths == &self.paths
                && paths
                    .iter()
                    .map(|path| span_processor::file_digest(path))
                    .collect::<Vec<_>>()
                    == failure.processors
            {
                let mut failure = failure.clone();
                failure.failure.newly_seen = false;
                return PluginOutcome::Failed(failure);
            }
        }
        self.blocked = None;
        match span_processor::process_isolated(&self.paths, input, &self.source, &self.session_id) {
            Ok(result) => match (result.op, result.failure) {
                (_, Some(failure)) => {
                    let digest = span_processor::failed_digest(&failure.path);
                    let processors = span_processor::chain_digests(&self.paths, &failure.path);
                    let failure = PluginRejected {
                        failure,
                        digest,
                        processors,
                    };
                    self.blocked = Some((self.paths.clone(), failure.clone()));
                    PluginOutcome::Failed(failure)
                }
                (Some(op), None) => PluginOutcome::Prepared(Box::new(op)),
                (None, None) => PluginOutcome::Filtered,
            },
            Err(error) => PluginOutcome::Unavailable(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanRow;
    #[tokio::test]
    async fn failure_is_local_to_one_route_and_equal_stat_edit_reloads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("conditional.mjs");
        let failing =
            "export default span => {if(span.name==='bad') throw new Error('bad'); return span}";
        std::fs::write(&path, failing).unwrap();
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut bad = Plugins::new("debug", "bad");
        bad.paths = vec![path.clone()];
        let bad_op = SpanOp::Insert(SpanRow {
            span_id: "one".into(),
            name: "bad".into(),
            ..Default::default()
        });
        assert!(matches!(
            bad.process(&bad_op).await,
            PluginOutcome::Failed(_)
        ));
        let mut healthy = Plugins::new("debug", "healthy");
        healthy.paths = vec![path.clone()];
        assert!(matches!(
            healthy
                .process(&SpanOp::Insert(SpanRow {
                    name: "good".into(),
                    ..Default::default()
                }))
                .await,
            PluginOutcome::Prepared(_)
        ));
        std::fs::write(&path, failing.replace("==='bad'", "==='foo'")).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            modified
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            failing.len() as u64
        );
        assert!(matches!(
            bad.process(&bad_op).await,
            PluginOutcome::Prepared(_)
        ));
    }
}
