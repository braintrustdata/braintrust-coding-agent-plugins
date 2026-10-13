//! Route overlay is independent of native translation and never mutates shared ops.
use crate::{
    translate::{SpanOp, SpanType},
    wire::SessionConfig,
};
use std::collections::HashSet;

pub(crate) struct Prepare {
    source: String,
    roots: HashSet<String>,
}

impl Prepare {
    pub fn new(source: &str) -> Self {
        Self {
            source: source.into(),
            roots: HashSet::new(),
        }
    }

    pub fn apply(&mut self, ops: &mut [SpanOp], config: &SessionConfig) {
        let (parent, root) = config.attached_span_ids();
        let root = root.or_else(|| parent.clone());
        for op in ops {
            let insert = matches!(op, SpanOp::Insert(_));
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            let is_root = self.roots.contains(&row.span_id)
                || (row.span_type == SpanType::Task
                    && row.parent_span_ids.is_empty()
                    && row.span_id == row.root_span_id);
            if is_root {
                self.roots.insert(row.span_id.clone());
            }
            if let Some(root) = &root {
                row.root_span_id = root.clone();
            }
            if is_root {
                if let Some(parent) = &parent {
                    row.parent_span_ids = vec![parent.clone()];
                }
                if insert {
                    let metadata = row.metadata.get_or_insert_with(|| serde_json::json!({}));
                    if let Some(metadata) = metadata.as_object_mut() {
                        if let Some(extra) = config
                            .additional_metadata
                            .as_ref()
                            .and_then(|v| v.as_object())
                        {
                            for (key, value) in extra {
                                if !key.starts_with("_bt_") {
                                    metadata.entry(key.clone()).or_insert_with(|| value.clone());
                                }
                            }
                        }
                        if self.source == "codex" {
                            if let Some(project) = config.project_name() {
                                metadata.insert("project".into(), project.into());
                            }
                        }
                    }
                    if !config.tags.is_empty() {
                        row.tags = Some(config.tags.clone());
                    }
                }
            }
        }
    }
}

/// Retain the public translator/import API: route settings are applied here,
/// outside the native translators, while the daemon uses canonical output directly.
pub(crate) struct ConfiguredTranslator {
    inner: Box<dyn crate::translate::AgentTranslator>,
    prepare: Prepare,
}

impl ConfiguredTranslator {
    pub fn new(inner: Box<dyn crate::translate::AgentTranslator>, source: &str) -> Self {
        Self {
            inner,
            prepare: Prepare::new(source),
        }
    }
    fn apply(&mut self, mut ops: Vec<SpanOp>, ctx: &crate::translate::SessionCtx) -> Vec<SpanOp> {
        if let Some(config) = &ctx.config {
            self.prepare.apply(&mut ops, config);
        }
        ops
    }
}

impl crate::translate::AgentTranslator for ConfiguredTranslator {
    fn handle(
        &mut self,
        env: &crate::wire::Envelope,
        ctx: &crate::translate::SessionCtx,
    ) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.handle(env, ctx)?;
        Ok(self.apply(ops, ctx))
    }
    fn drain_pending(
        &mut self,
        ctx: &crate::translate::SessionCtx,
    ) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let ops = self.inner.drain_pending(ctx)?;
        Ok(ops.map(|ops| self.apply(ops, ctx)))
    }
    fn checkpoint(&mut self, ctx: &crate::translate::SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.checkpoint(ctx)?;
        Ok(self.apply(ops, ctx))
    }
    fn finalize(&mut self, ctx: &crate::translate::SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.finalize(ctx)?;
        Ok(self.apply(ops, ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        translate::{Registry, SessionCtx},
        wire::{BackendAuth, Envelope, SessionRoute},
    };

    #[test]
    fn canonical_translation_does_not_depend_on_route_customization() {
        let registry = Registry::default_agents();
        let env = Envelope {
            source: "claude-code".into(),
            source_version: Some("test".into()),
            plugin_version: Some("test".into()),
            session_id: "neutral".into(),
            event: "UserPromptSubmit".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            route: None,
            config: None,
            payload: serde_json::json!({"cwd": "/workspace", "session_id": "neutral", "prompt": "hello"}),
        };
        let plain = SessionCtx {
            session_id: "neutral".into(),
            config: None,
        };
        let config = SessionRoute {
            tags: vec!["route-tag".into()],
            additional_metadata: Some(
                serde_json::json!({"custom": true, "source": "wrong", "_bt_private": true}),
            ),
            ..SessionRoute::default()
        }
        .with_auth(BackendAuth {
            token: "not-persisted".into(),
            api_url: None,
            app_url: None,
            org_name: None,
            org_id: None,
        });
        let customized = SessionCtx {
            session_id: "neutral".into(),
            config: Some(config.clone()),
        };
        let mut a = registry
            .create_checked_with_session_namespace("claude-code", "neutral")
            .unwrap();
        let mut b = registry
            .create_checked_with_session_namespace("claude-code", "neutral")
            .unwrap();
        let canonical = a.handle(&env, &plain).unwrap();
        assert!(!canonical.is_empty());
        let other = b.handle(&env, &customized).unwrap();
        assert_eq!(
            serde_json::to_value(&canonical).unwrap(),
            serde_json::to_value(other).unwrap()
        );
        let mut routed = canonical.clone();
        Prepare::new("claude-code").apply(&mut routed, &config);
        let (SpanOp::Insert(root) | SpanOp::Merge(root)) = &routed[0];
        assert_eq!(root.tags.as_ref().unwrap(), &["route-tag"]);
        let metadata = root.metadata.as_ref().unwrap();
        assert_eq!(metadata["custom"], true);
        assert_eq!(metadata["source"], "claude-code");
        assert!(metadata.get("_bt_private").is_none());
        let (SpanOp::Insert(root) | SpanOp::Merge(root)) = &canonical[0];
        assert!(root.tags.is_none());
        assert!(root.metadata.as_ref().unwrap().get("custom").is_none());
    }
}
