//! Route overlay: applies one route's destination and settings to the
//! route-neutral rows a translator emits.
//!
//! Translators never read route configuration. They mark the places where a
//! route's values belong ([`RouteSlots`]) and emit rows rooted at their own
//! session root. Each delivery branch fills those slots for its route, so one
//! translation is shared by every route of a session.

use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow};
use crate::wire::{SessionConfig, TraceDestination};
use serde_json::{Map, Value};

/// The route values an overlay applies.
#[derive(Debug, Clone, Default)]
pub struct RouteOverlay {
    attached_parent: Option<String>,
    attached_root: Option<String>,
    additional_metadata: Map<String, Value>,
    tags: Vec<String>,
    project: Option<String>,
}

impl RouteOverlay {
    pub fn new(
        destination: Option<&TraceDestination>,
        additional_metadata: Option<&Value>,
        tags: &[String],
    ) -> Self {
        let (attached_parent, attached_root) = match destination {
            Some(TraceDestination::ParentSpan { components }) => {
                (components.span_id.clone(), components.root_span_id.clone())
            }
            _ => (None, None),
        };
        let mut metadata = additional_metadata
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        // Internal routing settings must never appear as user metadata.
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        Self {
            attached_root: attached_root.or_else(|| attached_parent.clone()),
            attached_parent,
            additional_metadata: metadata,
            tags: tags.to_vec(),
            project: destination
                .and_then(TraceDestination::project_name)
                .map(str::to_owned),
        }
    }

    pub fn from_config(config: &SessionConfig) -> Self {
        Self::new(
            config.destination.as_ref(),
            config.additional_metadata.as_ref(),
            &config.tags,
        )
    }

    /// Apply this route to one route-neutral row.
    pub fn apply_row(&self, row: &mut SpanRow) {
        if let Some(root) = &self.attached_root {
            row.root_span_id = root.clone();
        }
        let slots = row.route_slots;
        if slots.attach {
            if let Some(parent) = &self.attached_parent {
                if row.parent_span_ids.is_empty() {
                    row.parent_span_ids = vec![parent.clone()];
                }
            }
        }
        if slots.metadata && !self.additional_metadata.is_empty() {
            let mut merged = self.additional_metadata.clone();
            if let Some(own) = row.metadata.as_ref().and_then(Value::as_object) {
                // The translator's own keys win over user-supplied metadata.
                for (key, value) in own {
                    merged.insert(key.clone(), value.clone());
                }
            }
            row.metadata = Some(Value::Object(merged));
        }
        if slots.project {
            if let Some(project) = &self.project {
                let metadata = row
                    .metadata
                    .get_or_insert_with(|| Value::Object(Map::new()));
                if let Some(metadata) = metadata.as_object_mut() {
                    metadata.insert("project".into(), Value::String(project.clone()));
                }
            }
        }
        if slots.tags && !self.tags.is_empty() {
            row.tags = Some(self.tags.clone());
        }
    }

    pub fn apply(&self, op: &mut SpanOp) {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        self.apply_row(row);
    }

    pub fn apply_all(&self, mut ops: Vec<SpanOp>) -> Vec<SpanOp> {
        for op in &mut ops {
            self.apply(op);
        }
        ops
    }
}

/// Applies the overlay for a caller-supplied [`SessionCtx::config`].
///
/// The pipeline never supplies a config to translators; it overlays each
/// branch itself. Direct callers that pass a config, such as translator unit
/// tests, receive the same rows a branch with that route would deliver.
pub(crate) struct ConfiguredOverlay {
    inner: Box<dyn AgentTranslator>,
}

impl ConfiguredOverlay {
    pub(crate) fn new(inner: Box<dyn AgentTranslator>) -> Self {
        Self { inner }
    }
}

fn overlay(ctx: &SessionCtx, ops: Vec<SpanOp>) -> Vec<SpanOp> {
    match &ctx.config {
        Some(config) => RouteOverlay::from_config(config).apply_all(ops),
        None => ops,
    }
}

fn neutral(ctx: &SessionCtx) -> SessionCtx {
    SessionCtx {
        session_id: ctx.session_id.clone(),
        config: None,
    }
}

impl AgentTranslator for ConfiguredOverlay {
    fn handle(
        &mut self,
        event: &crate::wire::Envelope,
        ctx: &SessionCtx,
    ) -> anyhow::Result<Vec<SpanOp>> {
        Ok(overlay(ctx, self.inner.handle(event, &neutral(ctx))?))
    }

    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        Ok(self
            .inner
            .drain_pending(&neutral(ctx))?
            .map(|ops| overlay(ctx, ops)))
    }

    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(overlay(ctx, self.inner.checkpoint(&neutral(ctx))?))
    }

    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(overlay(ctx, self.inner.finalize(&neutral(ctx))?))
    }

    fn flush(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(overlay(ctx, self.inner.flush(&neutral(ctx))?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::RouteSlots;
    use braintrust_sdk_rust::{SpanComponents, SpanObjectType};

    fn attached() -> TraceDestination {
        TraceDestination::ParentSpan {
            components: SpanComponents {
                object_type: SpanObjectType::ProjectLogs,
                object_id: Some("project".into()),
                compute_object_metadata_args: None,
                row_id: None,
                span_id: Some("parent".into()),
                root_span_id: Some("root".into()),
                span_parents: None,
                propagated_event: None,
            },
        }
    }

    #[test]
    fn fills_only_the_marked_slots() {
        let overlay = RouteOverlay::new(
            Some(&attached()),
            Some(&serde_json::json!({"team": "x", "source": "user", "_bt_internal": 1})),
            &["t".into()],
        );
        let mut root = SpanRow {
            span_id: "session".into(),
            root_span_id: "session".into(),
            metadata: Some(serde_json::json!({"source": "codex"})),
            route_slots: RouteSlots::ROOT,
            ..Default::default()
        };
        overlay.apply_row(&mut root);
        assert_eq!(root.root_span_id, "root");
        assert_eq!(root.parent_span_ids, vec!["parent".to_string()]);
        assert_eq!(
            root.metadata,
            Some(serde_json::json!({"team": "x", "source": "codex"}))
        );
        assert_eq!(root.tags, Some(vec!["t".to_string()]));

        let mut child = SpanRow {
            span_id: "child".into(),
            root_span_id: "session".into(),
            parent_span_ids: vec!["session".into()],
            ..Default::default()
        };
        overlay.apply_row(&mut child);
        assert_eq!(child.root_span_id, "root");
        assert_eq!(child.parent_span_ids, vec!["session".to_string()]);
        assert!(child.metadata.is_none() && child.tags.is_none());
    }
}
