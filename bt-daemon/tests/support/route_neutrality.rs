//! Route neutrality: a translator's rows must not depend on route settings.
//! Each delivery route applies its own [`RouteOverlay`] to the shared rows.
//! Include `span_identity` alongside this module.

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, FlushMode, SessionConfig, TraceDestination};
use bt_daemon::{AgentTranslator, Registry, RouteOverlay, SessionCtx, SpanOp};
use serde_json::json;

pub(crate) const ROUTE_PARENT: &str = "route-parent";
pub(crate) const ROUTE_ROOT: &str = "route-root";

fn config(
    destination: TraceDestination,
    metadata: serde_json::Value,
    tags: &[&str],
) -> SessionConfig {
    SessionConfig {
        auth: BackendAuth {
            token: "test-token".into(),
            api_url: None,
            app_url: None,
            org_name: None,
            org_id: None,
        },
        destination: Some(destination),
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: Some(metadata),
        tags: tags.iter().map(|tag| tag.to_string()).collect(),
        span_plugins: Vec::new(),
    }
}

/// A route attached under an existing span, and a project-logs route with
/// different metadata and tags. User metadata deliberately collides with
/// translator-owned keys and includes an internal `_bt_` key.
pub(crate) fn routes() -> [SessionConfig; 2] {
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.object_id = Some("attached-project".into());
    components.span_id = Some(ROUTE_PARENT.into());
    components.root_span_id = Some(ROUTE_ROOT.into());
    [
        config(
            TraceDestination::ParentSpan { components },
            json!({"team": "a", "source": "user", "session_id": "user", "_bt_internal": true}),
            &["route-a"],
        ),
        config(
            TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("route-b-project".into()),
            },
            json!({"team": "b", "project": "user-project"}),
            &["route-b", "second"],
        ),
    ]
}

fn ctx(session_id: &str, config: Option<SessionConfig>) -> SessionCtx {
    SessionCtx {
        session_id: session_id.into(),
        config,
    }
}

fn rendered(ops: &[SpanOp]) -> Vec<String> {
    ops.iter().map(|op| format!("{op:?}")).collect()
}

/// Drive one fresh translator per run with `drive`, and assert that:
/// * the route-neutral translator emits identical rows with no config and with
///   each route's config, so its state never reads route settings;
/// * overlaying each route on the neutral rows reproduces exactly what the
///   registry's configured translator emits for that route.
///
/// Returns the neutral rows and the attached route's rows for extra checks.
pub(crate) fn assert_route_neutral<F>(
    source: &str,
    session_id: &str,
    mut drive: F,
) -> (Vec<SpanOp>, Vec<SpanOp>)
where
    F: FnMut(&mut dyn AgentTranslator, &SessionCtx) -> Vec<SpanOp>,
{
    let registry = Registry::default_agents();
    let neutral = drive(
        registry.create_unrouted(source, session_id).as_mut(),
        &ctx(session_id, None),
    );
    assert!(!neutral.is_empty(), "{source}: scenario emitted no rows");
    let mut attached = Vec::new();
    for (index, route) in routes().into_iter().enumerate() {
        let unrouted = drive(
            registry.create_unrouted(source, session_id).as_mut(),
            &ctx(session_id, Some(route.clone())),
        );
        assert_eq!(
            rendered(&unrouted),
            rendered(&neutral),
            "{source}: route {index} changed route-neutral rows"
        );
        let routed = drive(
            registry.create(source, session_id).as_mut(),
            &ctx(session_id, Some(route.clone())),
        );
        let overlaid = RouteOverlay::from_config(&route).apply_all(neutral.clone());
        assert_eq!(
            rendered(&routed),
            rendered(&overlaid),
            "{source}: overlay of route {index} differs from configured output"
        );
        super::span_identity::assert_merges_preserve_insert_identity(&routed);
        if index == 0 {
            attached = routed;
        }
    }
    for op in &neutral {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        assert!(
            row.metadata
                .as_ref()
                .and_then(|metadata| metadata.get("_bt_defer_root"))
                .is_none(),
            "{source}: deferral must be a row flag, not metadata"
        );
        assert!(row
            .tags
            .as_ref()
            .is_none_or(|tags| !tags.iter().any(|tag| tag.starts_with("route-"))));
    }
    for op in &attached {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        assert_eq!(row.root_span_id, ROUTE_ROOT, "{source}: {row:?}");
    }
    assert!(
        attached.iter().any(|op| matches!(
            op,
            SpanOp::Insert(row) if row.parent_span_ids == [ROUTE_PARENT]
        )),
        "{source}: no top-level row was attached to the route parent"
    );
    (neutral, attached)
}
