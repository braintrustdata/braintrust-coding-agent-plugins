#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]

//! Phase 2: the Braintrust sink actually delivers spans. Runs against a
//! wiremock stand-in for the Braintrust backend (the endpoints the SDK hits
//! with `skip_login`: GET /version, POST /api/project/register, POST /logs3).

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, FlushMode, SessionConfig, TraceDestination};
use bt_daemon::{
    BraintrustSinkConfig, BraintrustSinkFactory, OriginSnapshot, SinkFactory, SpanOp, SpanRow,
    SpanType,
};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn session_config(base: &str) -> SessionConfig {
    SessionConfig {
        auth: BackendAuth {
            token: "sk-test".into(),
            api_url: Some(base.to_string()),
            app_url: Some(base.to_string()),
            org_name: Some("acme".into()),
            org_id: None,
        },
        destination: Some(TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("my-project".into()),
        }),
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
        tags: Vec::new(),
        span_plugins: Vec::new(),
    }
}

fn row(
    span_id: &str,
    root: &str,
    parents: &[&str],
    name: &str,
    ty: SpanType,
    start: i64,
    end: Option<i64>,
) -> SpanRow {
    SpanRow {
        span_id: span_id.into(),
        root_span_id: root.into(),
        parent_span_ids: parents.iter().map(|s| s.to_string()).collect(),
        name: name.into(),
        span_type: ty,
        start_ms: Some(start),
        end_ms: end,
        input: None,
        output: None,
        metadata: None,
        metrics: None,
        error: None,
        late_merge_key: None,
        turn_span_id: None,
        tags: None,
        origin: Some(
            OriginSnapshot {
                plugin_version: None,
                bt_version: "test".into(),
                source_version: None,
            }
            .into(),
        ),
        is_turn: false,
    }
}

/// Mount the three endpoints the SDK hits under `skip_login`.
async fn mock_backend() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/project/register"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "project": { "id": "proj-1" } })),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    server
}

async fn logs3_bodies(server: &MockServer) -> String {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/logs3")
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

async fn logs3_rows(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/logs3")
        .flat_map(|request| {
            serde_json::from_slice::<Value>(&request.body).unwrap()["rows"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_project_id_does_not_register_a_project_name() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/project/register"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("sess-id", "antigravity").unwrap();
    let mut config = session_config(&base);
    config.destination = Some(TraceDestination::ProjectLogs {
        project_id: Some("proj-existing".into()),
        project_name: None,
    });
    sink.configure(&config);
    sink.emit(&[SpanOp::Insert(row(
        "root-id",
        "root-id",
        &[],
        "Antigravity",
        SpanType::Task,
        1,
        Some(2),
    ))])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == "/logs3"),
        "expected delivery to the existing project"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/api/project/register"),
        "an explicit project id must not trigger project registration"
    );
}

/// Two sessions on two different backend URLs, from one factory, each deliver
/// only to their own collector — the per-`(api_url, app_url)` client cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_profile_sessions_route_to_their_own_backend() {
    let server_a = mock_backend().await;
    let server_b = mock_backend().await;
    let (base_a, base_b) = (server_a.uri(), server_b.uri());

    // No daemon-level default URLs: each session brings its own (as bt does).
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: None,
        app_url: None,
    });

    let mut sink_a = factory.create("sess-a", "codex").unwrap();
    sink_a.configure(&session_config(&base_a));
    sink_a
        .emit(&[SpanOp::Insert(row(
            "span-A",
            "span-A",
            &[],
            "A",
            SpanType::Task,
            1,
            Some(2),
        ))])
        .await
        .unwrap();
    sink_a.flush().await.unwrap();

    let mut sink_b = factory.create("sess-b", "codex").unwrap();
    sink_b.configure(&session_config(&base_b));
    sink_b
        .emit(&[SpanOp::Insert(row(
            "span-B",
            "span-B",
            &[],
            "B",
            SpanType::Task,
            1,
            Some(2),
        ))])
        .await
        .unwrap();
    sink_b.flush().await.unwrap();

    let a = logs3_bodies(&server_a).await;
    let b = logs3_bodies(&server_b).await;
    assert!(a.contains("span-A"), "server A missing its span");
    assert!(!a.contains("span-B"), "server A leaked session B's span");
    assert!(b.contains("span-B"), "server B missing its span");
    assert!(!b.contains("span-A"), "server B leaked session A's span");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provisional_lifecycle_root_is_not_exported_without_work() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("empty", "codex").unwrap();
    sink.configure(&session_config(&base));

    let mut root = row("root", "root", &[], "codex", SpanType::Task, 1, None);
    root.metadata = Some(json!({"_bt_defer_root": true}));
    let closing = row("root", "root", &[], "", SpanType::Task, 1, Some(2));
    assert_eq!(
        sink.emit(&[SpanOp::Insert(root), SpanOp::Merge(closing)])
            .await
            .unwrap(),
        0
    );
    sink.flush().await.unwrap();

    assert!(logs3_bodies(&server).await.is_empty());
    assert!(
        sink.has_pending_delivery(),
        "an ordinary flush must retain the root for a later replay or work row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provisional_root_survives_a_nonterminal_flush_until_work_arrives() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("flush", "codex").unwrap();
    sink.configure(&session_config(&base));

    let mut root = row("root", "root", &[], "codex", SpanType::Task, 1, None);
    root.metadata = Some(json!({"_bt_defer_root": true}));
    assert_eq!(sink.emit(&[SpanOp::Insert(root)]).await.unwrap(), 0);
    sink.flush().await.unwrap();

    let turn = row(
        "turn",
        "root",
        &["root"],
        "Turn 1",
        SpanType::Task,
        2,
        Some(3),
    );
    assert_eq!(sink.emit(&[SpanOp::Insert(turn)]).await.unwrap(), 2);
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(bodies.contains("codex"));
    assert!(bodies.contains("Turn 1"));
    let rows = logs3_rows(&server).await;
    let root = rows.iter().find(|row| row["span_id"] == "root").unwrap();
    let origin = &root["context"]["span_origin"];
    assert_eq!(origin["name"], "braintrust.plugin.codex");
    assert_eq!(origin.get("version"), Some(&Value::Null));
    assert_eq!(origin["bt"]["version"], "test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn work_releases_only_its_direct_provisional_root() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("multi-root", "opencode").unwrap();
    sink.configure(&session_config(&base));

    let mut root_a = row("root-a", "root-a", &[], "root A", SpanType::Task, 1, None);
    root_a.metadata = Some(json!({"_bt_defer_root": true}));
    let mut root_b = row("root-b", "root-b", &[], "root B", SpanType::Task, 1, None);
    root_b.metadata = Some(json!({"_bt_defer_root": true}));
    assert_eq!(
        sink.emit(&[SpanOp::Insert(root_a), SpanOp::Insert(root_b)])
            .await
            .unwrap(),
        0
    );

    let work_b = row(
        "work-b",
        "root-b",
        &["root-b"],
        "work B",
        SpanType::Task,
        2,
        Some(3),
    );
    assert_eq!(sink.emit(&[SpanOp::Insert(work_b)]).await.unwrap(), 2);
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(bodies.contains("root B"));
    assert!(bodies.contains("work B"));
    assert!(!bodies.contains("root A"));
    assert!(sink.has_pending_delivery(), "root A should remain deferred");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provisional_root_is_exported_before_its_first_work_row() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("work", "codex").unwrap();
    sink.configure(&session_config(&base));

    let mut root = row("root", "root", &[], "codex", SpanType::Task, 1, None);
    root.metadata = Some(json!({"_bt_defer_root": true, "source": "codex"}));
    let turn = row(
        "turn",
        "root",
        &["root"],
        "Turn 1",
        SpanType::Task,
        2,
        Some(3),
    );
    assert_eq!(
        sink.emit(&[SpanOp::Insert(root), SpanOp::Insert(turn)])
            .await
            .unwrap(),
        2
    );
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(bodies.contains("codex"));
    assert!(bodies.contains("Turn 1"));
    assert!(!bodies.contains("_bt_defer_root"));
}

/// Open, terminal, and recovered updates keep their name, linkage, and origin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn span_updates_preserve_name_linkage_and_origin() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
    // Exercise every agent, with span kinds and version availability spread
    // across the cases instead of repeating every source/kind combination.
    for (source, span_type, plugin_version, source_version) in [
        (
            "codex",
            SpanType::Tool,
            Some("1.2.3"),
            Some("native-version"),
        ),
        ("claude-code", SpanType::Llm, None, Some("native-version")),
        (
            "grok",
            SpanType::Task,
            Some("1.2.3"),
            Some("native-version"),
        ),
        ("opencode", SpanType::Tool, Some("1.2.3"), None),
        ("pi", SpanType::Llm, Some("1.2.3"), Some("native-version")),
        ("cursor", SpanType::Tool, None, Some("native-version")),
        ("antigravity", SpanType::Task, None, None),
    ] {
        let span_id = format!("span-{source}");
        let origin = OriginSnapshot {
            plugin_version: plugin_version.map(str::to_owned),
            source_version: source_version.map(str::to_owned),
            bt_version: "birth-bt".into(),
        };
        let mut initial = row(
            &span_id,
            "trace-root",
            &["turn-parent"],
            "original name",
            span_type,
            1,
            None,
        );
        initial.origin = Some(origin.clone().into());
        let mut sink = factory.create(&span_id, source).unwrap();
        sink.configure(&session_config(&base));
        sink.emit(&[SpanOp::Insert(initial)]).await.unwrap();
        sink.flush().await.unwrap();

        let update = SpanRow {
            span_id: span_id.clone(),
            root_span_id: "trace-root".into(),
            parent_span_ids: vec!["turn-parent".into()],
            span_type,
            origin: Some(origin.into()),
            ..Default::default()
        };
        for (status, end_ms) in [("open", None), ("closed", Some(2)), ("late", None)] {
            sink.emit(&[SpanOp::Merge(SpanRow {
                output: Some(json!({"status": status})),
                end_ms,
                ..update.clone()
            })])
            .await
            .unwrap();
            sink.flush().await.unwrap();
        }
        drop(sink);
        // Use a new factory/client so recovery cannot retain an old handle.
        let recovered_factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
        let mut recovered = recovered_factory.create(&span_id, source).unwrap();
        recovered.configure(&session_config(&base));
        recovered
            .emit(&[SpanOp::Merge(SpanRow {
                output: Some(json!({"status": "recovered"})),
                ..update
            })])
            .await
            .unwrap();
        recovered.flush().await.unwrap();

        let rows: Vec<_> = logs3_rows(&server)
            .await
            .into_iter()
            .filter(|row| row["span_id"] == span_id)
            .collect();
        assert!(rows
            .iter()
            .any(|row| row["span_attributes"]["name"] == "original name"));
        for status in ["open", "closed", "late", "recovered"] {
            assert!(
                rows.iter().any(|row| row["output"]["status"] == status),
                "{source}: missing {status} update: {rows:?}"
            );
        }
        for row in rows {
            // Omitting a name preserves it; emitting an empty name would erase it.
            if let Some(name) = row.pointer("/span_attributes/name") {
                assert_eq!(name, "original name");
            }
            assert_eq!(row["root_span_id"], "trace-root");
            assert_eq!(row["span_parents"], json!(["turn-parent"]));
            if row.get("output").is_some() {
                assert_eq!(row["_is_merge"], true);
            }
            let origin = &row["context"]["span_origin"];
            assert_eq!(origin["name"], format!("braintrust.plugin.{source}"));
            assert_eq!(origin.get("version"), Some(&json!(plugin_version)));
            assert_eq!(origin["bt"]["version"], "birth-bt");
            assert_eq!(origin[source].get("version"), Some(&json!(source_version)));
            assert!(origin.get("agent").is_none());
            assert_eq!(origin["instrumentation"]["name"], "braintrust-plugin");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_session_can_extend_an_already_ended_root() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("sess-resumed", "antigravity").unwrap();
    sink.configure(&session_config(&base));

    let root = row(
        "resumed-root",
        "resumed-root",
        &[],
        "Antigravity",
        SpanType::Task,
        1_000,
        None,
    );
    let first_stop = row(
        "resumed-root",
        "resumed-root",
        &[],
        "",
        SpanType::Task,
        1_000,
        Some(2_000),
    );
    let resumed_stop = row(
        "resumed-root",
        "resumed-root",
        &[],
        "",
        SpanType::Task,
        1_000,
        Some(5_000),
    );
    sink.emit(&[
        SpanOp::Insert(root),
        SpanOp::Merge(first_stop),
        SpanOp::Merge(resumed_stop),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(
        bodies.contains("\"end\":5.0"),
        "later stop did not extend the root end time: {bodies}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attached_trace_children_keep_the_external_root() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("sess-1", "codex").unwrap();
    let mut config = session_config(&base);
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.object_id = Some("proj-parent".into());
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    config.destination = Some(TraceDestination::ParentSpan { components });
    sink.configure(&config);
    sink.emit(&[SpanOp::Insert(row(
        "child",
        "daemon-internal-root",
        &["daemon-parent"],
        "tool",
        SpanType::Tool,
        1,
        Some(2),
    ))])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(
        bodies.contains("external-root"),
        "child lost attached trace root: {bodies}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exported_parent_preserves_object_root_and_propagated_event() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("sess-parent", "codex").unwrap();
    let mut config = session_config(&base);
    let mut components = SpanComponents::new(SpanObjectType::Experiment);
    components.object_id = Some("exp-parent".into());
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    components.propagated_event = Some(serde_json::Map::from_iter([(
        "tenant".into(),
        json!("acme"),
    )]));
    config.destination = Some(TraceDestination::ParentSpan { components });
    sink.configure(&config);
    let mut root = row(
        "session-root",
        "daemon-internal-root",
        &["external-parent"],
        "codex",
        SpanType::Task,
        1,
        Some(2),
    );
    root.origin = Some(
        OriginSnapshot {
            plugin_version: Some("attached-plugin".into()),
            bt_version: "attached-bt".into(),
            source_version: Some("attached-codex".into()),
        }
        .into(),
    );
    sink.emit(&[SpanOp::Insert(root)]).await.unwrap();
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    for expected in [
        "exp-parent",
        "external-root",
        "external-parent",
        "tenant",
        "acme",
    ] {
        assert!(bodies.contains(expected), "{expected} absent: {bodies}");
    }
    let rows = logs3_rows(&server).await;
    let root = rows
        .iter()
        .find(|row| row["span_id"] == "session-root")
        .unwrap();
    let origin = &root["context"]["span_origin"];
    assert_eq!(origin["name"], "braintrust.plugin.codex");
    assert_eq!(origin["version"], "attached-plugin");
    assert_eq!(origin["bt"]["version"], "attached-bt");
    assert_eq!(origin["codex"]["version"], "attached-codex");
    assert!(origin.get("agent").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_merge_preserves_origin_without_losing_attached_context() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
    let mut sink = factory.create("legacy-session", "codex").unwrap();
    let mut config = session_config(&base);
    let mut components = SpanComponents::new(SpanObjectType::Experiment);
    components.object_id = Some("exp-parent".into());
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    components.propagated_event = Some(serde_json::Map::from_iter([
        ("tenant".into(), json!("acme")),
        (
            "context".into(),
            json!({
                "request_id": "request-1",
                "span_origin": {"name": "parent-instrumentation", "version": "parent-version"}
            }),
        ),
    ]));
    config.destination = Some(TraceDestination::ParentSpan { components });
    sink.configure(&config);
    sink.emit(&[SpanOp::Merge(SpanRow {
        span_id: "legacy-root".into(),
        root_span_id: "internal-root".into(),
        parent_span_ids: vec!["external-parent".into()],
        output: Some(json!("late output")),
        end_ms: Some(2_000),
        ..Default::default()
    })])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let rows = logs3_rows(&server).await;
    let update = rows
        .iter()
        .find(|row| row["span_id"] == "legacy-root")
        .unwrap();
    assert_eq!(update["_is_merge"], true);
    assert_eq!(update["output"], "late output");
    assert_eq!(update["metrics"]["end"], 2.0);
    assert_eq!(update["experiment_id"], "exp-parent");
    assert_eq!(update["root_span_id"], "external-root");
    assert_eq!(update["span_parents"], json!(["external-parent"]));
    assert_eq!(update["tenant"], "acme");
    assert_eq!(update["context"], json!({"request_id": "request-1"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn braintrust_sink_delivers_spans_to_collector() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });

    let mut sink = factory.create("sess-1", "codex").unwrap();
    sink.configure(&session_config(&base));

    // A session root (task) and a child tool span under it.
    let root = row(
        "rootspan1",
        "rootspan1",
        &[],
        "codex: sess-1",
        SpanType::Task,
        1000,
        None,
    );
    let mut tool = row(
        "toolspan1",
        "rootspan1",
        &["rootspan1"],
        "shell",
        SpanType::Tool,
        1001,
        Some(1002),
    );
    tool.input = Some(json!({ "command": "ls" }));
    tool.output = Some(json!("ok"));

    sink.emit(&[SpanOp::Insert(root), SpanOp::Insert(tool)])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    let rows = logs3_rows(&server).await;
    let root = rows
        .iter()
        .find(|row| row["span_id"] == "rootspan1")
        .unwrap();
    assert_eq!(root["span_attributes"]["name"], "codex: sess-1");
    let tool = rows
        .iter()
        .find(|row| row["span_id"] == "toolspan1")
        .unwrap();
    assert_eq!(tool["root_span_id"], "rootspan1");
    assert_eq!(tool["span_parents"], json!(["rootspan1"]));
    assert_eq!(tool["input"], json!({"command": "ls"}));
    assert_eq!(tool["output"], "ok");

    // Project registration happened (org_name path, no login).
    assert!(
        requests
            .iter()
            .any(|r| r.url.path() == "/api/project/register"),
        "expected project registration"
    );
    // skip_login: no apikey login call.
    assert!(
        !requests.iter().any(|r| r.url.path() == "/api/apikey/login"),
        "should not have called login with skip_login"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn experiment_sessions_use_experiment_object_type_and_id() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
    });
    let mut sink = factory.create("sess-exp", "claude-code").unwrap();
    let mut config = session_config(&base);
    config.destination = Some(TraceDestination::Experiment {
        experiment_id: "exp-42".into(),
    });
    config.additional_metadata = Some(json!({"_bt_experiment_id":"legacy-exp"}));
    sink.configure(&config);
    sink.emit(&[
        SpanOp::Insert(row(
            "exp-root",
            "exp-root",
            &[],
            "Claude Code",
            SpanType::Task,
            1,
            None,
        )),
        SpanOp::Insert(row(
            "exp-child",
            "exp-root",
            &["exp-root"],
            "Turn 1",
            SpanType::Task,
            2,
            Some(3),
        )),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(bodies.contains("exp-42"), "experiment id absent: {bodies}");
    assert!(
        !bodies.contains("legacy-exp"),
        "legacy routing overrode typed destination: {bodies}"
    );
    assert!(
        !bodies.contains("\"project_id\""),
        "experiment spans were routed as project logs: {bodies}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_backend_batch_is_not_a_successful_flush() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid row"))
        .mount(&server)
        .await;
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
    let mut sink = factory.create("rejected", "debug").unwrap();
    let mut config = session_config(&server.uri());
    config.destination = Some(TraceDestination::ProjectLogs {
        project_id: Some("project".into()),
        project_name: None,
    });
    sink.configure(&config);
    sink.emit(&[SpanOp::Insert(row(
        "rejected",
        "rejected",
        &[],
        "rejected",
        SpanType::Task,
        1,
        Some(2),
    ))])
    .await
    .unwrap();
    let flushed = sink.flush().await;
    assert!(
        flushed.is_err(),
        "a rejected SDK batch must hold completion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_route_does_not_poison_a_sibling_flush_on_the_same_backend() {
    use wiremock::matchers::header;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .and(header("authorization", "Bearer sk-bad"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .and(header("authorization", "Bearer sk-good"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
    let mut bad = factory.create("bad", "debug").unwrap();
    let mut good = factory.create("good", "debug").unwrap();
    for (sink, token) in [(&mut bad, "sk-bad"), (&mut good, "sk-good")] {
        let mut config = session_config(&server.uri());
        config.auth.token = token.into();
        config.destination = Some(TraceDestination::ProjectLogs {
            project_id: Some("project".into()),
            project_name: None,
        });
        sink.configure(&config);
        sink.emit(&[SpanOp::Insert(row(
            token,
            token,
            &[],
            token,
            SpanType::Task,
            1,
            Some(2),
        ))])
        .await
        .unwrap();
    }
    let (bad_result, good_result) = tokio::join!(bad.flush(), good.flush());
    assert!(bad_result.is_err());
    good_result.unwrap();
}
