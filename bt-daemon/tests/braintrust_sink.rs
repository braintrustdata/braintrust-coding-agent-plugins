//! Phase 2: the Braintrust sink actually delivers spans. Runs against a
//! wiremock stand-in for the Braintrust backend (the endpoints the SDK hits
//! with `skip_login`: GET /version, POST /api/project/register, POST /logs3).

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, FlushMode, SessionConfig, TraceDestination};
use bt_daemon::{
    BraintrustSinkConfig, BraintrustSinkFactory, SinkFactory, SpanOp, SpanRow, SpanType,
};
use serde_json::json;
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
        destination: None,
        project: Some("my-project".into()),
        parent_span_id: None,
        root_span_id: None,
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
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
        tags: None,
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
        version: "test".into(),
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

/// Regression: an `Insert` that names a span, followed by a `Merge` that
/// doesn't (the common "close/annotate" pattern, which builds `SpanRow` with
/// `..Default::default()` and an empty `name`), must not clobber the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_with_empty_name_does_not_clobber_the_original_name() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
        version: "test".into(),
    });
    let mut sink = factory.create("sess-1", "codex").unwrap();
    sink.configure(&session_config(&base));

    let named = row("s1", "s1", &[], "codex: myapp", SpanType::Task, 1, None);
    let mut closing = row("s1", "s1", &[], "", SpanType::Task, 1, Some(2));
    closing.name = String::new(); // as produced by `..Default::default()`

    sink.emit(&[SpanOp::Insert(named), SpanOp::Merge(closing)])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let bodies = logs3_bodies(&server).await;
    assert!(
        bodies.contains("codex: myapp"),
        "name lost after merge: {bodies}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attached_trace_children_keep_the_external_root() {
    let server = mock_backend().await;
    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
        version: "test".into(),
    });
    let mut sink = factory.create("sess-1", "codex").unwrap();
    let mut config = session_config(&base);
    config.parent_span_id = Some("external-parent".into());
    config.root_span_id = Some("external-root".into());
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
        version: "test".into(),
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
    sink.emit(&[SpanOp::Insert(row(
        "session-root",
        "daemon-internal-root",
        &["external-parent"],
        "codex",
        SpanType::Task,
        1,
        Some(2),
    ))])
    .await
    .unwrap();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn braintrust_sink_delivers_spans_to_collector() {
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

    let base = server.uri();
    let factory = BraintrustSinkFactory::new(BraintrustSinkConfig {
        api_url: Some(base.clone()),
        app_url: Some(base.clone()),
        version: "test".into(),
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
    let tool = row(
        "toolspan1",
        "rootspan1",
        &["rootspan1"],
        "shell",
        SpanType::Tool,
        1001,
        Some(1002),
    );
    let mut tool = tool;
    tool.input = Some(json!({ "command": "ls" }));
    tool.output = Some(json!("ok"));

    sink.emit(&[SpanOp::Insert(root), SpanOp::Insert(tool)])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    let logs: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path() == "/logs3")
        .collect();
    assert!(!logs.is_empty(), "expected at least one POST /logs3");

    let bodies: String = logs
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect::<Vec<_>>()
        .join("\n");

    // Both spans, our deterministic ids, and the parent linkage made it into a
    // logs3 payload.
    assert!(
        bodies.contains("rootspan1"),
        "root span id missing from logs3 body"
    );
    assert!(
        bodies.contains("toolspan1"),
        "tool span id missing from logs3 body"
    );
    assert!(bodies.contains("codex: sess-1"), "root span name missing");
    assert!(bodies.contains("\"command\""), "tool input missing");

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
        version: "test".into(),
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
