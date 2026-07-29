use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct CollectorServer {
    uri: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl CollectorServer {
    async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind trace collector");
        let address = listener.local_addr().expect("read trace collector address");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            uri: format!("http://{address}"),
            shutdown: Some(shutdown),
            task,
        }
    }
}

impl Drop for CollectorServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

#[derive(Default)]
struct CollectorState {
    rows: Mutex<Vec<Value>>,
    registrations: AtomicUsize,
    log_requests: AtomicUsize,
}

pub struct TraceCollector {
    server: CollectorServer,
    state: Arc<CollectorState>,
}

impl TraceCollector {
    pub async fn start() -> Self {
        let state = Arc::new(CollectorState::default());
        let router = Router::new()
            .route("/version", get(version))
            .route("/api/apikey/login", post(login))
            .route("/api/project/register", post(register_project))
            .route("/logs3", post(logs))
            .route("/logs3/overflow", post(logs))
            .with_state(Arc::clone(&state));
        Self {
            server: CollectorServer::start(router).await,
            state,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.server.uri
    }

    pub fn rows(&self) -> Vec<Value> {
        self.state.rows.lock().expect("trace row lock").clone()
    }

    pub fn diagnostics(&self) -> String {
        format!(
            "project registrations: {}; log requests: {}; rows: {}",
            self.state.registrations.load(Ordering::SeqCst),
            self.state.log_requests.load(Ordering::SeqCst),
            self.rows().len()
        )
    }
}

async fn version() -> Json<Value> {
    Json(json!({"logs3_payload_max_bytes": null}))
}

async fn login() -> Json<Value> {
    Json(json!({
        "org_info": [{
            "id": "mock-org",
            "name": "mock",
            "api_url": "unused",
            "proxy_url": "unused"
        }]
    }))
}

async fn register_project(State(state): State<Arc<CollectorState>>) -> Json<Value> {
    state.registrations.fetch_add(1, Ordering::SeqCst);
    Json(json!({
        "project": {
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "agent-e2e"
        }
    }))
}

async fn logs(
    State(state): State<Arc<CollectorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<Value> {
    state.log_requests.fetch_add(1, Ordering::SeqCst);
    let decoded = match headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if value.split(',').any(|part| part.trim() == "gzip") => {
            // The SDK currently sends uncompressed bodies in this path. Keep a
            // clear failure if that changes so the collector can add decoding.
            panic!("gzip-compressed Braintrust rows are not yet supported")
        }
        _ => body.to_vec(),
    };
    let payload: Value = serde_json::from_slice(&decoded).expect("decode /logs3 body");
    if let Some(rows) = payload["rows"].as_array() {
        state
            .rows
            .lock()
            .expect("trace row lock")
            .extend(rows.iter().cloned());
    }
    Json(json!({}))
}
