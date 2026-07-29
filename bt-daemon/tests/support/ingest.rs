use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type RowMatcher = dyn Fn(&Value) -> bool + Send + Sync + 'static;

struct ExpectedRow {
    name: String,
    matcher: Arc<RowMatcher>,
    strict: bool,
}

#[derive(Default)]
pub struct IngestScenario {
    expected: Vec<ExpectedRow>,
}

impl IngestScenario {
    pub fn new() -> Self {
        Self::default()
    }

    /// Require a row shape after all previously declared shapes. Unrelated
    /// rows are ignored, so matching is independent of HTTP batching and
    /// SDK-generated update rows.
    pub fn expect(
        mut self,
        name: impl Into<String>,
        matcher: impl Fn(&Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.expected.push(ExpectedRow {
            name: name.into(),
            matcher: Arc::new(matcher),
            strict: false,
        });
        self
    }

    /// Require an additional row shape when deterministic inference is in use.
    /// Baseline expectations declared with [`Self::expect`] are always active.
    pub fn expect_strict(
        mut self,
        name: impl Into<String>,
        matcher: impl Fn(&Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.expected.push(ExpectedRow {
            name: name.into(),
            matcher: Arc::new(matcher),
            strict: true,
        });
        self
    }

    pub fn evaluate(&self, rows: &[Value], include_strict: bool) -> Result<(), String> {
        let active_expected = self
            .expected
            .iter()
            .filter(|expected| include_strict || !expected.strict)
            .collect::<Vec<_>>();
        let mut cursor = 0;
        for (matched, expected) in active_expected.iter().enumerate() {
            let Some(offset) = rows[cursor..]
                .iter()
                .position(|row| (expected.matcher)(row))
            else {
                return Err(format!(
                    "missing ingest shape {:?} after matching {} of {} shapes",
                    expected.name,
                    matched,
                    active_expected.len()
                ));
            };
            cursor += offset + 1;
        }
        Ok(())
    }
}

#[derive(Default)]
struct CollectorState {
    rows: Mutex<Vec<Value>>,
    registrations: AtomicUsize,
    log_requests: AtomicUsize,
}

pub struct IngestMock {
    state: Arc<CollectorState>,
}

impl IngestMock {
    pub fn new() -> Self {
        let state = Arc::new(CollectorState::default());
        Self { state }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/version", get(version))
            .route("/api/apikey/login", post(login))
            .route("/api/project/register", post(register_project))
            .route("/logs3", post(logs))
            .route("/logs3/overflow", post(logs))
            .with_state(Arc::clone(&self.state))
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

    pub fn evaluate(
        &self,
        scenario: &IngestScenario,
        include_strict: bool,
    ) -> Result<Vec<Value>, String> {
        let rows = self.rows();
        scenario.evaluate(&rows, include_strict)?;
        Ok(rows)
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
