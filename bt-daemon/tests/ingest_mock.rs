mod support;

use serde_json::json;
use support::ingest::{IngestMock, IngestScenario};
use support::server::TestServer;

#[tokio::test]
async fn ingest_router_captures_rows_and_matches_ordered_shapes() {
    let ingest = IngestMock::new();
    let server = TestServer::start(ingest.router()).await;

    let response = reqwest::Client::new()
        .post(format!("{}/logs3", server.uri()))
        .json(&json!({
            "rows": [
                {"span_attributes":{"type":"task"},"metadata":{"source":"codex"}},
                {"span_attributes":{"type":"llm"}},
                {"span_attributes":{"type":"tool"},"output":"deterministic"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let scenario = IngestScenario::new()
        .expect("root task", |row| row["span_attributes"]["type"] == "task")
        .expect("tool result", |row| {
            row["span_attributes"]["type"] == "tool" && row["output"] == "deterministic"
        });
    assert_eq!(ingest.evaluate(&scenario).unwrap().len(), 3);

    let reversed = IngestScenario::new()
        .expect("tool first", |row| row["span_attributes"]["type"] == "tool")
        .expect("task later", |row| row["span_attributes"]["type"] == "task");
    assert!(ingest.evaluate(&reversed).is_err());
}
