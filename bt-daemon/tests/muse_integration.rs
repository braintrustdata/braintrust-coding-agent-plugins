#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions fail immediately."
)]
mod support;

use axum::http::StatusCode;
use serde_json::json;
use support::agent_process::AgentTestWorld;
use support::agents::MuseAgent;
use support::inference::{MockReply, OpenAiMock, OpenAiTurn};
use support::ingest::IngestScenario;
use support::server::TestServer;

fn muse_tool_call(
    request: &support::inference::OpenAiRequest,
    call_id: &str,
    command: &str,
) -> MockReply<OpenAiTurn> {
    let names = request.tool_names();
    if names.contains(&"muse") {
        MockReply::response(OpenAiTurn::Events(vec![
            json!({"type":"response.created", "response":{"id":format!("resp_{call_id}")}}),
            json!({"type":"response.function_call_arguments.done", "output_index":0,
                "item_id":format!("fc_{call_id}"), "namespace":"muse", "name":"shell", "call_id":call_id,
                "arguments":json!({"command":command}).to_string()}),
            json!({"type":"response.completed", "response":{"id":format!("resp_{call_id}"), "status":"completed",
                "output":[], "usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}),
        ]))
    } else {
        let name = ["shell", "bash"]
            .into_iter()
            .find(|name| names.contains(name))
            .unwrap_or_else(|| panic!("no shell tool: {names:?}"));
        MockReply::response(OpenAiTurn::tool_call(
            call_id,
            name,
            json!({"command": command, "description": "Print integration marker"}),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Muse Code 1.3+ installed; uses loopback inference and ingest"]
async fn muse_session_emits_traces() {
    let inference = OpenAiMock::new(|context, request| {
        assert_eq!(request.model(), Some("mock-model"));
        match context.request_index {
            0 => {
                assert!(request.contains_text("MUSE_TOOL_OK"));
                muse_tool_call(&request, "call_muse_1", "printf MUSE_TOOL_OK")
            }
            1 => {
                assert!(
                    request.has_function_output("call_muse_1"),
                    "{}",
                    request.body
                );
                MockReply::response(OpenAiTurn::text("MUSE_MOCK_OK"))
            }
            2 => MockReply::http_error(
                StatusCode::BAD_REQUEST,
                json!({"error": {
                    "type": "invalid_request_error", "message": "deterministic Muse inference failure"
                }}),
            ),
            3 => MockReply::http_error(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": {
                    "type":"server_error", "message":"transient Muse inference failure"
                }}),
            ),
            4 => MockReply::response(OpenAiTurn::text("MUSE_RETRY_OK")),
            5 => muse_tool_call(
                &request,
                "call_muse_failure",
                "printf MUSE_TOOL_FAILURE; exit 7",
            ),
            6 => MockReply::response(OpenAiTurn::text("MUSE_FAILURE_HANDLED")),
            n => panic!(
                "unexpected Muse inference request {n}; model {:?}",
                request.model()
            ),
        }
    });
    let server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    assert!(
        world.uses_mock_inference() && world.uses_mock_ingest(),
        "Muse test requires mock backends"
    );
    let muse = MuseAgent::new(&world, server.uri());
    let output = muse
        .run(
            &world,
            "Run the shell command printf MUSE_TOOL_OK, then reply MUSE_MOCK_OK.",
        )
        .await;
    let export = muse.export_latest(&world).await;
    assert_eq!(export["export_schema_version"], 1);
    output.assert_success();
    output.assert_contains("MUSE_MOCK_OK");
    assert_eq!(inference.requests().len(), 2);
    assert!(
        inference.requests()[1].has_function_output("call_muse_1"),
        "Muse did not return shell output"
    );
    let failed = muse
        .run(&world, "Trigger the deterministic inference error.")
        .await;
    failed.assert_failure();
    failed.assert_contains("deterministic Muse inference failure");
    assert_eq!(inference.requests().len(), 3);
    let retried = muse.run(&world, "Reply after the transient failure.").await;
    retried.assert_success();
    retried.assert_contains("MUSE_RETRY_OK");
    assert_eq!(inference.requests().len(), 5);
    let tool_failed = muse
        .run(&world, "Run a shell command that fails with exit 7.")
        .await;
    tool_failed.assert_success();
    tool_failed.assert_contains("MUSE_FAILURE_HANDLED");
    assert_eq!(inference.requests().len(), 7);
    assert!(inference.requests()[6].has_function_output("call_muse_failure"));
    let scenario = IngestScenario::new()
        .expect("Muse version and provenance", |row| {
            row["metadata"]["source"] == "muse"
                && row["metadata"]["session_id"].is_string()
                && row["metadata"]["muse_version"].is_string()
        })
        .expect("Muse tool completion", |row| {
            row["span_attributes"]["type"] == "tool"
                && row["output"]
                    .as_str()
                    .is_some_and(|text| text.contains("MUSE_TOOL_OK"))
                && row["error"].is_null()
                && row["metrics"]["end"].is_number()
        })
        .expect("Muse final LLM", |row| {
            row["span_attributes"]["type"] == "llm" && row.to_string().contains("MUSE_MOCK_OK")
        })
        .expect("Muse terminal session output", |row| {
            row["span_attributes"]["name"] == "Muse Code"
                && row["output"] == "MUSE_MOCK_OK"
                && row["metrics"]["end"].is_number()
        })
        .expect("Muse inference failure", |row| {
            row.to_string()
                .contains("deterministic Muse inference failure")
                && row["error"].is_string()
        });
    let scenario = scenario
        .expect("Muse retries complete", |row| {
            row["span_attributes"]["type"] == "llm" && row.to_string().contains("MUSE_RETRY_OK")
        })
        .expect("Muse tool failures retain outcome", |row| {
            row["span_attributes"]["type"] == "tool"
                && row["metadata"]["tool_outcome"] == "error"
                && row["error"].is_string()
        });
    let rows = world.wait_for_mock_ingest_scenario(&scenario).await;
    let root = rows
        .iter()
        .find(|row| {
            row["span_attributes"]["name"] == "Muse Code"
                && row["output"] == "MUSE_MOCK_OK"
                && row["metrics"]["end"].is_number()
        })
        .expect("successful session has authoritative terminal output");
    assert_eq!(root["metrics"]["prompt_tokens"], 20.0);
    assert_eq!(root["metrics"]["completion_tokens"], 10.0);
    assert_eq!(root["metrics"]["tokens"], 30.0);
    let llms: std::collections::HashSet<_> = rows
        .iter()
        .filter(|row| {
            row["span_attributes"]["type"] == "llm" && row["root_span_id"] == root["span_id"]
        })
        .map(|row| row["span_id"].as_str().unwrap())
        .collect();
    assert_eq!(llms.len(), 2, "one LLM span per native model step");
    assert!(
        rows.iter()
            .filter(|row| row["root_span_id"] == root["span_id"])
            .all(|row| row["error"].is_null()),
        "successful tool_calls must not become LLM errors"
    );
}
