mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::agent_process::AgentTestWorld;
use support::agents::{ClaudeAgent, ClaudeRun, CodexAgent, CodexRun, OpenCodeAgent, OpenCodeRun};
use support::inference::{
    AnthropicMock, AnthropicRequest, AnthropicTurn, MockReply, OpenAiMock, OpenAiRequest,
    OpenAiTurn,
};
use support::ingest::IngestScenario;
use support::server::TestServer;

fn codex_tool_call(request: &OpenAiRequest) -> OpenAiTurn {
    let names = request.tool_names();
    if names.contains(&"exec_command") {
        return OpenAiTurn::tool_call(
            "call_mock_1",
            "exec_command",
            json!({"cmd":codex_tool_command(),"login":false}),
        );
    }
    if names.contains(&"shell") {
        return OpenAiTurn::tool_call(
            "call_mock_1",
            "shell",
            json!({"command":codex_tool_command()}),
        );
    }
    if names.contains(&"shell_command") {
        return OpenAiTurn::tool_call(
            "call_mock_1",
            "shell_command",
            json!({"command":codex_tool_command()}),
        );
    }
    panic!("Codex offered no supported shell tool; offered tools: {names:?}");
}

fn codex_tool_command() -> &'static str {
    #[cfg(unix)]
    {
        "printf CODEX_TOOL_OK"
    }
    #[cfg(windows)]
    {
        "Write-Output CODEX_TOOL_OK"
    }
}

fn row_contains(row: &Value, fragments: &[&str]) -> bool {
    let serialized = serde_json::to_string(row).expect("serialize trace row");
    fragments
        .iter()
        .all(|fragment| serialized.contains(fragment))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the Codex CLI installed on PATH"]
async fn codex_session_emits_traces() {
    let inference = OpenAiMock::new(|context, request| {
        assert_eq!(request.model(), Some("mock-model"));
        match context.request_index {
            0 => {
                assert!(
                    request.contains_text("CODEX_TOOL_OK"),
                    "unexpected Codex request: {}",
                    request.body
                );
                MockReply::response(codex_tool_call(&request))
            }
            1 => {
                assert!(
                    request.has_function_output("call_mock_1"),
                    "Codex did not return the tool result: {}",
                    request.body
                );
                MockReply::response(OpenAiTurn::text("CODEX_MOCK_OK"))
            }
            2 => MockReply::http_error(
                StatusCode::BAD_REQUEST,
                json!({
                    "error": {
                        "type": "invalid_request_error",
                        "code": "mock_bad_request",
                        "message": "deterministic Codex inference failure"
                    }
                }),
            ),
            index => panic!(
                "unexpected Codex inference request {index}: {}",
                request.body
            ),
        }
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let codex = CodexAgent::install(&world).await;

    let output = codex
        .run(
            &world,
            CodexRun::new("Run the command `printf CODEX_TOOL_OK` and then reply briefly.")
                .mock_inference(inference_server.uri()),
        )
        .await;
    output.assert_success();
    if world.uses_mock_inference() {
        output.assert_contains("CODEX_MOCK_OK");
        assert_eq!(inference.requests().len(), 2);

        let failed = codex
            .run(
                &world,
                CodexRun::new("Trigger the deterministic inference error.")
                    .mock_inference(inference_server.uri()),
            )
            .await;
        failed.assert_failure();
        failed.assert_contains("deterministic Codex inference failure");
        assert_eq!(inference.requests().len(), 3);
    }

    let rows = world.wait_for_trace_delivery().await;
    if world.uses_mock_ingest() {
        assert!(
            rows.iter()
                .any(|row| { row_contains(row, &["braintrust.plugin.codex", "test_harness"]) }),
            "Codex trace origin metadata was not emitted"
        );
    }
    if world.uses_mock_inference() && world.uses_mock_ingest() {
        let scenario = IngestScenario::new()
            .expect("Codex trace origin", |row| {
                row_contains(row, &["braintrust.plugin.codex", "test_harness"])
            })
            .expect("Codex tool output", |row| {
                row_contains(row, &[r#""type":"tool""#, "CODEX_TOOL_OK"])
            });
        world.wait_for_mock_ingest_scenario(&scenario).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the Claude Code CLI installed on PATH"]
async fn claude_session_emits_traces() {
    let inference = AnthropicMock::new(|context, request| match context.request_index {
        0 => {
            assert_eq!(request.model(), Some("mock-model"));
            assert!(
                request.contains_text("CLAUDE_TOOL_OK"),
                "unexpected Claude request: {}",
                request.body
            );
            MockReply::response(AnthropicTurn::tool_use(
                "toolu_mock_1",
                "Bash",
                json!({"command":"printf CLAUDE_TOOL_OK"}),
            ))
        }
        1 => {
            assert!(
                request.has_tool_result("toolu_mock_1"),
                "Claude did not return the tool result: {}",
                request.body
            );
            MockReply::response(AnthropicTurn::text("CLAUDE_MOCK_OK"))
        }
        2 => MockReply::http_error(
            StatusCode::BAD_REQUEST,
            json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "deterministic Claude inference failure"
                }
            }),
        ),
        index => panic!(
            "unexpected Claude inference request {index}: {}",
            request.body
        ),
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let claude = ClaudeAgent::new(&world);

    let output = claude
        .run(
            &world,
            ClaudeRun::new("Run the command `printf CLAUDE_TOOL_OK` and then reply briefly.")
                .mock_inference(inference_server.uri()),
        )
        .await;
    output.assert_success();
    if world.uses_mock_inference() {
        output.assert_contains("CLAUDE_MOCK_OK");
        assert_eq!(inference.requests().len(), 2);

        let failed = claude
            .run(
                &world,
                ClaudeRun::new("Trigger the deterministic inference error.")
                    .mock_inference(inference_server.uri()),
            )
            .await;
        failed.assert_failure();
        failed.assert_contains("deterministic Claude inference failure");
        assert_eq!(inference.requests().len(), 3);
    }

    let rows = world.wait_for_trace_delivery().await;
    if world.uses_mock_ingest() {
        assert!(
            rows.iter()
                .any(|row| { row_contains(row, &[r#""source":"claude-code""#, "test_harness"]) }),
            "Claude trace source metadata was not emitted"
        );
    }
    if world.uses_mock_inference() && world.uses_mock_ingest() {
        let scenario = IngestScenario::new()
            .expect("Claude trace source", |row| {
                row_contains(row, &[r#""source":"claude-code""#, "test_harness"])
            })
            .expect("Claude tool output", |row| {
                row_contains(row, &[r#""type":"tool""#, "CLAUDE_TOOL_OK"])
            });
        world.wait_for_mock_ingest_scenario(&scenario).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the OpenCode CLI and built plugin"]
async fn opencode_session_emits_traces() {
    let inference = OpenAiMock::new(|_context, request| {
        assert_eq!(request.model(), Some("mock-model"));
        MockReply::response(OpenAiTurn::text("OPENCODE_MOCK_OK"))
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let opencode = OpenCodeAgent::new(&world);

    let output = opencode
        .run(
            &world,
            OpenCodeRun::new("Reply with the exact text OPENCODE_MOCK_OK.")
                .mock_inference(inference_server.uri()),
        )
        .await;
    output.assert_success();
    if world.uses_mock_inference() {
        output.assert_contains("OPENCODE_MOCK_OK");
        assert!(!inference.requests().is_empty(), "{}", output.text());
    }

    let rows = world.wait_for_trace_delivery().await;
    if world.uses_mock_ingest() {
        assert!(
            rows.iter()
                .any(|row| row_contains(row, &["braintrust.plugin.opencode", "test_harness"])),
            "OpenCode trace origin metadata was not emitted"
        );
    }
    if world.uses_mock_inference() && world.uses_mock_ingest() {
        let scenario = IngestScenario::new()
            .expect("OpenCode trace origin", |row| {
                row_contains(row, &["braintrust.plugin.opencode", "test_harness"])
            })
            .expect("OpenCode turn input", |row| {
                row_contains(row, &["Turn 1", "Reply with the exact text OPENCODE_MOCK_OK"])
            });
        world.wait_for_mock_ingest_scenario(&scenario).await;
    }
}

#[test]
fn request_helpers_recognize_tool_results_and_advertised_tools() {
    let openai = OpenAiRequest {
        body: json!({
            "input":[{"type":"function_call_output","call_id":"call-1"}],
            "tools":[{"type":"function","name":"shell_command"}]
        }),
    };
    assert!(openai.has_function_output("call-1"));
    assert_eq!(openai.tool_names(), vec!["shell_command"]);
    match codex_tool_call(&openai) {
        OpenAiTurn::ToolCall {
            name, arguments, ..
        } => {
            assert_eq!(name, "shell_command");
            assert!(arguments["command"].is_string());
        }
        _ => panic!("expected a Codex tool call"),
    }

    let anthropic = AnthropicRequest {
        body: json!({
            "messages":[{
                "content":[{"type":"tool_result","tool_use_id":"toolu-1"}]
            }]
        }),
    };
    assert!(anthropic.has_tool_result("toolu-1"));
}
