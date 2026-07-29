mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::agent_process::AgentTestWorld;
use support::agents::{ClaudeAgent, ClaudeRun, CodexAgent, CodexRun};
use support::inference::{
    AnthropicMock, AnthropicRequest, AnthropicTurn, MockReply, OpenAiMock, OpenAiRequest,
    OpenAiTurn,
};
use support::ingest::IngestScenario;
use support::server::TestServer;

const TEST_MODE_ENV: &str = "BT_AGENT_TEST_MODE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentTestMode {
    Mock,
    Live,
}

impl AgentTestMode {
    fn from_env() -> Self {
        match std::env::var(TEST_MODE_ENV).as_deref() {
            Ok("live") => Self::Live,
            Ok("mock" | "deterministic") | Err(std::env::VarError::NotPresent) => Self::Mock,
            Ok(value) => panic!("{TEST_MODE_ENV} must be `mock` or `live`, got {value:?}"),
            Err(error) => panic!("could not read {TEST_MODE_ENV}: {error}"),
        }
    }
}

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
    match AgentTestMode::from_env() {
        AgentTestMode::Mock => run_codex_mock().await,
        AgentTestMode::Live => run_codex_live().await,
    }
}

async fn run_codex_mock() {
    let inference = OpenAiMock::new(|context, request| {
        assert_eq!(request.model(), Some("mock-model"));
        match context.request_index {
            0 => {
                assert!(
                    request.contains_text("Run the deterministic command"),
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
        .run(CodexRun::mock(
            "Run the deterministic command, then return the deterministic marker.",
            inference_server.uri(),
        ))
        .await;
    output.assert_success();
    output.assert_contains("CODEX_MOCK_OK");
    assert_eq!(inference.requests().len(), 2);

    let failed = codex
        .run(CodexRun::mock(
            "Trigger the deterministic inference error.",
            inference_server.uri(),
        ))
        .await;
    failed.assert_failure();
    failed.assert_contains("deterministic Codex inference failure");
    assert_eq!(inference.requests().len(), 3);

    let scenario = IngestScenario::new()
        .expect("Codex trace origin", |row| {
            row_contains(row, &["braintrust.plugin.codex", "test_harness"])
        })
        .expect("Codex tool output", |row| {
            row_contains(row, &[r#""type":"tool""#, "CODEX_TOOL_OK"])
        });
    assert!(!world.wait_for_ingest_scenario(&scenario).await.is_empty());
}

async fn run_codex_live() {
    let world = AgentTestWorld::start().await;
    let codex = CodexAgent::install(&world).await;
    codex.seed_live_auth();

    codex
        .run(CodexRun::live(
            "Reply briefly to confirm this tracing integration test.",
        ))
        .await
        .assert_success();

    let scenario = IngestScenario::new().expect("Codex trace origin", |row| {
        row_contains(row, &["braintrust.plugin.codex", "test_harness"])
    });
    assert!(!world.wait_for_ingest_scenario(&scenario).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the Claude Code CLI installed on PATH"]
async fn claude_session_emits_traces() {
    match AgentTestMode::from_env() {
        AgentTestMode::Mock => run_claude_mock().await,
        AgentTestMode::Live => run_claude_live().await,
    }
}

async fn run_claude_mock() {
    let inference = AnthropicMock::new(|context, request| match context.request_index {
        0 => {
            assert_eq!(request.model(), Some("mock-model"));
            assert!(
                request.contains_text("Run the deterministic command"),
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
        .run(ClaudeRun::mock(
            "Run the deterministic command, then return the deterministic marker.",
            inference_server.uri(),
        ))
        .await;
    output.assert_success();
    output.assert_contains("CLAUDE_MOCK_OK");
    assert_eq!(inference.requests().len(), 2);

    let failed = claude
        .run(ClaudeRun::mock(
            "Trigger the deterministic inference error.",
            inference_server.uri(),
        ))
        .await;
    failed.assert_failure();
    failed.assert_contains("deterministic Claude inference failure");
    assert_eq!(inference.requests().len(), 3);

    let scenario = IngestScenario::new()
        .expect("Claude trace source", |row| {
            row_contains(row, &[r#""source":"claude-code""#, "test_harness"])
        })
        .expect("Claude tool output", |row| {
            row_contains(row, &[r#""type":"tool""#, "CLAUDE_TOOL_OK"])
        });
    assert!(!world.wait_for_ingest_scenario(&scenario).await.is_empty());
}

async fn run_claude_live() {
    let world = AgentTestWorld::start().await;
    let claude = ClaudeAgent::new(&world);

    claude
        .run(ClaudeRun::live(
            "Reply briefly to confirm this tracing integration test.",
        ))
        .await
        .assert_success();

    let scenario = IngestScenario::new().expect("Claude trace source", |row| {
        row_contains(row, &[r#""source":"claude-code""#, "test_harness"])
    });
    assert!(!world.wait_for_ingest_scenario(&scenario).await.is_empty());
}

#[test]
fn request_helpers_recognize_tool_results_and_advertised_tools() {
    let openai = OpenAiRequest {
        body: json!({
            "input":[{"type":"function_call_output","call_id":"call-1"}],
            "tools":[{"type":"function","name":"shell"}]
        }),
    };
    assert!(openai.has_function_output("call-1"));
    assert_eq!(openai.tool_names(), vec!["shell"]);

    let anthropic = AnthropicRequest {
        body: json!({
            "messages":[{
                "content":[{"type":"tool_result","tool_use_id":"toolu-1"}]
            }]
        }),
    };
    assert!(anthropic.has_tool_result("toolu-1"));
}

#[test]
fn test_modes_remain_distinct() {
    assert_ne!(AgentTestMode::Mock, AgentTestMode::Live);
}
