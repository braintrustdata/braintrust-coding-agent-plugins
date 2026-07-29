#![cfg(unix)]

mod support;

use axum::http::StatusCode;
use serde_json::json;
use std::path::{Path, PathBuf};
use support::agent_process::AgentTestWorld;
use support::inference::{AnthropicMock, AnthropicTurn, MockReply, OpenAiMock, OpenAiTurn};
use tokio::process::Command;
use uuid::Uuid;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repository root")
        .to_path_buf()
}

fn command_from_env(name: &str, fallback: &str) -> Command {
    Command::new(std::env::var_os(name).unwrap_or_else(|| fallback.into()))
}

fn output_text(output: &std::process::Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the latest Codex CLI installed on PATH"]
async fn latest_codex_runs_through_mock_inference_and_emits_traces() {
    let inference = OpenAiMock::start(|context, request| {
        assert_eq!(request.model(), Some("mock-model"));
        match context.request_index {
            0 => {
                assert!(
                    request.contains_text("Run the deterministic command"),
                    "unexpected Codex request: {}",
                    request.body
                );
                MockReply::response(OpenAiTurn::tool_call(
                    "call_mock_1",
                    "exec_command",
                    json!({"cmd":"printf CODEX_TOOL_OK","login":false}),
                ))
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
    })
    .await;
    let world = AgentTestWorld::start().await;
    let codex_home = world.temp_path("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();

    let marketplace = repository_root().join("src/plugins/codex/content");
    let mut add_marketplace = command_from_env("CODEX_BIN", "codex");
    add_marketplace
        .arg("plugin")
        .arg("marketplace")
        .arg("add")
        .arg(&marketplace)
        .env("CODEX_HOME", &codex_home);
    world.configure(&mut add_marketplace);
    let output = world.output(&mut add_marketplace).await;
    assert!(output.status.success(), "{}", output_text(&output));

    let mut add_plugin = command_from_env("CODEX_BIN", "codex");
    add_plugin
        .args(["plugin", "add", "trace-codex@braintrust-codex-plugins"])
        .env("CODEX_HOME", &codex_home);
    world.configure(&mut add_plugin);
    let output = world.output(&mut add_plugin).await;
    assert!(output.status.success(), "{}", output_text(&output));

    let provider = format!(
        r#"model_providers.mock={{name="Mock",base_url="{}/v1",wire_api="responses",env_key="MOCK_API_KEY",request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=5000}}"#,
        inference.base_url()
    );
    let chatgpt_base_url = format!(r#"chatgpt_base_url="{}/backend-api""#, inference.base_url());
    let mut codex = command_from_env("CODEX_BIN", "codex");
    codex
        .args([
            "exec",
            "--skip-git-repo-check",
            "--dangerously-bypass-hook-trust",
            "--sandbox",
            "read-only",
            "-c",
            r#"model="mock-model""#,
            "-c",
            r#"model_provider="mock""#,
            "-c",
            r#"approval_policy="never""#,
            "-c",
            &provider,
            "-c",
            &chatgpt_base_url,
            "Run the deterministic command, then return the deterministic marker.",
        ])
        .current_dir(world.workspace())
        .env("CODEX_HOME", &codex_home)
        .env("MOCK_API_KEY", "test-key");
    world.configure(&mut codex);
    let output = world.output(&mut codex).await;
    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("CODEX_MOCK_OK"),
        "{}",
        output_text(&output)
    );
    assert_eq!(inference.requests().len(), 2);

    let mut failing_codex = command_from_env("CODEX_BIN", "codex");
    failing_codex
        .args([
            "exec",
            "--skip-git-repo-check",
            "--dangerously-bypass-hook-trust",
            "--sandbox",
            "read-only",
            "-c",
            r#"model="mock-model""#,
            "-c",
            r#"model_provider="mock""#,
            "-c",
            r#"approval_policy="never""#,
            "-c",
            &provider,
            "-c",
            &chatgpt_base_url,
            "Trigger the deterministic inference error.",
        ])
        .current_dir(world.workspace())
        .env("CODEX_HOME", &codex_home)
        .env("MOCK_API_KEY", "test-key");
    world.configure(&mut failing_codex);
    let failed = world.output(&mut failing_codex).await;
    assert!(!failed.status.success(), "{}", output_text(&failed));
    assert!(
        output_text(&failed).contains("deterministic Codex inference failure"),
        "{}",
        output_text(&failed)
    );
    assert_eq!(inference.requests().len(), 3);

    let rows = world.wait_for_trace_rows().await;
    let serialized = serde_json::to_string(&rows).unwrap();
    assert!(
        serialized.contains("braintrust.plugin.codex"),
        "{serialized}"
    );
    assert!(serialized.contains("test_harness"), "{serialized}");
    assert!(serialized.contains("\"type\":\"tool\""), "{serialized}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the latest Claude Code CLI installed on PATH"]
async fn latest_claude_runs_through_mock_inference_and_emits_traces() {
    let inference = AnthropicMock::start(|context, request| match context.request_index {
        0 => {
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
    })
    .await;
    let world = AgentTestWorld::start().await;
    let claude_config = world.temp_path("claude-config");
    let home = world.temp_path("home");
    std::fs::create_dir_all(&claude_config).unwrap();
    std::fs::create_dir_all(&home).unwrap();

    let plugin = repository_root().join("src/plugins/claude/content/plugins/trace-claude-code");
    let session_id = Uuid::new_v4().to_string();
    let mut claude = command_from_env("CLAUDE_BIN", "claude");
    claude
        .args([
            "-p",
            "--output-format",
            "json",
            "--dangerously-skip-permissions",
            "--model",
            "mock-model",
            "--session-id",
            &session_id,
            "--plugin-dir",
        ])
        .arg(plugin)
        .arg("Run the deterministic command, then return the deterministic marker.")
        .current_dir(world.workspace())
        .env("HOME", &home)
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .env("ANTHROPIC_BASE_URL", inference.base_url())
        .env("ANTHROPIC_API_KEY", "test-key")
        .env("ANTHROPIC_AUTH_TOKEN", "test-key")
        .env("ANTHROPIC_DEFAULT_OPUS_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_SONNET_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", "mock-model")
        .env("ANTHROPIC_MAX_RETRIES", "0")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("DISABLE_TELEMETRY", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
    world.configure(&mut claude);
    let output = world.output(&mut claude).await;
    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("CLAUDE_MOCK_OK"),
        "{}",
        output_text(&output)
    );
    assert!(!inference.requests().is_empty());

    let failing_session_id = Uuid::new_v4().to_string();
    let mut failing_claude = command_from_env("CLAUDE_BIN", "claude");
    failing_claude
        .args([
            "-p",
            "--output-format",
            "json",
            "--dangerously-skip-permissions",
            "--model",
            "mock-model",
            "--session-id",
            &failing_session_id,
            "--plugin-dir",
        ])
        .arg(repository_root().join("src/plugins/claude/content/plugins/trace-claude-code"))
        .arg("Trigger the deterministic inference error.")
        .current_dir(world.workspace())
        .env("HOME", &home)
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .env("ANTHROPIC_BASE_URL", inference.base_url())
        .env("ANTHROPIC_API_KEY", "test-key")
        .env("ANTHROPIC_AUTH_TOKEN", "test-key")
        .env("ANTHROPIC_DEFAULT_OPUS_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_SONNET_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", "mock-model")
        .env("ANTHROPIC_MAX_RETRIES", "0")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("DISABLE_TELEMETRY", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
    world.configure(&mut failing_claude);
    let failed = world.output(&mut failing_claude).await;
    assert!(!failed.status.success(), "{}", output_text(&failed));
    assert!(
        output_text(&failed).contains("deterministic Claude inference failure"),
        "{}",
        output_text(&failed)
    );
    assert_eq!(inference.requests().len(), 3);

    let rows = world.wait_for_trace_rows().await;
    let serialized = serde_json::to_string(&rows).unwrap();
    assert!(
        serialized.contains("\"source\":\"claude-code\""),
        "{serialized}"
    );
    assert!(serialized.contains("test_harness"), "{serialized}");
    assert!(serialized.contains("\"type\":\"tool\""), "{serialized}");
}

#[test]
fn request_helpers_recognize_tool_results() {
    let openai = support::inference::OpenAiRequest {
        body: json!({"input":[{"type":"function_call_output","call_id":"call-1"}]}),
    };
    assert!(openai.has_function_output("call-1"));

    let anthropic = support::inference::AnthropicRequest {
        body: json!({
            "messages":[{
                "content":[{"type":"tool_result","tool_use_id":"toolu-1"}]
            }]
        }),
    };
    assert!(anthropic.has_tool_result("toolu-1"));
}
