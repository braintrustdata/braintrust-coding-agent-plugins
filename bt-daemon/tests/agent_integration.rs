mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use support::agent_process::AgentTestWorld;
use support::inference::{AnthropicMock, AnthropicTurn, MockReply, OpenAiMock, OpenAiTurn};
use support::ingest::IngestScenario;
use support::server::TestServer;
use tokio::process::Command;
use uuid::Uuid;

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

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repository root")
        .to_path_buf()
}

fn command_from_env(name: &str, fallback: &str) -> Command {
    if let Some(command) = std::env::var_os(name) {
        return Command::new(command);
    }
    #[cfg(windows)]
    let fallback = format!("{fallback}.cmd");
    Command::new(fallback)
}

fn output_text(output: &std::process::Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
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

fn configured_codex_home() -> Option<PathBuf> {
    std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .map(|home| home.join(".codex"))
        })
}

fn seed_codex_live_auth(codex_home: &Path) {
    if std::env::var_os("OPENAI_API_KEY").is_some() {
        return;
    }
    let source = configured_codex_home()
        .map(|home| home.join("auth.json"))
        .filter(|path| path.is_file())
        .unwrap_or_else(|| {
            panic!(
                "{TEST_MODE_ENV}=live requires OPENAI_API_KEY or auth.json in the configured Codex home"
            )
        });
    std::fs::copy(source, codex_home.join("auth.json")).expect("copy Codex live credentials");
}

async fn install_codex_plugin(world: &AgentTestWorld, codex_home: &Path) {
    let marketplace = repository_root().join("src/plugins/codex/content");
    let mut add_marketplace = command_from_env("CODEX_BIN", "codex");
    add_marketplace
        .arg("plugin")
        .arg("marketplace")
        .arg("add")
        .arg(&marketplace)
        .env("CODEX_HOME", codex_home);
    world.configure(&mut add_marketplace);
    let output = world.output(&mut add_marketplace).await;
    assert!(output.status.success(), "{}", output_text(&output));

    let mut add_plugin = command_from_env("CODEX_BIN", "codex");
    add_plugin
        .args(["plugin", "add", "trace-codex@braintrust-codex-plugins"])
        .env("CODEX_HOME", codex_home);
    world.configure(&mut add_plugin);
    let output = world.output(&mut add_plugin).await;
    assert!(output.status.success(), "{}", output_text(&output));
}

fn codex_command(world: &AgentTestWorld, codex_home: &Path) -> Command {
    let mut command = command_from_env("CODEX_BIN", "codex");
    command
        .args([
            "exec",
            "--skip-git-repo-check",
            "--dangerously-bypass-hook-trust",
            "--sandbox",
            "read-only",
            "-c",
            r#"approval_policy="never""#,
        ])
        .current_dir(world.workspace())
        .env("CODEX_HOME", codex_home);
    world.configure(&mut command);
    command
}

fn claude_command(world: &AgentTestWorld) -> Command {
    let session_id = Uuid::new_v4().to_string();
    let plugin = repository_root().join("src/plugins/claude/content/plugins/trace-claude-code");
    let mut command = command_from_env("CLAUDE_BIN", "claude");
    command
        .args([
            "-p",
            "--output-format",
            "json",
            "--dangerously-skip-permissions",
            "--session-id",
            &session_id,
            "--plugin-dir",
        ])
        .arg(plugin)
        .current_dir(world.workspace())
        .env("ANTHROPIC_MAX_RETRIES", "0")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("DISABLE_TELEMETRY", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
    world.configure(&mut command);
    command
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
                MockReply::response(OpenAiTurn::tool_call(
                    "call_mock_1",
                    "exec_command",
                    json!({"cmd":codex_tool_command(),"login":false}),
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
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let codex_home = world.temp_path("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    install_codex_plugin(&world, &codex_home).await;

    let provider = format!(
        r#"model_providers.mock={{name="Mock",base_url="{}/v1",wire_api="responses",env_key="MOCK_API_KEY",request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=5000}}"#,
        inference_server.uri()
    );
    let chatgpt_base_url = format!(
        r#"chatgpt_base_url="{}/backend-api""#,
        inference_server.uri()
    );
    let mut codex = codex_command(&world, &codex_home);
    codex
        .args([
            "-c",
            r#"model="mock-model""#,
            "-c",
            r#"model_provider="mock""#,
            "-c",
            &provider,
            "-c",
            &chatgpt_base_url,
        ])
        .arg("Run the deterministic command, then return the deterministic marker.")
        .env("MOCK_API_KEY", "test-key");
    let output = world.output(&mut codex).await;
    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("CODEX_MOCK_OK"),
        "{}",
        output_text(&output)
    );
    assert_eq!(inference.requests().len(), 2);

    let mut failing_codex = codex_command(&world, &codex_home);
    failing_codex
        .args([
            "-c",
            r#"model="mock-model""#,
            "-c",
            r#"model_provider="mock""#,
            "-c",
            &provider,
            "-c",
            &chatgpt_base_url,
        ])
        .arg("Trigger the deterministic inference error.")
        .env("MOCK_API_KEY", "test-key");
    let failed = world.output(&mut failing_codex).await;
    assert!(!failed.status.success(), "{}", output_text(&failed));
    assert!(
        output_text(&failed).contains("deterministic Codex inference failure"),
        "{}",
        output_text(&failed)
    );
    assert_eq!(inference.requests().len(), 3);

    let scenario = IngestScenario::new()
        .expect("Codex trace origin", |row| {
            row_contains(row, &["braintrust.plugin.codex", "test_harness"])
        })
        .expect("Codex tool output", |row| {
            row_contains(row, &[r#""type":"tool""#, "CODEX_TOOL_OK"])
        });
    let rows = world.wait_for_ingest_scenario(&scenario).await;
    assert!(!rows.is_empty());
}

async fn run_codex_live() {
    let world = AgentTestWorld::start().await;
    let codex_home = world.temp_path("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    seed_codex_live_auth(&codex_home);
    install_codex_plugin(&world, &codex_home).await;

    let mut codex = codex_command(&world, &codex_home);
    codex.arg("Reply briefly to confirm this tracing integration test.");
    let output = world.output(&mut codex).await;
    assert!(output.status.success(), "{}", output_text(&output));

    let scenario = IngestScenario::new().expect("Codex trace origin", |row| {
        row_contains(row, &["braintrust.plugin.codex", "test_harness"])
    });
    let rows = world.wait_for_ingest_scenario(&scenario).await;
    assert!(!rows.is_empty());
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
    let claude_config = world.temp_path("claude-config");
    let home = world.temp_path("home");
    std::fs::create_dir_all(&claude_config).unwrap();
    std::fs::create_dir_all(&home).unwrap();

    let mut claude = claude_command(&world);
    claude
        .args(["--model", "mock-model"])
        .arg("Run the deterministic command, then return the deterministic marker.")
        .env("HOME", &home)
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .env("ANTHROPIC_BASE_URL", inference_server.uri())
        .env("ANTHROPIC_API_KEY", "test-key")
        .env("ANTHROPIC_AUTH_TOKEN", "test-key")
        .env("ANTHROPIC_DEFAULT_OPUS_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_SONNET_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", "mock-model");
    let output = world.output(&mut claude).await;
    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        output_text(&output).contains("CLAUDE_MOCK_OK"),
        "{}",
        output_text(&output)
    );
    assert_eq!(inference.requests().len(), 2);

    let mut failing_claude = claude_command(&world);
    failing_claude
        .args(["--model", "mock-model"])
        .arg("Trigger the deterministic inference error.")
        .env("HOME", &home)
        .env("CLAUDE_CONFIG_DIR", &claude_config)
        .env("ANTHROPIC_BASE_URL", inference_server.uri())
        .env("ANTHROPIC_API_KEY", "test-key")
        .env("ANTHROPIC_AUTH_TOKEN", "test-key")
        .env("ANTHROPIC_DEFAULT_OPUS_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_SONNET_MODEL", "mock-model")
        .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", "mock-model");
    let failed = world.output(&mut failing_claude).await;
    assert!(!failed.status.success(), "{}", output_text(&failed));
    assert!(
        output_text(&failed).contains("deterministic Claude inference failure"),
        "{}",
        output_text(&failed)
    );
    assert_eq!(inference.requests().len(), 3);

    let scenario = IngestScenario::new()
        .expect("Claude trace source", |row| {
            row_contains(row, &[r#""source":"claude-code""#, "test_harness"])
        })
        .expect("Claude tool output", |row| {
            row_contains(row, &[r#""type":"tool""#, "CLAUDE_TOOL_OK"])
        });
    let rows = world.wait_for_ingest_scenario(&scenario).await;
    assert!(!rows.is_empty());
}

async fn run_claude_live() {
    let world = AgentTestWorld::start().await;
    let mut claude = claude_command(&world);
    claude.arg("Reply briefly to confirm this tracing integration test.");
    let output = world.output(&mut claude).await;
    assert!(output.status.success(), "{}", output_text(&output));

    let scenario = IngestScenario::new().expect("Claude trace source", |row| {
        row_contains(row, &[r#""source":"claude-code""#, "test_harness"])
    });
    let rows = world.wait_for_ingest_scenario(&scenario).await;
    assert!(!rows.is_empty());
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

#[test]
fn test_mode_defaults_to_mock_and_accepts_documented_values() {
    // Parsing itself is intentionally tiny and covered indirectly by every
    // agent integration run. Keep the enum shape explicit for future modes.
    assert_ne!(AgentTestMode::Mock, AgentTestMode::Live);
}
