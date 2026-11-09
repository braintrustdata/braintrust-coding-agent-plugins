#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]

mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::agent_process::AgentTestWorld;
use support::agents::{
    ClaudeAgent, ClaudeRun, CodexAgent, CodexRun, OpenCodeAgent, OpenCodeRun, PiAgent, PiRun,
};
use support::inference::{
    AnthropicMock, AnthropicRequest, AnthropicTurn, MockReply, OpenAiMock, OpenAiRequest,
    OpenAiTurn,
};
use support::ingest::IngestScenario;
use support::server::TestServer;

fn shell_tool_call(request: &OpenAiRequest, call_id: &str, command: &str) -> OpenAiTurn {
    let names = request.tool_names();
    let name = ["bash", "exec_command", "shell", "shell_command"]
        .into_iter()
        .find(|name| names.contains(name))
        .unwrap_or_else(|| {
            panic!("agent offered no supported shell tool; offered tools: {names:?}")
        });
    let arguments = match name {
        "exec_command" => json!({"cmd": command, "login": false}),
        "bash" => json!({"command": command, "description": "Print the integration marker"}),
        _ => json!({"command": command}),
    };
    OpenAiTurn::tool_call(call_id, name, arguments)
}

fn tool_command(marker: &str) -> String {
    #[cfg(unix)]
    {
        format!("printf {marker}")
    }
    #[cfg(windows)]
    {
        format!("Write-Output {marker}")
    }
}

fn row_contains(row: &Value, fragments: &[&str]) -> bool {
    let serialized = serde_json::to_string(row).expect("serialize trace row");
    fragments
        .iter()
        .all(|fragment| serialized.contains(fragment))
}

fn opencode_test_env(
    workspace: &std::path::Path,
    home: &std::path::Path,
) -> [(std::ffi::OsString, std::ffi::OsString); 6] {
    [
        ("PWD".into(), workspace.into()),
        ("HOME".into(), home.into()),
        ("USERPROFILE".into(), home.into()),
        ("XDG_CONFIG_HOME".into(), home.join("config").into()),
        ("XDG_DATA_HOME".into(), home.join("data").into()),
        ("XDG_CACHE_HOME".into(), home.join("cache").into()),
    ]
}

fn opencode_v2_standalone(
    workspace: &std::path::Path,
    home: &std::path::Path,
    args: &[&str],
) -> tokio::process::Command {
    let mut command = support::agent_process::command(
        std::env::var_os("OPENCODE_V2_BIN").expect("set OPENCODE_V2_BIN"),
    );
    command
        .args(["run", "--standalone", "--model", "mock/mock-model"])
        .args(args)
        .current_dir(workspace)
        .envs(opencode_test_env(workspace, home));
    command
}

#[cfg(unix)]
#[tokio::test]
async fn agent_output_does_not_wait_for_inherited_descendant_handles() {
    let world = AgentTestWorld::start().await;
    let mut command = support::agent_process::command("sh");
    command.args(["-c", "sleep 5 & printf captured"]);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        world.output(&mut command),
    )
    .await
    .expect("foreground command should not wait for its background child");
    assert!(output.status.success());
    assert_eq!(output.stdout, b"captured");
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
                MockReply::response(shell_tool_call(
                    &request,
                    "call_mock_1",
                    &tool_command("CODEX_TOOL_OK"),
                ))
            }
            1 => {
                assert!(
                    request.has_function_output("call_mock_1"),
                    "Codex did not return the tool result: {}",
                    request.body
                );
                MockReply::response(shell_tool_call(
                    &request,
                    "call_mock_failure",
                    &format!("{}; exit 7", tool_command("CODEX_TOOL_FAILED")),
                ))
            }
            2 => {
                assert!(request.has_function_output("call_mock_failure"));
                MockReply::response(OpenAiTurn::text("CODEX_MOCK_OK"))
            }
            3 => MockReply::http_error(
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
        assert_eq!(inference.requests().len(), 3);

        let failed = codex
            .run(
                &world,
                CodexRun::new("Trigger the deterministic inference error.")
                    .mock_inference(inference_server.uri()),
            )
            .await;
        failed.assert_failure();
        failed.assert_contains("deterministic Codex inference failure");
        assert_eq!(inference.requests().len(), 4);
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
                    && row["error"].is_null()
                    && row["metadata"]["command_outcomes"][0]["exit_code"] == 0
            })
            .expect("Codex command failure", |row| {
                row_contains(row, &["CODEX_TOOL_FAILED"])
                    && row["error"] == "Command exited with code 7"
                    && row["metadata"]["command_outcomes"][0]["outcome"] == "nonzero_exit"
                    && row["metadata"]["command_outcomes"][0]["exit_code"] == 7
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
        // Claude Code may retry the failed request, so keep failing it.
        _ => MockReply::http_error(
            StatusCode::BAD_REQUEST,
            json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "deterministic Claude inference failure"
                }
            }),
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
        assert!(inference.requests().len() >= 3);
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
        if request.tool_names().is_empty() {
            return MockReply::response(OpenAiTurn::text("OpenCode harness"));
        }
        if request.has_function_output("call_opencode_1") {
            return MockReply::response(OpenAiTurn::text("OPENCODE_MOCK_OK"));
        }
        MockReply::response(shell_tool_call(
            &request,
            "call_opencode_1",
            &tool_command("OPENCODE_TOOL_OK"),
        ))
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let opencode = OpenCodeAgent::new(&world);
    let plugin_version = opencode.plugin_version();

    let output = opencode
        .run(
            &world,
            OpenCodeRun::new(
                "Run a shell command that prints OPENCODE_TOOL_OK, then reply with OPENCODE_MOCK_OK.",
            )
                .mock_inference(inference_server.uri()),
        )
        .await;
    output.assert_success();
    if world.uses_mock_inference() {
        output.assert_contains("OPENCODE_MOCK_OK");
        assert!(
            inference
                .requests()
                .iter()
                .any(|request| request.has_function_output("call_opencode_1")),
            "OpenCode did not return the tool result: {}",
            output.text()
        );
    }

    let rows = world.wait_for_trace_delivery().await;
    if world.uses_mock_ingest() {
        assert!(
            rows.iter()
                .any(|row| row_contains(row, &["braintrust.plugin.opencode", &plugin_version])),
            "OpenCode trace origin metadata was not emitted"
        );
    }
    if world.uses_mock_inference() && world.uses_mock_ingest() {
        let scenario = IngestScenario::new()
            .expect("OpenCode trace origin", move |row| {
                row_contains(row, &["braintrust.plugin.opencode", &plugin_version])
            })
            .expect("OpenCode turn input", |row| {
                row_contains(row, &["Turn 1", "OPENCODE_TOOL_OK"])
            })
            .expect("OpenCode tool span", |row| {
                row_contains(row, &[r#""type":"tool""#, "OPENCODE_TOOL_OK"])
            })
            .expect("OpenCode LLM span", |row| {
                row_contains(row, &[r#""type":"llm""#, "call_opencode_1"])
            })
            .expect("OpenCode final LLM span", |row| {
                row_contains(row, &[r#""type":"llm""#, "OPENCODE_MOCK_OK"])
            });
        world.wait_for_mock_ingest_scenario(&scenario).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the Pi CLI and built extension"]
async fn pi_session_emits_traces() {
    let inference = OpenAiMock::new(|_context, request| {
        assert!(request.model().is_some());
        if request.has_function_output("call_pi_1") {
            return MockReply::response(OpenAiTurn::text("PI_MOCK_OK"));
        }
        MockReply::response(shell_tool_call(
            &request,
            "call_pi_1",
            &tool_command("PI_TOOL_OK"),
        ))
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let pi = PiAgent::new(&world);
    let plugin_version = pi.plugin_version();

    let output = pi
        .run(
            &world,
            PiRun::new("Run a shell command that prints PI_TOOL_OK, then reply with PI_MOCK_OK.")
                .mock_inference(inference_server.uri()),
        )
        .await;
    output.assert_success();
    if world.uses_mock_inference() {
        output.assert_contains("PI_MOCK_OK");
        assert!(
            !output.text().contains("PI_MOCK_OKPI_MOCK_OK"),
            "Pi rendered the final response twice: {}",
            output.text()
        );
        assert!(
            inference
                .requests()
                .iter()
                .any(|request| request.has_function_output("call_pi_1")),
            "Pi did not return the tool result: {}",
            output.text()
        );
    }

    let rows = world.wait_for_trace_delivery().await;
    if world.uses_mock_ingest() {
        assert!(
            rows.iter()
                .any(|row| row_contains(row, &["braintrust.plugin.pi", &plugin_version])),
            "Pi trace origin metadata was not emitted"
        );
        assert!(
            rows.iter().any(|row| row["span_attributes"]["name"] == "Pi"
                && row["metadata"]["pi_version"].is_string()),
            "Pi root span did not report the running Pi version"
        );
    }
    if world.uses_mock_inference() && world.uses_mock_ingest() {
        let scenario = IngestScenario::new()
            .expect("Pi LLM span", |row| {
                row_contains(row, &[r#""type":"llm""#, "PI_MOCK_OK"])
            })
            .expect("Pi tool span", |row| {
                row_contains(row, &[r#""type":"tool""#, "PI_TOOL_OK"])
            });
        world.wait_for_mock_ingest_scenario(&scenario).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Pi 1.0.4+ and the packed Pi extension"]
async fn pi_codemode_traces_nested_tools_and_images() {
    let inference = OpenAiMock::new(|_context, request| {
        if request.has_function_output("call_pi_cm") {
            return MockReply::response(OpenAiTurn::text("PI_MOCK_OK"));
        }
        assert!(
            request.tool_names().contains(&"codemode"),
            "Pi did not offer codemode: {:?}",
            request.tool_names()
        );
        let script = format!(
            "const b = await tools.bash({{command: {:?}}}); const r = await tools.read({{path: 'tiny.png'}}); image(r); text(b.output);",
            tool_command("PI_NESTED_OK")
        );
        MockReply::response(OpenAiTurn::tool_call(
            "call_pi_cm",
            "codemode",
            json!({"code": script}),
        ))
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let image_path = world.workspace().join("tiny.png");
    std::fs::write(&image_path, include_bytes!("fixtures/images/tiny.png"))
        .expect("write Pi image fixture");
    let pi = PiAgent::new(&world);

    let output = pi
        .run(
            &world,
            PiRun::new("Use codemode to read and show tiny.png, print PI_NESTED_OK, then reply with PI_MOCK_OK.")
                .mock_inference(inference_server.uri())
                .supports_images()
                .arg("-e")
                .arg("builtin:codemode")
                .arg("--tools")
                .arg("read,bash,codemode"),
        )
        .await;
    output.assert_success();
    if !world.uses_mock_inference() || !world.uses_mock_ingest() {
        return;
    }
    output.assert_contains("PI_MOCK_OK");

    // Codemode runs nested tools through ctx.executeTool(). Both calls should
    // have child spans under the codemode span, and image blocks should remain
    // available on the nested read span and the following LLM input.
    let scenario = IngestScenario::new()
        .expect("Pi codemode span", |row| {
            row_contains(row, &[r#""type":"tool""#, r#""name":"codemode""#])
        })
        .expect("Pi nested bash span", |row| {
            row_contains(row, &[r#""type":"tool""#, "PI_NESTED_OK"])
        })
        .expect("Pi nested image read span", |row| {
            row_contains(
                row,
                &[
                    r#""type":"tool""#,
                    r#""name":"read""#,
                    r#""type":"image""#,
                    "image/png",
                ],
            )
        });
    let rows = world.wait_for_mock_ingest_scenario(&scenario).await;
    let span_named = |name: &str| {
        rows.iter()
            .find(|row| {
                row["span_attributes"]["type"] == "tool" && row["span_attributes"]["name"] == name
            })
            .unwrap_or_else(|| panic!("no {name} tool span in {rows:#?}"))
    };
    let codemode = span_named("codemode");
    let bash = span_named("bash");
    assert_eq!(
        bash["span_parents"],
        json!([codemode["span_id"]]),
        "nested bash span is not a child of the codemode span"
    );
    let image_read = rows
        .iter()
        .find(|row| {
            row["span_attributes"]["type"] == "tool"
                && row["span_attributes"]["name"] == "read"
                && row.to_string().contains(r#""type":"image""#)
        })
        .expect("read span should preserve Pi's image block");
    assert_eq!(image_read["span_parents"], json!([codemode["span_id"]]));
    assert!(image_read.to_string().contains("image/png"));
    assert!(image_read.to_string().contains("iVBORw0KGgo"));
    assert!(
        rows.iter().any(|row| {
            row["span_attributes"]["type"] == "llm"
                && row.to_string().contains(r#""type":"image""#)
                && row.to_string().contains("image/png")
        }),
        "the following LLM span should retain the image attached by codemode"
    );
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
    match shell_tool_call(&openai, "call_mock_1", "echo CODEX_TOOL_OK") {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2 and a built OpenCode package"]
async fn opencode_v2_session_emits_traces() {
    use base64::Engine;
    let inference = OpenAiMock::new(|_, request| {
        if request.body["messages"]
            .as_array()
            .and_then(|messages| {
                messages
                    .iter()
                    .rev()
                    .find(|message| message["role"] == "user")
            })
            .is_some_and(|message| message.to_string().contains("model failure marker"))
        {
            return MockReply::http_error(
                axum::http::StatusCode::BAD_REQUEST,
                json!({"error":{"message":"MODEL_FAILURE_FIXTURE","type":"invalid_request_error"}}),
            );
        }
        let tools = request.body["tools"].as_array();
        let has_tool_result = request.body["messages"]
            .as_array()
            .is_some_and(|messages| messages.iter().any(|message| message["role"] == "tool"));
        if tools.is_some() && !has_tool_result {
            if request.contains_text("image tool marker") {
                return MockReply::response(OpenAiTurn::tool_call(
                    "call_image",
                    "fixture_image",
                    json!({}),
                ));
            }
            let command = if request.contains_text("interrupt marker") {
                if cfg!(windows) {
                    "Start-Sleep -Seconds 30".to_string()
                } else {
                    "sleep 30".to_string()
                }
            } else {
                tool_command("OPENCODE_V2_TOOL_OK")
            };
            MockReply::response(OpenAiTurn::tool_call(
                "call_v2",
                "shell",
                json!({"command":command,"description":"Run a test command"}),
            ))
        } else {
            MockReply::response(OpenAiTurn::text("OPENCODE_V2_OK"))
        }
    });
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let workspace = world.workspace();
    let image = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEElEQVR4nGP4z8AARAwQCgAf7gP9i18U1AAAAABJRU5ErkJggg==").unwrap();
    std::fs::write(workspace.join("image.png"), image).unwrap();
    let home = world.temp_path("opencode-v2-home");
    std::fs::create_dir_all(&home).unwrap();
    let config = json!({
        "model": "mock/mock-model",
        "providers": {"mock": {"package":"@opencode/ai/providers/openai-compatible", "settings":{"baseURL":format!("{}/v1", inference_server.uri()),"apiKey":"test-key"},"models":{"mock-model":{"name":"Mock Model"}}}}
    });
    std::fs::write(
        workspace.join("opencode.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let mut command = support::agent_process::daemon_command();
    command
        .args([
            "run",
            "--project",
            "agent-e2e",
            "opencode",
            "run",
            "--auto",
            "--model",
            "mock/mock-model",
            "--file",
            "image.png",
            "say hello",
        ])
        .current_dir(&workspace)
        .envs(opencode_test_env(&workspace, &home))
        .env(
            "OPENCODE_BIN",
            std::env::var_os("OPENCODE_V2_BIN").expect("set OPENCODE_V2_BIN"),
        )
        .env(
            "BRAINTRUST_TRACE_OPENCODE_PLUGIN_SPEC",
            std::env::var_os("OPENCODE_V2_PLUGIN_DIR").expect("set OPENCODE_V2_PLUGIN_DIR"),
        );
    world.configure(&mut command);
    let output = world.output(&mut command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        inference
            .requests()
            .iter()
            .any(|request| request.contains_text("braintrust_list_projects")),
        "Braintrust tools were not exposed to the managed OpenCode 2 run"
    );
    let scenario = IngestScenario::new()
        .expect("v2 input", |row| {
            row_contains(row, &["Turn 1", "say hello", "data:image/png;base64,"])
        })
        .expect("v2 tool", |row| {
            row_contains(row, &[r#""type":"tool""#, "OPENCODE_V2_TOOL_OK"])
        })
        .expect("v2 LLM", |row| {
            row_contains(row, &[r#""type":"llm""#, "OPENCODE_V2_OK"])
        });
    let rows = world.wait_for_mock_ingest_scenario(&scenario).await;
    let final_llm = rows
        .iter()
        .find(|row| {
            row["span_attributes"]["type"] == "llm"
                && row["output"].to_string().contains("OPENCODE_V2_OK")
        })
        .unwrap();
    assert_eq!(final_llm["metadata"]["provider"], "mock");
    assert_eq!(final_llm["metadata"]["model"], "mock-model");
    assert_eq!(final_llm["metrics"]["prompt_tokens"].as_f64(), Some(10.0));
    assert_eq!(
        final_llm["metrics"]["completion_tokens"].as_f64(),
        Some(5.0)
    );
    assert_eq!(final_llm["metrics"]["tokens"].as_f64(), Some(15.0));
    assert!(
        final_llm["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "tool"
                && message["tool_call_id"] == "call_v2"
                && message["content"]
                    .to_string()
                    .contains("OPENCODE_V2_TOOL_OK")),
        "unexpected model input: {}",
        final_llm["input"]
    );

    let managed_turn_id = rows
        .iter()
        .find(|row| row_contains(row, &["Turn 1", "say hello", "data:image/png;base64,"]))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new().expect("managed v2 completed turn", move |row| {
                row["id"] == managed_turn_id && row["output"] == "OPENCODE_V2_OK"
            }),
        )
        .await;

    // Exercise the persistent plugin configuration separately from managed runs.
    let mut persistent_config = config.clone();
    persistent_config["plugin"] = json!([std::env::var("OPENCODE_V2_PLUGIN_DIR").unwrap()]);
    std::fs::write(
        workspace.join("opencode.json"),
        serde_json::to_vec(&persistent_config).unwrap(),
    )
    .unwrap();
    let config_dir = home.join("config/opencode");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(
        config_dir.join("braintrust.json"),
        serde_json::to_vec(&json!({
            "trace_to_braintrust": true,
            "route": {"destination":{"type":"project_logs","project_name":"agent-e2e"}}
        }))
        .unwrap(),
    )
    .unwrap();
    let mut persistent =
        opencode_v2_standalone(&workspace, &home, &["--auto", "persistent marker"]);
    world.configure(&mut persistent);
    let output = world.output(&mut persistent).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        inference
            .requests()
            .iter()
            .any(|request| request.contains_text("braintrust_list_projects")),
        "Braintrust tools were not exposed to OpenCode 2"
    );
    world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("persistent v2 turn", |row| {
            row_contains(row, &["Turn 1", "persistent marker"])
        }))
        .await;

    persistent_config["permissions"] = json!([{"action":"shell","resource":"*","effect":"ask"}]);
    std::fs::write(
        workspace.join("opencode.json"),
        serde_json::to_vec(&persistent_config).unwrap(),
    )
    .unwrap();
    let plugins = workspace.join(".opencode/plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    std::fs::write(plugins.join("reject.js"), r#"export default { id: 'test.reject', setup(ctx) { const c = new AbortController(); void (async () => { for await (const event of ctx.event.subscribe({signal:c.signal})) { if (event.type === 'permission.asked') await ctx.permission.reply({sessionID:event.data.sessionID,requestID:event.data.id,decision:'reject',message:'test denied'}); } })().catch(() => {}); return () => c.abort(); } };"#).unwrap();
    let mut rejected = opencode_v2_standalone(&workspace, &home, &["permission marker"]);
    world.configure(&mut rejected);
    // OpenCode 2.0 exits nonzero after a rejection; later releases continue.
    let _output = world.output(&mut rejected).await;
    world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new()
                .expect("permission turn", |row| {
                    row_contains(row, &["Turn 1", "permission marker"])
                })
                .expect("denied shell", |row| {
                    row["span_attributes"]["type"] == "tool"
                        && row["metadata"]["tool_approval"] == "denied"
                }),
        )
        .await;

    std::fs::remove_file(plugins.join("reject.js")).unwrap();
    std::fs::write(plugins.join("interrupt.js"), r#"export default { id: 'test.interrupt', setup(ctx) { const c = new AbortController(); void (async () => { for await (const event of ctx.event.subscribe({signal:c.signal})) { if (event.type === 'session.tool.called') await ctx.session.interrupt({sessionID:event.data.sessionID}); } })().catch(() => {}); return () => c.abort(); } };"#).unwrap();
    persistent_config
        .as_object_mut()
        .unwrap()
        .remove("permissions");
    std::fs::write(
        workspace.join("opencode.json"),
        serde_json::to_vec(&persistent_config).unwrap(),
    )
    .unwrap();
    let mut interrupted =
        opencode_v2_standalone(&workspace, &home, &["--auto", "interrupt marker"]);
    world.configure(&mut interrupted);
    let _output = world.output(&mut interrupted).await;
    world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("interrupted turn", |row| {
            row_contains(
                row,
                &["Turn 1", "interrupt marker", "OpenCode session interrupted"],
            )
        }))
        .await;

    std::fs::remove_file(plugins.join("interrupt.js")).unwrap();
    let mut failed = opencode_v2_standalone(&workspace, &home, &["--auto", "model failure marker"]);
    world.configure(&mut failed);
    let _output = world.output(&mut failed).await;
    let failure_rows = world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new()
                .expect("failed model turn", |row| {
                    row_contains(row, &["Turn 1", "model failure marker"])
                })
                .expect("model error", |row| {
                    row["error"].to_string().contains("MODEL_FAILURE_FIXTURE")
                }),
        )
        .await;
    assert!(failure_rows
        .iter()
        .any(|row| row["error"].to_string().contains("MODEL_FAILURE_FIXTURE")));

    let failed_turn = failure_rows
        .iter()
        .find(|row| row_contains(row, &["Turn 1", "model failure marker"]))
        .unwrap();
    let failed_root = failed_turn["root_span_id"].clone();
    let native_session = failure_rows
        .iter()
        .find(|row| row["span_id"] == failed_root && row["metadata"]["session_id"].is_string())
        .expect("failed native session root")["metadata"]["session_id"]
        .as_str()
        .unwrap();
    let mut resumed = opencode_v2_standalone(
        &workspace,
        &home,
        &[
            "--auto",
            "--session",
            native_session,
            "model recovery marker",
        ],
    );
    world.configure(&mut resumed);
    let output = world.output(&mut resumed).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let recovered_rows = world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new().expect("resumed failed session", |row| {
                row_contains(row, &["Turn 2", "model recovery marker"])
            }),
        )
        .await;
    assert!(recovered_rows
        .iter()
        .any(|row| row["root_span_id"] == failed_root
            && row_contains(row, &["Turn 2", "model recovery marker"])));

    std::fs::write(plugins.join("image.js"), r#"export default { id:'test.image', async setup(ctx) { await ctx.tool.transform(editor => editor.add({name:'fixture_image',description:'Return an image',input:{type:'object',properties:{},additionalProperties:false},options:{codemode:false},async execute() { return {content:[{type:'file',mime:'image/png',uri:'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEElEQVR4nGP4z8AARAwQCgAf7gP9i18U1AAAAABJRU5ErkJggg=='}]}; }})); } };"#).unwrap();
    let mut image_run = support::agent_process::daemon_command();
    image_run
        .args([
            "run",
            "--project",
            "agent-e2e",
            "opencode",
            "run",
            "--auto",
            "--model",
            "mock/mock-model",
            "image tool marker",
        ])
        .current_dir(&workspace)
        .envs(opencode_test_env(&workspace, &home))
        .env("OPENCODE_BIN", std::env::var_os("OPENCODE_V2_BIN").unwrap())
        .env(
            "BRAINTRUST_TRACE_OPENCODE_PLUGIN_SPEC",
            std::env::var_os("OPENCODE_V2_PLUGIN_DIR").unwrap(),
        );
    world.configure(&mut image_run);
    let output = world.output(&mut image_run).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let image_rows = world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new()
                .expect("image tool turn", |row| {
                    row_contains(row, &["Turn 1", "image tool marker"])
                })
                .expect("image tool result", |row| {
                    row["span_attributes"]["type"] == "tool"
                        && row["output"].to_string().contains("image_url")
                        && row["output"].to_string().contains("image/png")
                })
                .expect("model input with tool image", |row| {
                    row["span_attributes"]["type"] == "llm"
                        && row["input"].to_string().contains("image tool marker")
                        && row["input"].to_string().contains("image/png")
                        && row["input"].to_string().contains("image_url")
                }),
        )
        .await;
    assert!(
        image_rows
            .iter()
            .any(|row| row["span_attributes"]["type"] == "llm"
                && row["input"].to_string().contains("image tool marker")
                && row["input"].to_string().contains("image/png")
                && row["input"].to_string().contains("image_url")),
        "model input lost tool image: {image_rows:?}"
    );
    let database = rusqlite::Connection::open(home.join("data/opencode/opencode.db")).unwrap();
    let native_session: String = database.query_row(
        "SELECT session_id FROM session_message WHERE type='user' AND data LIKE '%image tool marker%' LIMIT 1",
        [], |row| row.get(0),
    ).unwrap();
    let recovery_journal =
        bt_daemon::source_journal_path(&world.temp_path("daemon"), "opencode", &native_session);
    let recovery = std::fs::read_to_string(recovery_journal).unwrap();
    assert!(
        recovery.contains("session.snapshot"),
        "native projection recovery was not journaled; journal: {recovery}; daemon stderr: {}; run stderr: {}",
        std::fs::read_to_string(world.temp_path("daemon/daemon-stderr.log")).unwrap_or_default(),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2 and a built OpenCode package"]
async fn opencode_v2_subagent_is_nested() {
    let inference = OpenAiMock::new(|_, request| {
        let messages = request.body["messages"].as_array();
        let user = messages
            .and_then(|ms| ms.iter().rev().find(|m| m["role"] == "user"))
            .map(Value::to_string)
            .unwrap_or_default();
        let results = messages
            .into_iter()
            .flatten()
            .filter(|m| m["role"] == "tool")
            .collect::<Vec<_>>();
        if user.contains("CHILD_MARKER") {
            MockReply::response(OpenAiTurn::text("CHILD_DONE"))
        } else if request.body["tools"].is_array() && results.is_empty() {
            MockReply::response(OpenAiTurn::tool_call(
                "subagent_call",
                "subagent",
                json!({"agent":"general","description":"Test child","prompt":"CHILD_MARKER"}),
            ))
        } else if results.len() == 1 {
            let content = results[0]["content"]
                .as_str()
                .expect("subagent result text");
            let session = content
                .split("sessionID=\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .expect("subagent session ID");
            MockReply::response(OpenAiTurn::tool_call(
                "subagent_resume",
                "subagent",
                json!({"agent":"general","description":"Continue child","sessionID":session,"prompt":"CHILD_MARKER_SECOND"}),
            ))
        } else {
            MockReply::response(OpenAiTurn::text("PARENT_DONE"))
        }
    });
    let server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let workspace = world.workspace();
    let home = world.temp_path("v2-subagent-home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(workspace.join("opencode.json"), serde_json::to_vec(&json!({
        "model":"mock/mock-model", "providers":{"mock":{"package":"@opencode/ai/providers/openai-compatible","settings":{"baseURL":format!("{}/v1",server.uri()),"apiKey":"test"},"models":{"mock-model":{"name":"Mock"}}}}
    })).unwrap()).unwrap();
    let mut command = support::agent_process::daemon_command();
    command
        .args([
            "run",
            "--project",
            "agent-e2e",
            "opencode",
            "run",
            "--auto",
            "--model",
            "mock/mock-model",
            "spawn subagent",
        ])
        .current_dir(&workspace)
        .envs(opencode_test_env(&workspace, &home))
        .env(
            "OPENCODE_BIN",
            std::env::var_os("OPENCODE_V2_BIN").expect("set OPENCODE_V2_BIN"),
        )
        .env(
            "BRAINTRUST_TRACE_OPENCODE_PLUGIN_SPEC",
            std::env::var_os("OPENCODE_V2_PLUGIN_DIR").expect("set OPENCODE_V2_PLUGIN_DIR"),
        );
    world.configure(&mut command);
    let output = world.output(&mut command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = world.wait_for_trace_delivery().await;
    let root = rows
        .iter()
        .find(|r| r["span_attributes"]["name"] == "OpenCode")
        .expect("root span");
    let child = rows
        .iter()
        .find(|r| r["metadata"]["is_subagent"] == true)
        .expect("child session span");
    assert_eq!(child["root_span_id"], root["root_span_id"]);
    assert!(child["span_parents"]
        .as_array()
        .is_some_and(|parents| !parents.is_empty()));
    assert!(
        rows.iter().any(|row| row_contains(row, &["CHILD_DONE"])),
        "child result missing: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["span_attributes"]["name"] == "Turn 2"
                && row["span_parents"]
                    .as_array()
                    .is_some_and(|parents| parents.contains(&child["span_id"]))
                && row.to_string().contains("CHILD_MARKER_SECOND")),
        "continued child turn missing: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2 and a built OpenCode package"]
async fn opencode_v2_compaction_after_idle() {
    run_v2_compaction_after_idle(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2.1+ and a built OpenCode package"]
async fn opencode_v2_native_compaction_after_idle() {
    run_v2_compaction_after_idle(true).await;
}

async fn run_v2_compaction_after_idle(native: bool) {
    let binary = std::env::var_os("OPENCODE_V2_BIN").expect("set OPENCODE_V2_BIN");
    if native {
        let version = support::agent_process::command(&binary)
            .arg("--version")
            .output()
            .await
            .unwrap();
        if String::from_utf8_lossy(&version.stdout).contains("v2.0.") {
            return;
        }
    }
    let summary = if native {
        "NATIVE_COMPACTION_WINDOW"
    } else {
        "LEGACY_COMPACTION_SUMMARY"
    };
    let inference = OpenAiMock::new(|_, _| MockReply::response(OpenAiTurn::text("LEGACY_TEXT")));
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let workspace = world.workspace();
    let home = world.temp_path("v2-legacy-compact-home");
    let config_dir = home.join("config/opencode");
    let plugins = workspace.join(".opencode/plugins");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    std::fs::write(config_dir.join("braintrust.json"), serde_json::to_vec(&json!({"trace_to_braintrust":true,"route":{"destination":{"type":"project_logs","project_name":"agent-e2e"}}})).unwrap()).unwrap();
    std::fs::write(workspace.join("opencode.json"), serde_json::to_vec(&json!({
        "plugin":[std::env::var("OPENCODE_V2_PLUGIN_DIR").unwrap()], "model":"mock/mock-model", "providers":{"mock":{"package":"@opencode/ai/providers/openai-compatible","settings":{"baseURL":format!("{}/v1",inference_server.uri()),"apiKey":"test"},"models":{"mock-model":{"name":"Mock"}}}}
    })).unwrap()).unwrap();
    let hook = if native {
        "export default {id:'test.compact',async setup(ctx){await ctx.session.hook('compaction',e=>{e.result={summary:'',native:{messages:[{role:'user',content:[{type:'text',text:'NATIVE_COMPACTION_WINDOW'}]}]}}})}};"
    } else {
        "export default {id:'test.compact',async setup(ctx){await ctx.session.hook('compaction',e=>{e.result={summary:'LEGACY_COMPACTION_SUMMARY'}})}};"
    };
    std::fs::write(plugins.join("compact.js"), hook).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let mut server = support::agent_process::command(&binary);
    server
        .args([
            "serve",
            "--hostname",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .current_dir(&workspace)
        .envs(opencode_test_env(&workspace, &home))
        .env("OPENCODE_PASSWORD", "integration-test-password")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    world.configure(&mut server);
    let mut server = server.spawn().unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut ready = false;
    for _ in 0..100 {
        if client
            .get(format!("{url}/api/session"))
            .basic_auth("opencode", Some("integration-test-password"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready, "OpenCode 2 server did not start");
    let mut run = support::agent_process::command(&binary);
    run.args([
        "run",
        "--server",
        &url,
        "--auto",
        "--model",
        "mock/mock-model",
        "--format",
        "json",
        "legacy compaction",
    ])
    .current_dir(&workspace)
    .envs(opencode_test_env(&workspace, &home))
    .env("OPENCODE_PASSWORD", "integration-test-password");
    let output = world.output(&mut run).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let session_id = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|event| event["sessionID"].as_str().map(str::to_owned))
        .expect("session ID");
    let initial_rows = world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("initial turn", |row| {
            row_contains(row, &["Turn 1", "legacy compaction"])
        }))
        .await;
    let root_id = initial_rows
        .iter()
        .find(|row| row_contains(row, &["Turn 1", "legacy compaction"]))
        .unwrap()["root_span_id"]
        .clone();
    let compact_request = client
        .post(format!("{url}/api/session/{session_id}/compact"))
        .basic_auth("opencode", Some("integration-test-password"))
        .json(&json!({"delivery":"queue"}))
        .build()
        .unwrap();
    client
        .execute(compact_request.try_clone().unwrap())
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let compacted_rows = world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("idle compaction", |row| {
            row_contains(row, &[r#""type":"llm""#, summary])
        }))
        .await;
    assert!(compacted_rows
        .iter()
        .any(|row| row_contains(row, &[r#""type":"llm""#, summary])
            && row["root_span_id"] == root_id));
    let mut resume = support::agent_process::command(&binary);
    resume
        .args([
            "run",
            "--server",
            &url,
            "--auto",
            "--model",
            "mock/mock-model",
            "--session",
            &session_id,
            "legacy after compaction",
        ])
        .current_dir(&workspace)
        .envs(opencode_test_env(&workspace, &home))
        .env("OPENCODE_PASSWORD", "integration-test-password");
    let output = world.output(&mut resume).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let resumed_rows = world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("resumed context", |row| {
            row_contains(
                row,
                &[r#""type":"llm""#, summary, "legacy after compaction"],
            )
        }))
        .await;
    let resumed_llm = resumed_rows
        .iter()
        .find(|row| {
            row_contains(
                row,
                &[r#""type":"llm""#, summary, "legacy after compaction"],
            )
        })
        .unwrap();
    assert_eq!(resumed_llm["root_span_id"], root_id);
    assert!(!resumed_llm["input"]
        .to_string()
        .contains("legacy compaction"));
    if native {
        client
            .execute(compact_request)
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        let second = world
            .wait_for_mock_ingest_scenario(&IngestScenario::new().expect(
                "second native compaction",
                |row| {
                    row["tags"]
                        .as_array()
                        .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction"))
                        && row["input"].to_string().contains("legacy after compaction")
                },
            ))
            .await;
        let compact = second
            .iter()
            .find(|row| {
                row["tags"]
                    .as_array()
                    .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction"))
                    && row["input"].to_string().contains("legacy after compaction")
            })
            .unwrap();
        assert!(compact["input"]
            .to_string()
            .contains("NATIVE_COMPACTION_WINDOW"));
        assert!(!compact["input"].to_string().contains("legacy compaction"));
    }
    server.kill().await.unwrap();
    server.wait().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2 and a built OpenCode package"]
async fn opencode_v2_standalone_delivers_events_when_daemon_acknowledgements_are_slow() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    let inference =
        OpenAiMock::new(|_, _| MockReply::response(OpenAiTurn::text("SLOW_ACK_RESPONSE")));
    let inference_server = TestServer::start(inference.router()).await;
    let world = AgentTestWorld::start().await;
    let workspace = world.workspace();
    let home = world.temp_path("slow-ack-home");
    let config_dir = home.join("config/opencode");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(config_dir.join("braintrust.json"), serde_json::to_vec(&json!({"trace_to_braintrust":true,"enable_tools":false,"route":{"destination":{"type":"project_logs","project_name":"agent-e2e"}}})).unwrap()).unwrap();
    std::fs::write(workspace.join("opencode.json"), serde_json::to_vec(&json!({
        "plugin":[std::env::var("OPENCODE_V2_PLUGIN_DIR").unwrap()], "model":"mock/mock-model", "providers":{"mock":{"package":"@opencode/ai/providers/openai-compatible","settings":{"baseURL":format!("{}/v1",inference_server.uri()),"apiKey":"test"},"models":{"mock-model":{"name":"Mock"}}}}
    })).unwrap()).unwrap();
    let proxy_path = world.temp_path("slow.sock");
    let listener = UnixListener::bind(&proxy_path).unwrap();
    let target = world.socket().to_owned();
    let (release_acknowledgements, acknowledgements) = tokio::sync::watch::channel(false);
    let proxy = tokio::spawn(async move {
        while let Ok((incoming, _)) = listener.accept().await {
            let target = target.clone();
            let mut acknowledgements = acknowledgements.clone();
            tokio::spawn(async move {
                let Ok(outgoing) = UnixStream::connect(target).await else {
                    return;
                };
                let (mut client_read, mut client_write) = incoming.into_split();
                let (daemon_read, mut daemon_write) = outgoing.into_split();
                let request = async move {
                    let _ = tokio::io::copy(&mut client_read, &mut daemon_write).await;
                    drop(daemon_write);
                };
                let response = async {
                    let mut daemon_read = BufReader::new(daemon_read);
                    let mut buffer = Vec::new();
                    let mut client_closed = false;
                    let mut initializing = true;
                    loop {
                        buffer.clear();
                        match daemon_read.read_until(b'\n', &mut buffer).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                        if initializing {
                            initializing = false;
                        } else {
                            while !*acknowledgements.borrow_and_update() {
                                if acknowledgements.changed().await.is_err() {
                                    break;
                                }
                            }
                        }
                        if !client_closed && client_write.write_all(&buffer).await.is_err() {
                            client_closed = true;
                        }
                    }
                };
                tokio::join!(request, response);
            });
        }
    });
    let mut run = support::agent_process::command(std::env::var_os("OPENCODE_V2_BIN").unwrap());
    run.args([
        "run",
        "--standalone",
        "--auto",
        "--model",
        "mock/mock-model",
        "slow ack marker",
    ])
    .current_dir(&workspace)
    .envs(opencode_test_env(&workspace, &home));
    world.configure(&mut run);
    run.env("BRAINTRUST_DAEMON_SOCKET", &proxy_path);
    let output = tokio::time::timeout(std::time::Duration::from_secs(20), world.output(&mut run))
        .await
        .expect("OpenCode waited for daemon acknowledgements during cleanup");
    release_acknowledgements.send_replace(true);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("standalone turn", |row| {
            row_contains(row, &["Turn 1", "slow ack marker"])
        }))
        .await;
    let turn_id = rows
        .iter()
        .find(|row| row_contains(row, &["Turn 1", "slow ack marker"]))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    world
        .wait_for_mock_ingest_scenario(
            &IngestScenario::new().expect("standalone completed turn", move |row| {
                row["id"] == turn_id && row["output"] == "SLOW_ACK_RESPONSE"
            }),
        )
        .await;
    proxy.abort();
}
