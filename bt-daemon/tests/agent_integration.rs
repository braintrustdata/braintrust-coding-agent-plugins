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
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
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
        support::agent_process::command(std::env::var_os("OPENCODE_V2_BIN").unwrap());
    persistent
        .args([
            "run",
            "--standalone",
            "--auto",
            "--model",
            "mock/mock-model",
            "persistent marker",
        ])
        .current_dir(&workspace)
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"));
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
    let mut rejected =
        support::agent_process::command(std::env::var_os("OPENCODE_V2_BIN").unwrap());
    rejected
        .args([
            "run",
            "--standalone",
            "--model",
            "mock/mock-model",
            "permission marker",
        ])
        .current_dir(&workspace)
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"));
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
        support::agent_process::command(std::env::var_os("OPENCODE_V2_BIN").unwrap());
    interrupted
        .args([
            "run",
            "--standalone",
            "--auto",
            "--model",
            "mock/mock-model",
            "interrupt marker",
        ])
        .current_dir(&workspace)
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"));
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
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
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
        let tool_result = messages.is_some_and(|ms| ms.iter().any(|m| m["role"] == "tool"));
        if user.contains("CHILD_MARKER") {
            MockReply::response(OpenAiTurn::text("CHILD_DONE"))
        } else if request.body["tools"].is_array() && !tool_result {
            MockReply::response(OpenAiTurn::tool_call(
                "subagent_call",
                "subagent",
                json!({"agent":"general","description":"Test child","prompt":"CHILD_MARKER"}),
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
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OpenCode 2 and a built OpenCode package"]
async fn opencode_v2_compaction_after_idle() {
    let binary = std::env::var_os("OPENCODE_V2_BIN").expect("set OPENCODE_V2_BIN");
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
    std::fs::write(plugins.join("compact.js"), "export default {id:'test.compact',async setup(ctx){await ctx.session.hook('compaction',e=>{e.result={summary:'LEGACY_COMPACTION_SUMMARY'}})}};").unwrap();
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
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
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
    .env("PWD", &workspace)
    .env("HOME", &home)
    .env("USERPROFILE", &home)
    .env("XDG_CONFIG_HOME", home.join("config"))
    .env("XDG_DATA_HOME", home.join("data"))
    .env("XDG_CACHE_HOME", home.join("cache"))
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
    client
        .post(format!("{url}/api/session/{session_id}/compact"))
        .basic_auth("opencode", Some("integration-test-password"))
        .json(&json!({"delivery":"queue"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let compacted_rows = world
        .wait_for_mock_ingest_scenario(&IngestScenario::new().expect("idle compaction", |row| {
            row_contains(row, &[r#""type":"llm""#, "LEGACY_COMPACTION_SUMMARY"])
        }))
        .await;
    assert!(compacted_rows.iter().any(|row| row_contains(
        row,
        &[r#""type":"llm""#, "LEGACY_COMPACTION_SUMMARY"]
    ) && row["root_span_id"] == root_id));
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
        .env("PWD", &workspace)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
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
                &[
                    r#""type":"llm""#,
                    "LEGACY_COMPACTION_SUMMARY",
                    "legacy after compaction",
                ],
            )
        }))
        .await;
    let resumed_llm = resumed_rows
        .iter()
        .find(|row| {
            row_contains(
                row,
                &[
                    r#""type":"llm""#,
                    "LEGACY_COMPACTION_SUMMARY",
                    "legacy after compaction",
                ],
            )
        })
        .unwrap();
    assert_eq!(resumed_llm["root_span_id"], root_id);
    assert!(!resumed_llm["input"]
        .to_string()
        .contains("legacy compaction"));
    server.kill().await.unwrap();
    server.wait().await.unwrap();
}
