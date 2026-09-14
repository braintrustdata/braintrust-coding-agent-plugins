# Deterministic inference test support

Two mocks implement the model API endpoints used by the integration tests:

- `OpenAiMock` implements the OpenAI Responses API used by Codex, OpenCode,
  and Pi tests.
- `AnthropicMock` implements the Anthropic Messages API surface used by
  Claude Code.

Each mock exports an Axum router and records incoming requests. Request and
response types are provider-specific; request indexing and transport outcomes
are shared.

Both mocks accept a thread-safe closure:

```rust,ignore
let mock = OpenAiMock::new(|context, request| {
    match context.request_index {
        0 => MockReply::response(OpenAiTurn::tool_call(
            "call-1",
            "exec_command",
            json!({"cmd":"printf hello"}),
        )),
        1 if request.has_function_output("call-1") => {
            MockReply::response(OpenAiTurn::text("done"))
        }
        index => panic!("unexpected request {index}: {}", request.body),
    }
});
let server = TestServer::start(mock.router()).await;
```

`MockReply` supports normal provider responses, arbitrary HTTP errors, and raw
response bodies for malformed or truncated stream tests. Typed turn builders
generate deterministic ids, token usage, and valid provider SSE sequences.
Every inference request is captured for later assertions.

The mocks can run under the shared test server or another Axum application.
They do not depend on the daemon, ingest mock, or agent runner.

`agent_integration.rs` runs Codex, Claude Code, OpenCode, and Pi against these
mocks. CI installs the latest agents and runs the tests on Linux, macOS, and
Windows. See the [harness guide](../README.md) for executables and package setup.

## Live backends

Run the following commands from the monorepo root after preparing the agents.

The same agent tests can run without mock inference while continuing to use
captured local ingest:

```console
BT_AGENT_INFERENCE_MODE=live BT_AGENT_INGEST_MODE=mock \
  cargo test --manifest-path bt-daemon/Cargo.toml \
  --all-features --locked --test agent_integration -- --ignored --test-threads=1
```

Live inference uses the normal provider endpoint/model and the agent's normal
login or provider credentials. Tests check trace delivery and origin metadata. Mock inference additionally
validates exact request sequences, tool results, output content, and injected
failures.

Inference and ingest selection are independent. To drive deterministic model
behavior while reporting traces to the normal Braintrust backend:

```console
BT_AGENT_INFERENCE_MODE=mock BT_AGENT_INGEST_MODE=live \
BT_AGENT_BT_BIN=/path/to/bt BT_AGENT_PROFILE=work \
BT_AGENT_PROJECT=agent-e2e \
  cargo test --manifest-path bt-daemon/Cargo.toml \
  --all-features --locked --test agent_integration -- --ignored --test-threads=1
```

Live ingest starts the daemon through `bt`, which handles profiles and OAuth
refresh. The standalone test daemon uses explicit API-key authentication.
