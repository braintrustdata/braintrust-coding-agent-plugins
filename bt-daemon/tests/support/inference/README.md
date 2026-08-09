# Deterministic inference test support

This directory is a self-contained mock-inference component with two
protocol-faithful servers:

- `OpenAiMock` implements the OpenAI Responses API surface used by Codex.
- `AnthropicMock` implements the Anthropic Messages API surface used by
  Claude Code.

Each public mock owns its protocol routes, scenario closure, and captured
requests, and exports an Axum `Router`. Callers can bind that router with the
shared ephemeral test server or embed it in another Axum application. The two
providers share request indexing and transport outcomes. Request and response
types remain provider-specific so a test cannot accidentally hide a
wire-protocol incompatibility behind a common model abstraction.

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

The component does not depend on `bt-daemon`, the coding-agent runner, the
ingest mock, or a particular listener implementation. The higher-level
`support::agent_process` harness composes with it only from the integration
test. This boundary is deliberate so the whole mock-inference component can
later move into a reusable crate and serve any client that can target an
OpenAI Responses or Anthropic Messages endpoint.

`agent_integration.rs` runs real Codex and Claude Code processes against these
mocks.
The tests are ignored in a plain Rust run because they require agent
executables. The core cross-platform CI matrix installs the latest release of
each agent and runs them in the default `mock` mode on every host. This is
intentionally unpinned so upstream compatibility breaks are visible
immediately.

The same agent tests can run without mock inference while continuing to use
captured local ingest:

```console
BT_AGENT_INFERENCE_MODE=live BT_AGENT_INGEST_MODE=mock \
  cargo test --manifest-path bt-daemon/Cargo.toml \
  --all-features --test agent_integration -- --ignored --test-threads=1
```

Live inference uses the normal provider endpoint/model and the agent's normal
login or provider credentials. It validates only stable integration invariants
such as trace delivery and origin metadata. Mock inference additionally
validates exact request sequences, tool results, output content, and injected
failures.

Inference and ingest selection are independent. To drive deterministic model
behavior while reporting traces to the normal Braintrust backend:

```console
BT_AGENT_INFERENCE_MODE=mock BT_AGENT_INGEST_MODE=live \
BT_AGENT_BT_BIN=/path/to/bt BT_AGENT_PROFILE=work \
BT_AGENT_PROJECT=agent-e2e \
  cargo test --manifest-path bt-daemon/Cargo.toml \
  --all-features --test agent_integration -- --ignored --test-threads=1
```

The live-ingest path starts the daemon through `bt`, so OAuth access-token
refresh and profile resolution remain inside the long-lived CLI host. The
standalone daemon is intentionally limited to mock-ingest and explicit API-key
development scenarios.
