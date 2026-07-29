# Deterministic inference test support

This directory contains two protocol-faithful, test-only inference servers:

- `OpenAiMock` implements the OpenAI Responses API surface used by Codex.
- `AnthropicMock` implements the Anthropic Messages API surface used by
  Claude Code.

They intentionally share only generic HTTP lifecycle, request indexing, and
transport outcomes. Request and response types remain provider-specific so a
test cannot accidentally hide a wire-protocol incompatibility behind a common
model abstraction.

Both mocks accept a thread-safe closure:

```rust,ignore
let mock = OpenAiMock::start(|context, request| {
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
}).await;
```

`MockReply` supports normal provider responses, arbitrary HTTP errors, and raw
response bodies for malformed or truncated stream tests. Typed turn builders
generate deterministic ids, token usage, and valid provider SSE sequences.
Every inference request is captured for later assertions.

The modules do not depend on `bt-daemon`. The higher-level
`support::agent_process` harness owns daemon, plugin, agent-process, and trace
collector integration. This boundary is deliberate so the inference mocks and
generic server handle can later move into a reusable crate without carrying
Braintrust-specific concepts with them.

`agent_e2e.rs` runs real Codex and Claude Code processes against these mocks.
The tests are ignored in the normal Rust suite because they require agent
executables, while the dedicated CI job installs the latest release of each
agent on every run. This is intentionally unpinned so upstream compatibility
breaks are visible immediately.
