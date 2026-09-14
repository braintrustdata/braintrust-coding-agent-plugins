# Agent integration test architecture

Run these tests from the monorepo root. They launch real agents against mock
model endpoints and mock Braintrust ingest by default.

## Run the tests

Install the agents you want to test. The harness finds `codex`, `claude`,
`opencode`, and `pi` on `PATH`; `CODEX_BIN`, `CLAUDE_BIN`, `OPENCODE_BIN`, and
`PI_BIN` override their executable paths.

OpenCode and Pi also require installed plugin packages. Set `OPENCODE_PLUGIN`
and `PI_EXTENSION_PATH` to absolute paths to their installed `dist/index.mjs`
files. Use packed npm artifacts with their peers installed, as shown in the
[CI workflow](../../../.github/workflows/ci.yml).

```bash
cargo test --manifest-path bt-daemon/Cargo.toml --all-features --locked \
  --test agent_integration -- --ignored --nocapture --test-threads=1
```

These tests are ignored by a normal `cargo test` because they require agent
executables. To run one agent, add a test-name filter before `--` (for example,
`codex`). The ordinary Rust suite covers translators and daemon behavior
without installing agents.

## Test layers

- `server` binds an Axum router to an ephemeral address and manages its lifetime.
- `inference` implements mock OpenAI Responses and Anthropic Messages endpoints.
  Scenarios control responses and record incoming requests.
- `ingest` implements a mock Braintrust API and records span rows. Scenarios
  match ordered row shapes independently of HTTP batching.
- `agent_process` starts the daemon and ingest server and supplies the shared
  process environment.
- `agents` configures and runs each CLI in an isolated workspace.

An integration test starts an inference mock and daemon, runs an agent, and
checks the resulting trace. Agent arguments and environment variables can be
customized per scenario.

## Mock and live backends

Inference and ingest are configured independently:

| Variable | Values | Default |
|---|---|---|
| `BT_AGENT_INFERENCE_MODE` | `mock`, `live` | `mock` |
| `BT_AGENT_INGEST_MODE` | `mock`, `live` | `mock` |

Mock ingest uses the standalone daemon with test credentials. Live ingest uses
the daemon embedded in `bt`, with these settings:

| Variable | Purpose | Default |
|---|---|---|
| `BT_AGENT_BT_BIN` | `bt` executable | `bt` on `PATH` |
| `BT_AGENT_PROFILE` | Saved profile | `BRAINTRUST_PROFILE` |
| `BT_AGENT_ORG` | Organization | `BRAINTRUST_ORG_NAME` |
| `BT_AGENT_PROJECT` | Destination project | `agent-e2e` |

The harness writes profile and destination selections to the route; `bt`
resolves credentials and refreshes OAuth tokens.

All modes check process success and trace delivery. Mock ingest also allows
assertions on captured rows. `IngestScenario` adds exact output, failure, and
ordering checks when both backends are mocked. See the
[inference guide](inference/README.md) for examples of live-backend runs.
