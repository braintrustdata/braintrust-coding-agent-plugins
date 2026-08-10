# Agent integration test architecture

The test infrastructure has three independent layers:

- `server` is a generic container that binds any Axum `Router` to an
  ephemeral address and owns its lifecycle.
- `inference` contains OpenAI Responses and Anthropic Messages protocol logic,
  programmable scenarios, and captured inference requests. Each mock exports
  an Axum router and can be hosted or embedded by any caller.
- `ingest` contains the mock Braintrust API and captured trace rows. It also
  exports an Axum router and has no dependency on the server container. Its
  scenario builder matches named row shapes as an ordered subsequence,
  independent of HTTP batching and unrelated SDK update rows.

`agent_process` is the Braintrust-specific orchestration layer. It hosts the
ingest router, starts the daemon, and provides the environment shared by agent
processes.

`agents` contains reusable adapters for real coding-agent CLIs. Each adapter
owns only agent installation and isolated configuration state. The daemon
world is passed to each run as its execution context, avoiding any lifetime or
ownership coupling between the two layers. Adapters provide standard
invocation flags, mock-inference routing, and process output. Runs remain
configurable with additional arguments and environment variables so scenarios
can add inputs such as attachment paths without duplicating CLI setup.

OpenCode integration runs set `OPENCODE_PLUGIN` to the entrypoint from an
isolated installation of the ephemeral npm package. This exercises the same
peer-dependency resolution and published file allowlist as a registry install,
rather than loading the monorepo build tree directly.

The integration test composes those pieces: it hosts an inference router,
starts the daemon world, runs an agent, and evaluates the ingest scenario. This
keeps both protocol mocks usable without coding agents, keeps the generic
server unaware of either protocol, and lets new end-to-end scenarios focus on
model behavior and expected trace shapes.

The world controls inference and ingest independently:

- `BT_AGENT_INFERENCE_MODE=mock|live` selects deterministic mock inference or
  the agent's normal provider.
- `BT_AGENT_INGEST_MODE=mock|live` selects captured local ingest or the normal
  Braintrust backend.

Mock ingest launches the feature-gated standalone daemon with test
credentials. Live ingest instead launches the profile-aware daemon embedded in
`bt`, selected by:

- `BT_AGENT_BT_BIN` — `bt` executable to test (defaults to `bt` on `PATH`);
- `BT_AGENT_PROFILE` — optional saved OAuth or API-key profile;
- `BT_AGENT_ORG` — optional organization constraint;
- `BT_AGENT_PROJECT` — destination project name (defaults to `agent-e2e`).

Only those non-secret selections are written to the harness route. The `bt`
daemon host resolves credentials and refreshes OAuth leases internally.

This allows deterministic inference to drive real Braintrust ingest without
paying for model inference. Every test uses ordinary assertions for stable
process behavior and trace delivery regardless of mode. When ingest is mocked,
the captured rows are also available for ordinary assertions over stable
metadata. With live ingest, the daemon must report emitted spans and no sink
errors.

`IngestScenario` is exclusively for the additional deterministic expectations
when both inference and ingest are mocked. Provider request sequences, exact
model output, injected provider failures, and ordered trace shapes are layered
on top of the always-run assertions.
