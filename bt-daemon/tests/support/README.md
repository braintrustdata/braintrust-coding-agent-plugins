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

This allows deterministic inference to drive real Braintrust ingest without
paying for model inference. Every test always asserts stable process behavior
and successful trace delivery. With mock ingest, `IngestScenario::expect`
declares baseline row shapes that are evaluated with both live and mock
inference. `IngestScenario::expect_strict` adds deterministic row shapes that
are evaluated in addition to the baseline when inference is mocked. With live
ingest, where captured rows are not locally inspectable, the equivalent
baseline is that the daemon reports emitted spans and no sink errors.

Provider request sequences, exact model output, and injected provider failures
are additional mock-inference assertions; they do not replace the baseline
assertions.
