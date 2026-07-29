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
owns agent installation, isolated configuration, standard invocation flags,
mock-inference routing, and process output. Runs remain configurable with
additional arguments and environment variables so scenarios can add inputs
such as attachment paths without duplicating CLI setup.

The integration test composes those pieces: it hosts an inference router,
starts the daemon world, runs an agent, and evaluates the ingest scenario. This
keeps both protocol mocks usable without coding agents, keeps the generic
server unaware of either protocol, and lets new end-to-end scenarios focus on
model behavior and expected trace shapes.
