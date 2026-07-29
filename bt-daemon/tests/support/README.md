# Agent integration test architecture

The test infrastructure has three independent layers:

- `server` is a generic container that binds any Axum `Router` to an
  ephemeral address and owns its lifecycle.
- `inference` contains OpenAI Responses and Anthropic Messages protocol logic,
  programmable scenarios, and captured inference requests. Each mock exports
  an Axum router and can be hosted or embedded by any caller.
- `ingest` contains the mock Braintrust API and captured trace rows. It also
  exports an Axum router and has no dependency on the server container.

`agent_process` is the Braintrust-specific orchestration layer. It hosts the
ingest router, starts the daemon, and configures coding-agent processes. The
integration test separately hosts an inference router when running in
deterministic mode. This keeps both protocol mocks usable without the coding
agent runner and keeps the generic server unaware of either protocol.
