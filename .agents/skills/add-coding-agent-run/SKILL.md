---
name: add-coding-agent-run
description: Add or review invocation-local managed-run support for a coding agent, including executable dispatch, temporary hook or plugin injection, route isolation, duplicate-capture suppression, process behavior, trust safety, and run-command tests. Use when implementing or fixing the command that launches an agent with tracing for one invocation.
---

# Add a managed coding-agent run

Require a production translator and a capture mechanism that can be injected for
one process invocation. Use the translator or capture skill first when missing.

## Implement managed run

- Add the agent to public run parsing, aliases, executable dispatch, and the
  canonical daemon source mapping.
- Require a resolved destination before launch and scope route overrides to the
  child process tree without rewriting persistent setup.
- Inject the full capture event set using agent-specific arguments, settings,
  environment, or plugin configuration.
- Suppress inherited Braintrust capture only for the managed process tree while
  allowing the injected adapter to run, preventing duplicate traces.
- Preserve normal hook or plugin trust review. Never enable a global trust
  bypass or authorize unrelated hooks.
- Inherit stdio, forward agent arguments verbatim, return its exit status, and
  terminate and reap it on interruption.
- Quote generated commands safely across supported platforms and unusual paths.

## Verify completion

Test parsing, aliases, generated configuration, every injected event, route
isolation, duplicate suppression, paths with spaces and non-ASCII text, missing
executables, failure status, interruption, and the absence of transcript polling
when live hooks or plugins are the selected capture path.
