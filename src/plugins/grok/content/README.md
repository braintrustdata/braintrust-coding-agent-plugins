# Braintrust tracing for Grok

Trace Grok coding sessions in Braintrust.

## Quickstart

### Prerequisites

- Grok CLI 1.0.13
- A recent `bt` CLI with `bt trace enable grok`
- Braintrust authentication configured in `bt`
- macOS or Linux with Bash

### 1. Install the plugin

The published plugin lives at
[`braintrustdata/braintrust-grok-plugin`](https://github.com/braintrustdata/braintrust-grok-plugin).

```bash
grok plugin install braintrustdata/braintrust-grok-plugin --trust
grok plugin enable trace-grok
```

`--trust` allows Grok to run the plugin's tracing hooks. Review the repository
before installing if required by your security policy.

### 2. Enable Braintrust tracing

```bash
bt trace enable grok
```

This saves a non-secret Braintrust destination, updates the published plugin
when necessary, and enables it. The command is safe to repeat.

If Grok was already open during installation or an update, activate the new
hooks in that session:

```text
/reload-plugins
```

New Grok sessions load the plugin automatically. Start Grok normally and use it
as usual; completed turns will appear in your configured Braintrust project.

## What is captured

Each traced session includes:

- a root span for the Grok session;
- a turn span for each user request and visible assistant response;
- reconstructed LLM spans with available model output and reasoning;
- tool spans with observable inputs, outputs, duration, outcome, and errors;
- per-turn token, cache, reasoning, model-call, API-duration, and raw cost
  metrics when Grok records them;
- the system prompt and first user message on the first LLM span when available;
- useful session metadata such as Grok and plugin versions, working directory,
  workspace, and native session ID.

The plugin forwards events only to the local Braintrust daemon. It does not
contain Braintrust credentials or send traces directly to Braintrust.

## Caveats

Grok's hooks and transcripts do not expose the complete provider request or
native boundaries for every model call. As a result:

- token and usage totals are accurate at the **turn** level, but are not
  available for each individual LLM call; the turn aggregate is also attached
  to the final reconstructed LLM span and labeled as turn-level usage;
- exact model input is available only for the first reconstructed LLM when the
  system prompt and first user message can be recovered;
- LLM boundaries are reconstructed from Grok stream timing and may not match
  provider-side spans exactly;
- permission, compaction, and subagent activity do not receive dedicated span
  hierarchies, and web or MCP operations may appear as generic tools;
- managed `bt trace run grok` and historical `bt trace import grok` workflows
  are not currently supported.

These limitations affect trace detail, not Grok execution. Tracing is fail-open:
if the plugin or local daemon is unavailable, Grok continues normally.
