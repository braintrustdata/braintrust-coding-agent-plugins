# Braintrust tracing for Grok

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

Trace Grok coding sessions in Braintrust.

## Quickstart

Prerequisites:

- The latest [Grok CLI](https://grok.com/build)
- The latest [Braintrust CLI (`bt`)](https://www.braintrust.dev/docs/reference/cli/quickstart)

Install:

```bash
grok plugin install braintrustdata/braintrust-grok-plugin --trust
grok plugin enable trace-grok
bt login --profile myprofile
bt trace --profile myprofile -p my-coding-agent-project enable grok
```

This causes `grok` sessions to report to your configured project.

> **Note:** Due to a bug in the Grok CLI, you must run `/reload-plugins` at the
> start of each session for traces to be reported. You can use
> `alias grok="grok /reload-plugins"` until the
> [issue is resolved](https://github.com/xai-org/plugin-marketplace/issues/236).

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
- exact model input is available only for the first reconstructed LLM span when
  the system prompt and first user message can be recovered;
- LLM boundaries are reconstructed from Grok stream timing and may not match
  provider-side spans exactly;
- permission, compaction, and subagent activity do not receive dedicated span
  hierarchies, and web or MCP operations may appear as generic tools;
- managed `bt trace run grok` and historical `bt trace import grok` workflows
  are not currently supported.

These limitations affect trace detail, not Grok execution. Tracing is fail-open:
if the plugin or local daemon is unavailable, Grok continues normally.
