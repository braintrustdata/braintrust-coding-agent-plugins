# Braintrust tracing for Grok (v1 beta)

This plugin traces interactive Grok sessions to Braintrust. It is useful for
observing turns, model output, tool calls, timing, and native aggregate usage,
but it is a beta integration: it is neither full-fidelity nor production-ready.

## Compatibility

The supported evidence baseline is **Grok 1.0.13**. Hook registration follows
Grok's documented lifecycle names, and the repository's fixtures cover that
version's `updates.jsonl`, `events.jsonl`, and `system_prompt.txt` shape. Later
Grok versions may work, but are not yet compatibility-covered.

The plugin supports **macOS and Linux** and requires **Bash**. Windows is not
supported. Install `grok` and a `bt` CLI version that includes
`bt trace enable grok`, and configure Braintrust authentication in `bt` before
enabling tracing.

## Setup

Install the published plugin, enable it in Grok, and save a non-secret
Braintrust route with:

```bash
bt trace enable grok
```

The command is safe to repeat. It installs
`braintrustdata/braintrust-grok-plugin` with `--trust`, updates that published
plugin when already installed, enables it, and writes only the
Braintrust-owned tracing settings. A same-named local or forked `trace-grok`
plugin is replaced with the published package; every other Grok plugin and
configuration entry is preserved.

Grok 1.0.13 discovers a newly installed or updated plugin but does not activate
its hooks in a session that is already open. In each active session, run this
before sending the next prompt:

```text
/reload-plugins
```

Wait for Grok to report that the hooks were reloaded. New Grok sessions load the
installed plugin normally.

### Manual plugin installation

To inspect or install the published package yourself, run:

```bash
grok plugin install braintrustdata/braintrust-grok-plugin --trust
grok plugin enable trace-grok
```

Manual Grok installation alone does not create a Braintrust destination. Run
`bt trace enable grok` afterward to reconcile the installed plugin and write
its route, then use `/reload-plugins` in any session that was already open.

## Diagnose or disable

Inspect the installed plugin, tracing route, authentication selection, and
known compatibility warnings without changing configuration:

```bash
bt trace doctor grok
```

For Grok 1.0.13, doctor also reminds you when `/reload-plugins` is required.

To uninstall the published `trace-grok` package and remove its Braintrust-owned
route while preserving unrelated Grok configuration:

```bash
bt trace disable grok
```

Disable removes only a `trace-grok` whose recorded source matches
`braintrustdata/braintrust-grok-plugin`; it leaves a same-named local or forked
plugin untouched. Disabling does not delete traces already sent to Braintrust
or the daemon's existing local recovery records.

## Trace shape and fidelity

Hooks synchronously and fail-open forward native lifecycle payloads to the
local Braintrust daemon. The adapter contains no Braintrust credential and does
not send data directly to Braintrust. Hooks are control-plane wake-up and flush
signals, plus terminal fallback when the transcript lacks a terminal record.
`updates.jsonl` is the trace data source of truth. `events.jsonl` is optional,
independently tailed enrichment for completed tool duration and outcome; a
missing, unreadable, malformed, or lagging events stream never delays updates,
terminal handling, or flush. A later hook can merge recovered enrichment onto
the same deterministic tool span identity. The daemon also snapshots the
optional sibling `system_prompt.txt` and attaches it to the first reconstructed
LLM when available; prompt capture failure does not block trace construction.

A traced session contains a `Grok` root span, turn task spans, reconstructed LLM
spans, and child tool spans. A turn span is the user-facing exchange: its input
is the user message and its output contains only observable assistant responses,
never reasoning. Tool spans retain transcript-observable input, output, timing,
and failure details. Turn spans also retain Grok's native aggregate tokens,
cache, reasoning, raw cost ticks, model-call count, and API duration when
present.

Grok's transcript does not expose a complete provider request or a native LLM
span boundary. The translator reconstructs each observed model stream from
`streamStartMs` evidence. The first reconstructed LLM receives the available
conversation start as `system` and `user` messages: the native
`system_prompt.txt` snapshot when present, followed by the first turn's user
message. It is marked with `system_prompt_included`, `user_message_included`,
and an `input_scope` such as `"system_and_user"`. Each LLM output is one
standard assistant message so Braintrust renders it as an LLM response. Its
string `content` is the observed assistant response; an optional `reasoning`
field contains Grok thought chunks when emitted. Later calls do not claim an
exact reconstructed input
because Grok does not expose their serialized provider history. LLM spans
retain `input_unavailable: true` to make that limitation explicit, plus
`boundary_source: "streamStartMs"` and
`trace_source: "session_transcript"`. The visible LLM span remains named
`{model} call {sequence}` and has the Braintrust LLM span type.

Usage is aggregate turn evidence, not per-call evidence. It remains on the turn
and is never divided among reconstructed calls. To expose the aggregate in LLM
views without double-attributing it across calls, the translator also copies it
to only the final reconstructed LLM span. That merge is labeled
`usage_scope: "turn"` and `usage_attribution: "last_llm"`; on a multi-call turn
its metrics describe the whole turn, not the final call alone. Native
`costUsdTicks` is reported as `cost_usd_ticks`; the plugin does not calculate
`estimated_cost`.

The v1 beta does **not** provide:

- `bt trace run grok` managed-run support;
- `bt trace import grok` or historical transcript import/attach support;
- dedicated subagent hierarchy or faithful subagent lifecycle spans;
- permission request/decision spans;
- compaction spans or compaction-aware transcript fidelity;
- faithful web-search, web-fetch, or MCP transport/server/method
  classification (observable operations may appear as generic tool spans);
- complete provider requests or per-call usage for multi-call turns.

Hook events for unsupported lifecycle areas may still be journaled for ordering
and recovery; their presence does not imply dedicated translated spans.

## Sensitive data, redaction, and retention

Treat Grok transcripts, daemon journals, transcript mirrors, and Braintrust
traces as sensitive. Depending on the session, captured content can include
system instructions, user prompts, assistant messages, reasoning summaries,
repository and file paths, tool inputs and outputs, command output, errors, and
source-control metadata. Secrets included in any of that content can be
captured too.

Braintrust credentials are not stored in the plugin, hook command, route,
envelope, or journal. The daemon keeps local files private and redacts routing
credentials, but the beta does **not** comprehensively redact prompt,
response, reasoning, or tool content. It also provides no plugin-level custom
redaction rules.

Local recovery journals and transcript mirrors are append-only while active and
become eligible for best-effort age collection after seven days; their contents
are not size-capped or truncated before collection. Do not treat that cleanup
as immediate or secure deletion. Data already delivered to Braintrust follows
the retention and deletion policy of the selected Braintrust
organization/project. Disabling the plugin is not a data-deletion operation.
Avoid placing secrets in agent input, restrict access to the host account, and
apply appropriate Braintrust retention and deletion controls.

## Troubleshooting

### No trace appears

1. Run `bt trace doctor grok` and resolve any reported installation, route, or
   authentication problem.
2. Run `grok plugin list --json` and confirm that `trace-grok` is installed and
   enabled.
3. If setup or update happened while Grok was open, run `/reload-plugins` in
   that session and wait for the reload confirmation before another prompt.
4. Re-run `bt trace enable grok` if `bt` was replaced, the route was removed, or
   the plugin was disabled.

Tracing is fail-open by design: a missing `bt`, unavailable daemon, forwarding
timeout, or translation failure must not interrupt Grok, so a normal Grok turn
does not by itself prove that tracing succeeded.

### A trace is partial

The daemon can translate only transcript records visible at a lifecycle
boundary. Keep the native transcript readable for the duration of the session,
let the turn finish, and exit Grok normally so the native `session_end` hook can
flush terminal state. Missing full LLM input, per-call metrics on multi-call
turns, dedicated permission/subagent/compaction spans, and web/MCP
classification are known fidelity limits rather than setup failures.

### Hooks do not activate after an update

This is a known Grok 1.0.13 activation behavior. Run `/reload-plugins` in every
already-open session, or start a new Grok session.
