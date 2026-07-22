---
name: audit-agent-tracing-support
description: Assess whether a coding agent's hooks/plugin API can support tracing (per docs/trace-coding-agents-with-hooks.md), or whether a reliable fallback like session transcripts exists. Use when evaluating a new or existing coding agent (Claude Code, Codex, Cursor, Gemini CLI, Aider, etc.) for a Braintrust tracing plugin, or when someone asks "can we trace agent X" / "is agent X's hook API good enough".
---

# Audit an agent's tracing support

Determine whether a specific coding agent can be traced through its hooks/plugin
API, and if not, whether a reliable fallback (e.g. session transcripts) exists.

The requirements below are a condensed, checkable form of
`docs/trace-coding-agents-with-hooks.md` — that doc is the source of truth; read
it if anything here is ambiguous.

## Inputs you need

Before auditing, gather:

- **Agent + version.** APIs change; pin what you looked at.
- **Where the hook/plugin API is documented.** Official docs URL, and/or the
  source that emits hook events. Prefer primary sources over blog posts.
- **A sample event payload** if you can get one (run the agent with a logging
  hook, or find an example in docs). Payloads reveal what's actually sent, not
  just what's promised.
- **Transcript format**, if the agent writes session transcript files — path,
  format (JSONL/JSON/markdown), and stability guarantees.

Do not guess. If you can't find evidence for a requirement, mark it **Unknown**
and say what you'd need to confirm it — never assume an API emits something.

## Requirements checklist

For each item, decide: **Yes** (emitted directly), **No** (not available), or
**Unknown** (couldn't confirm). Cite where you found the evidence.

1. **ms-precise timestamp** on every event (e.g. epoch millis).
2. **Unique IDs** for session start, each turn, and each operation that has a
   start and a stop — with the operation's ID **shared between its start and
   stop** so the pair can be matched. (A "turn" = one user message plus the
   agent's response to it.) Without shared operation IDs you can't pair or nest
   events, so an agent can emit all the right event *types* and still be
   untraceable — check this explicitly.
3. **Turn attribution** — every operation event (LLM call, tool use, web, MCP,
   subagent) carries the ID of the turn that spawned it.
4. **Paired start/stop events** for anything with a logical beginning and end
   (single events are fine for point-in-time things like session start).
5. **LLM API call start/stop** — with the **full request and response bodies**.
   This is the most commonly missing one; check carefully.
6. **Tool / web / MCP call start/stop** for every such call the agent makes.
7. **Turn start/stop.**
8. **Subagents (and sub-subagents) emit the same hook events** recursively,
   carrying the spawning turn ID (per #3) so the trace tree can be rebuilt.
9. **Session end / shutdown hook** (used to flush background data; need not be
   the definitive end since sessions can resume).
10. **Ordered, blocking delivery** — events arrive in order, and hooks support a
    blocking mode (async/non-blocking is fine as an *additional* mode).

## Fallback assessment

If the hook API misses one or more requirements, check the transcript fallback:

- Does the agent write a **session transcript file** that contains the missing
  data (especially full LLM request/response bodies and timing)?
- Is the format **reliable** — documented, versioned, stable across releases? An
  undocumented format that drifts between versions is a weak fallback and should
  be called out as a risk, not a solution.
- Can start/stop timing be **reconstructed** from the transcript, or only
  after-the-fact ordering? Note the loss of real timestamps if so.

## Verdict

Classify the agent as one of:

- **Sufficient** — hooks alone cover all (or all critical) requirements.
- **Fallback-viable** — hooks miss some data, but a reliable transcript makes
  tracing feasible. State exactly which data comes from the fallback and any
  fidelity loss (e.g. approximate timing).
- **Insufficient** — required data (esp. full LLM request/response bodies) is
  neither in hooks nor in a reliable fallback. List the specific gaps that block
  tracing and what the agent would need to add.

## Output format

Produce a short report:

```
# Tracing audit: <agent> <version>
Sources: <links / files inspected>

## Requirements matrix
| # | Requirement                        | Status  | Evidence / notes |
|---|------------------------------------|---------|------------------|
| 1 | ms-precise timestamps              | Yes/No/Unknown | ... |
| ... (all 10) ...                                                    |

## Fallback
<transcript availability, format, reliability>

## Verdict: <Sufficient | Fallback-viable | Insufficient>
<1-3 sentences justifying, and the concrete gaps if any>
```

Keep it evidence-first: every Yes/No must point at a doc section, payload field,
or source line. If the answer changes with agent version, say so.
