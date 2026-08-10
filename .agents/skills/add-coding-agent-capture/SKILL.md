---
name: add-coding-agent-capture
description: Add or review live coding-agent event capture through blocking command hooks or an in-process plugin such as a JavaScript adapter. Use when forwarding native lifecycle, model, tool, permission, or subagent events into the Braintrust daemon, or when fixing ordering, failure, or flush behavior in an adapter.
---

# Add coding-agent event capture

Confirm the canonical source identity and production translator exist. If not,
use `add-coding-agent-translator` first or include it earlier in the same plan.

## Choose the capture path

- Prefer blocking command hooks when the agent offers ordered lifecycle hooks.
- Prefer the shared long-lived daemon client for an in-process plugin API.
- Combine hooks with transcript paths only when hooks omit data required by the
  translator; keep trace construction out of the adapter.

## Implement a thin adapter

- Forward raw native payloads with source, session, event, capture timestamp,
  agent version, and adapter version. Add cwd or worktree only when supplied
  separately by the agent API.
- Preserve per-session ordering. Await daemon acknowledgement for blocking
  hooks; serialize requests on a long-lived connection for in-process plugins.
- Flush on terminal lifecycle events and close persistent clients cleanly.
- Fail open with bounded diagnostics when the daemon, host CLI, or network is
  unavailable. Never break an agent turn because tracing failed.
- Forward only non-secret route selections. Never handle Braintrust credentials
  or send spans directly to Braintrust.
- Capture every event required by the translator and quote commands safely on
  every supported platform.

## Verify completion

Test hook or plugin registration, payload mapping, ordering, reconnection,
failure behavior, terminal flush, configuration precedence, disablement, and
the exact source identity accepted by the production translator.
