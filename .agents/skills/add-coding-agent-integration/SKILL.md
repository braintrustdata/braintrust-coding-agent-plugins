---
name: add-coding-agent-integration
description: Orchestrate a complete Braintrust coding-agent integration by delegating feasibility, daemon translation, event capture, setup, managed run, transcript import, verification, and shipping to the specialized repo-local skills. Use for end-to-end support for a new agent or a completeness audit of an existing integration; use the component skills directly for focused work.
---

# Orchestrate a coding-agent integration

Coordinate the integration; do not implement component details from this skill.
Inspect current source, keep a requirement-to-evidence inventory, and invoke the
specialized skill for every applicable workstream.

## Delegate the work

1. Use `audit-agent-tracing-support` to establish whether hooks, an in-process
   plugin API, transcripts, or a combination can provide sufficient data.
2. Use `add-coding-agent-translator` to establish the canonical source identity
   and production daemon translator first.
3. Use `add-coding-agent-capture` to forward live native events through command
   hooks or an in-process plugin.
4. Once the translator exists, delegate independently:
   - persistent installation and configuration to `add-coding-agent-setup`;
   - invocation-local launching to `add-coding-agent-run`;
   - historical import and live transcript following to
     `add-coding-agent-import`, when a viable transcript exists.
5. Use `test-coding-agent-integration` to verify every implemented surface and
   the combined end-to-end path.
6. Use `ship-coding-agent-integration` only after verification evidence exists.

Add every applicable skill to the working plan and follow that skill when its
workstream begins. Do not copy its instructions into the plan or preload skills
for workstreams that are out of scope.

## Maintain the shared contract

Keep one stable source identity across the translator, capture adapter, setup,
run, import, status, tests, and documentation. Keep plugins and hooks thin,
fail-open, credential-free forwarders; the daemon owns correlation, trace
construction, recovery, routing, and delivery.

Treat a missing capability as `blocked` or explicitly `not applicable` with
evidence, never as silently complete. Do not call the integration complete
until each applicable component skill has produced implementation and test
evidence and the shipping skill has confirmed a releasable distribution path.

## Hand off

Report a compact matrix with one row per delegated skill: status, source
evidence, test evidence, blockers or fidelity loss, and remaining release work.
