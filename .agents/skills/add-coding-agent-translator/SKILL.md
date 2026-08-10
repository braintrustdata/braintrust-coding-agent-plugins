---
name: add-coding-agent-translator
description: Implement or review a coding agent's daemon translator, including source registration, native event correlation, trace shape, deterministic recovery, routing metadata, and translator tests. Use when adding a new agent translator or changing how an existing agent's hooks or transcripts become Braintrust spans.
---

# Add a coding-agent translator

Start from official agent documentation, captured native payloads, and native
transcripts. Establish one lowercase source identity for every integration
surface and record any version-dependent schema behavior.

## Implement the translator

- Add one stateful translator per daemon session and register its factory in the
  production registry and advertised capabilities.
- Build the supported hierarchy of session, turn, LLM, tool, permission,
  compaction, and recursive subagent spans. Preserve correct parents and the
  effective trace root.
- Populate native timing, inputs, outputs, model, token and cache metrics,
  errors, outcomes, tags, agent and adapter versions, session identity, cwd,
  and useful metadata without inventing unavailable facts.
- Honor resolved destinations, exported parent/root attachment, additional
  metadata, and flush behavior. Keep internal routing data out of user metadata.
- Use shared git enrichment and deterministic span identities so repeated,
  duplicate, late, and journal-replayed events converge instead of duplicating.
- Bound transcript observations to what was available when an event was
  captured. Handle unknown or drifted events without panicking.
- Flush late records and close defensibly any work left open by missing events.

## Verify completion

Add fixture-driven tests for happy paths, failures, duplicate or missing event
pairs, multiple turns, resume, supported subagents and permissions, token
accounting, attachment, cwd changes, version variants, flush, and replay.
Confirm the production registry selects this translator for the agreed source.
