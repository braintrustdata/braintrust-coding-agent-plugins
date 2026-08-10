---
name: add-coding-agent-import
description: Add or review historical transcript import and live attach support for a coding agent, including session lookup, native parsing, synthetic lifecycle envelopes, shared translator use, incremental tailing, destination overrides, and import tests. Use when implementing or fixing import, attach, resume-session lookup, or transcript-following behavior.
---

# Add coding-agent transcript import

Require a production translator and a viable native transcript containing the
facts to import. If either is missing, use `add-coding-agent-translator` or report
the fidelity blocker before implementing import.

## Implement import and attach

- Expose the agent, aliases, and display name through the public import command.
- Locate exactly one transcript from a validated session ID across documented
  active and archived locations. Avoid unsafe symlink traversal and fail clearly
  on zero or multiple matches.
- Parse records with contextual file and line errors, schema-version handling,
  authoritative native session identity, and native timestamps.
- Convert transcript records into synthetic envelopes for the same source and
  translator used by live capture. Never create a second trace builder or invent
  facts that exist only in hooks.
- Keep historical import and live attach on the same parser, processor,
  translator, and sink state. Vary only waiting and finalization.
- Tail incrementally without duplicates, tolerate partial final records, keep an
  active turn open, and finalize it on interruption.
- Apply typed destination or exported parent overrides before transcript lookup
  and fail fast when the host has not supplied resolved session configuration.

## Verify completion

Test active and archived discovery, invalid and ambiguous IDs, malformed and
empty files, one and many turns, incremental growth, no-growth polls, partial
writes, finalization, destination and parent attachment, resume behavior, and
deterministic replay through the production translator.
