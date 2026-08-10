---
name: test-coding-agent-integration
description: Test or audit a coding-agent tracing integration across translator fixtures, capture adapters, setup, managed run, transcript import, daemon recovery, real-agent execution, package installation, and CI. Use when adding verification, reviewing integration completeness, diagnosing coverage gaps, or proving that emitted events become correct Braintrust traces.
---

# Test a coding-agent integration

Identify the changed integration surfaces and use their specialized skills when
their intended behavior is unclear. Derive current commands and test seams from
source rather than copying another agent's test layout.

## Build layered evidence

- Add translator fixtures covering trace hierarchy, content, timing, metrics,
  errors, duplicates, missing events, flush, and deterministic replay.
- Test adapter mapping, ordering, failure-open behavior, reconnection, and flush.
- Test setup, managed run, and import or attach independently when supported.
- Exercise the full envelope-to-journal-to-translator-to-sink pipeline, including
  routing, attachment, credential redaction, restart recovery, and status errors.
- Run a real packaged agent against deterministic mock inference and mock ingest.
  Assert session, turn, LLM, tool, failure, version, provenance, and terminal
  delivery semantics rather than only process success.
- Install the adapter exactly as users will, including package and peer
  resolution. Add minimum and current compatibility coverage when applicable.
- Cover every claimed operating system and architecture, or document the
  deliberate product limitation.

## Validate and report

Run the repositories' canonical formatting, linting, unit, integration, locked
dependency, package, generated-artifact, and diff checks. Explicitly execute
real-agent tests that the default runner skips and confirm they actually ran.
Report exact commands, versions, results, skipped tests, and remaining gaps.
