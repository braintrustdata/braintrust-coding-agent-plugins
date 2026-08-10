---
name: ship-coding-agent-integration
description: Package and release a coding-agent tracing integration through its marketplace, package registry, or distribution repository, including manifests, builds, validation, versioning, CI, documentation, publishing automation, and installed-artifact smoke tests. Use when preparing an integration for release or auditing whether it has a complete distribution path.
---

# Ship a coding-agent integration

Require verification evidence from `test-coding-agent-integration`. Do not
publish, tag, or mutate a live distribution unless the user explicitly requests
that release action.

## Prepare the distribution

- Integrate the agent with the repository's build, validation, and publishing
  system using its supported marketplace or package format.
- Validate manifests, entrypoints, generated artifacts, package allowlists,
  platform binaries, dependency compatibility, and installability.
- Ensure distributed hooks and plugins remain thin daemon forwarders without
  direct Braintrust credential or ingest logic.
- Add versioning, tags, release notes, provenance, and release automation suited
  to the distribution channel, including a safe dry-run path.
- Document prerequisites, setup, run, import or attach, disablement,
  troubleshooting, fidelity limitations, privacy behavior, and release steps.
- Configure only the narrowly required publishing and optional smoke-test
  secrets.

## Prove release readiness

Install the built artifact through the user-facing distribution path. Verify
setup, one live traced session, managed run, import or attach when supported,
status or permalink output, and disablement without damaging unrelated agent
configuration. Add a post-deploy smoke test where the channel permits it.
