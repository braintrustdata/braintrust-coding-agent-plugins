---
name: add-coding-agent-setup
description: Add or review persistent setup for a coding-agent tracing integration, including public CLI exposure, plugin or hook installation, non-secret route configuration, idempotent updates, disablement, and isolated setup tests. Use when implementing or fixing the setup, install, enable, disable, or uninstall experience for an agent.
---

# Add persistent coding-agent setup

Require a registered translator and an installable capture adapter. If either is
missing, use `add-coding-agent-translator` or `add-coding-agent-capture` before
considering setup complete.

## Implement setup

- Expose the agent through the public setup command and help surfaces.
- Resolve the selected profile, organization, and typed trace destination in the
  profile-aware host before writing configuration.
- Install or enable the adapter through the agent's supported mechanism.
- Persist only non-secret route settings. Keep credentials, resolved backend
  URLs, and credential leases in the host.
- Make repeated setup idempotent and atomic. Preserve unrelated plugins, hooks,
  settings, and user configuration; avoid duplicate Braintrust entries.
- Keep generated hook definitions stable when the agent uses definition-based
  trust, and preserve the agent's normal review boundary.
- Provide a reversible disable or uninstall path.

## Verify completion

Test setup in an isolated home and configuration tree, including first install,
repeat setup, route changes, existing unrelated configuration, disablement,
missing dependencies, and a real agent launch using the installed adapter.
