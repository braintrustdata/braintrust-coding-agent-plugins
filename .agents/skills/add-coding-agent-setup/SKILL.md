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
- When setup talks to the agent's own API or protocol (for example Codex's
  app-server), report the underlying error and the agent's stderr, not only a
  generic setup failure.

## Verify completion

Test setup in an isolated home and configuration tree, including first install,
repeat setup, route changes, existing unrelated configuration, disablement,
missing dependencies, and a real agent launch using the installed adapter.

If setup calls the agent's API or protocol, add a credential-free contract test
that runs that setup path against the real agent in an isolated home, and run it
in CI against the latest agent release. Agent releases can break setup without
any change on our side. See
`codex_hooks::tests::activation_succeeds_against_real_codex`.
