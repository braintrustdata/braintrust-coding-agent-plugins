# Codex hook activation and enterprise deployment

Installing `trace-codex` does not approve its hooks. Codex skips untrusted or
modified plugin hooks before Braintrust receives their events. Restart Codex,
open `/hooks`, review the Braintrust definitions, and run `bt trace doctor codex`.
The doctor probes the installed Codex app server for the current working directory.
It distinguishes trusted, review-required, disabled, missing, erroneous, and
unknown configurations. Trust readiness does not prove capture or delivery.

## Supported approval mechanisms

[Official OpenAI hook documentation](https://learn.chatgpt.com/docs/hooks#review-and-trust-hooks)
describes user review through `/hooks` and administrator-managed hooks trusted by
policy. Codex CLI 0.163.0 exposes `hooks/list` with each definition's enabled and
trust state, but its generated app-server protocol has no hook approval method.
Braintrust therefore does not modify Codex's private trust files or approve
unrelated hooks. A release that changes hook definitions can require review again;
route changes and `bt` updates do not change the marketplace definitions.

The invocation-wide `--dangerously-bypass-hook-trust` flag is not scoped to
Braintrust. `bt trace run codex` continues to reject it. For unattended enterprise
activation, use Codex's supported managed policy rather than that flag.

## Administrator-managed local Codex hooks

This recipe targets local Codex execution. Administrators own policy deployment;
`bt trace enable` does not write managed policy. Check the installed Codex version
against [OpenAI's managed hook documentation](https://learn.chatgpt.com/docs/hooks#managed-hooks-from-requirementstoml).

1. Distribute a current `bt` executable to the managed directory on each platform.
2. Have each user authenticate and configure their route with
   `bt trace enable codex --project PROJECT`. Credentials stay in the CLI's
   authentication store; the route stays in the user's `braintrust.json`.
3. Remove the user plugin with
   `codex plugin remove trace-codex@braintrust-codex-plugins` after configuring
   the route. This avoids duplicate plugin and managed capture; this Codex
   command leaves Braintrust's route settings intact.
4. Merge the following into the organization's Codex `requirements.toml` using
   its normal device-management mechanism. Preserve unrelated requirements.
   Replace the executable directories with administrator-controlled locations.

```toml
[features]
hooks = true

[hooks]
managed_dir = "/opt/company/braintrust"
windows_managed_dir = 'C:\Braintrust'

[[hooks.SessionStart]]
[[hooks.SessionStart.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.UserPromptSubmit]]
[[hooks.UserPromptSubmit.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.PreToolUse]]
[[hooks.PreToolUse.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.PermissionRequest]]
[[hooks.PermissionRequest.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.PostToolUse]]
[[hooks.PostToolUse.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.PreCompact]]
[[hooks.PreCompact.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.PostCompact]]
[[hooks.PostCompact.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.SubagentStart]]
[[hooks.SubagentStart.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.SubagentStop]]
[[hooks.SubagentStop.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.Stop]]
[[hooks.Stop.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'

[[hooks.SessionEnd]]
[[hooks.SessionEnd.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'
timeout = 3

[[hooks.Interrupt]]
[[hooks.Interrupt.hooks]]
type = "command"
command = "/opt/company/braintrust/bt trace hook --source codex"
command_windows = 'C:\Braintrust\bt.exe trace hook --source codex'
timeout = 3
```

Keep all lifecycle events so sessions, turns, tools, and subagents retain their
normal trace structure. Codex does not distribute the executable itself. The
managed policy must include `Interrupt` to observe cancellation records even
when the session stays idle, and `SessionEnd` to observe session termination.
Doctor checks these managed-only requirements separately from the packaged
plugin's event set. It reports a configuration error if plugin and managed
Braintrust capture are both enabled, including after a later `bt trace enable
codex` reinstalls the plugin. Keep only one capture source active.

The policy above enables hooks without disabling other hook sources; an organization
may separately choose `allow_managed_hooks_only` if that is its intended policy.

After deployment, restart Codex and inspect `/hooks`: Braintrust definitions
should be managed and enabled. Run `bt trace doctor codex` from the actual project
directory, then run a short session and verify its matching trace in Braintrust.
A running daemon, trusted definitions, or historical traces alone do not confirm
that the newly deployed path delivered the session.

For ordinary plugin installs, hooks still need review after definition changes.
Events skipped before approval are not in the daemon's journal; replay cannot
recover those events. Historical transcript import may recover supported data,
with the limitations documented for that import path.
