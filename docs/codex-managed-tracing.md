# Codex hook activation and enterprise deployment

`bt trace enable codex` and `bt trace update codex` install the plugin and trust
its current Braintrust hook definitions automatically. Restart Codex after setup
so existing sessions load the plugin and trust settings.

## Scoped approval through Codex

Codex CLI 0.163.0 exposes the same configuration API used by its `/hooks` UI:
`hooks/list` supplies each definition's key and current hash, and
`config/batchWrite` upserts `hooks.state` entries containing `trusted_hash`.
Braintrust uses that API, reloads configuration, and checks that the hooks are
trusted. It does not calculate Codex hashes or write private trust files.

Only hooks belonging to `trace-codex@braintrust-codex-plugins` are eligible. Before
writing trust, Braintrust verifies the complete installed hook file against the
definitions bundled with this version of `bt`, including Windows commands, and
checks that Codex reports the expected commands and lifecycle events. Unexpected
definitions fail activation; update `bt` and retry. Unrelated hook trust and
existing configuration are preserved. Already trusted definitions need no write.
Update preserves disabled hooks rather than enabling them.

An unsupported API, failed write, missing definition, or configuration error makes
setup report an activation failure instead of claiming success. Older Codex
versions can still use manual approval through `/hooks`. After a marketplace
update changes definitions, run `bt trace update codex` to trust the current
Braintrust definitions. For `bt trace run codex`, the same packaged definitions and exact Codex keys and
hashes are passed as invocation-only command-line configuration. Run does not
write persistent trust or change the user's global plugin settings. The
invocation-wide `--dangerously-bypass-hook-trust` flag is not used and remains
rejected by `bt trace run codex`.

`bt trace doctor codex` remains read-only. It probes the installed Codex app
server for the current directory and distinguishes trusted, review-required,
disabled, missing, erroneous, and unknown configurations. Trust readiness does
not prove capture or delivery. See the
[official OpenAI hook documentation](https://learn.chatgpt.com/docs/hooks#review-and-trust-hooks)
for Codex's trust model.

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

For plugin installs performed outside `bt`, approve hooks through `/hooks` or
run `bt trace update codex` after definition changes.
Events skipped before approval are not in the daemon's journal; replay cannot
recover those events. Historical transcript import may recover supported data,
with the limitations documented for that import path.
