# trace-cursor

Capture Cursor's native agent hooks through `bt trace hook --source cursor`.
The shared daemon owns journaling, translation, transcript mirroring, and
delivery. The plugin contains no tracing runtime or credentials.

This is the local development integration for issue #120's first PR. Persistent
setup, managed runs, historical import, marketplace installation, and release
automation are follow-up work.

## Build and load locally

From the monorepo root:

```bash
make build-cursor
make validate-cursor
agent --plugin-dir "$PWD/dist/cursor"
```

The Cursor CLI exposes `--plugin-dir`. Desktop Cursor discovers local plugins
after a reload from `~/.cursor/plugins/local`:

```bash
mkdir -p "$HOME/.cursor/plugins/local/trace-cursor"
cp -R dist/cursor/. "$HOME/.cursor/plugins/local/trace-cursor/"
```

Run **Developer: Reload Window** and inspect the plugin in **Customize**. Local
imports must be allowed by your organization. Copy the directory: current
Cursor skips symlinks whose targets are outside its local plugin directory.
Repeat the copy after rebuilding. Remove that development copy to disable it.
See [Cursor's local plugin documentation](https://cursor.com/docs/plugins#test-plugins-locally).

### Current CLI lifecycle limitation

The audited CLI `2026.10.01-14929f9` checks only project/user hooks before firing
interactive prompt, response, and stop hooks. A plugin-only CLI session
can therefore omit these events. In an isolated development workspace, add the
following `.cursor/hooks.json` to make those event checks discoverable;
these no-op hooks leave the tracing itself to the plugin:

```json
{
  "version": 1,
  "hooks": {
    "beforeSubmitPrompt": [{ "command": "printf '%s\\n' '{\"continue\":true}'" }],
    "afterAgentResponse": [{ "command": "printf '%s\\n' '{}'" }],
    "stop": [{ "command": "printf '%s\\n' '{}'" }]
  }
}
```

Merge these entries if a workspace already has hooks. Remove the no-op entries
when the CLI fixes its plugin discovery checks. `agent --print` uses another
execution path that omits these local lifecycle events even with project hooks;
use interactive CLI sessions for full turn evidence. Desktop `3.4.16` considers
plugin hooks in the corresponding discovery checks, based on implementation
inspection. This does not establish desktop execution parity.

The default plugin omits `afterAgentThought`. Real CLI captures reproduced
`WritableIterable is closed` failures when that native hook was registered;
the same tool scenario completed successfully without it. The translator can
consume thought events, but enabling this hook on desktop is unverified and
requires an explicit hook registration in a separate local development copy.

The hooks need a `bt` build containing the Cursor translator on Cursor's `PATH`,
and a configured non-secret tracing route. Until persistent Cursor setup is
implemented, `BT_DAEMON_CONFIG` can select an isolated route file using the schema
in the monorepo's `bt-daemon/config.json.example`. Authentication and backend
selection remain owned by `bt`.

## Inspect traces without a production bt build

The standalone daemon provides an offline development sink. From the monorepo
root, build it and create private development storage:

```bash
cargo build --manifest-path bt-daemon/Cargo.toml --features cli
export CURSOR_TRACE_DEV_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cursor-trace.XXXXXX")"
mkdir -p "$CURSOR_TRACE_DEV_DIR/bin"
cp src/plugins/cursor/test/bt-standalone-wrapper.sh "$CURSOR_TRACE_DEV_DIR/bin/bt"
cat > "$CURSOR_TRACE_DEV_DIR/braintrust.json" <<'JSON'
{
  "trace_to_braintrust": true,
  "route": {
    "destination": { "type": "project_logs", "project_name": "cursor-local-dev" },
    "flush_mode": "flush_on_turn_end"
  }
}
JSON
bt-daemon/target/debug/bt-daemon serve --debug-sink \
  --socket "$CURSOR_TRACE_DEV_DIR/daemon.sock" \
  --data-dir "$CURSOR_TRACE_DEV_DIR/state" --idle-timeout-secs 0
```

Leave the daemon running in that terminal. In another terminal, set
`CURSOR_TRACE_DEV_DIR` to the directory printed by `echo "$CURSOR_TRACE_DEV_DIR"`
in the first terminal, then run:

```bash
export BT_DAEMON_BIN="$PWD/bt-daemon/target/debug/bt-daemon"
export BT_DAEMON_SOCKET="$CURSOR_TRACE_DEV_DIR/daemon.sock"
export BT_DAEMON_CONFIG="$CURSOR_TRACE_DEV_DIR/braintrust.json"
BT_BIN="$CURSOR_TRACE_DEV_DIR/bin/bt" \
  agent --plugin-dir "$PWD/dist/cursor"
```

Inspect `state/spans/*.ndjson` for translated spans and `state/journal` for the
original events. Stop the foreground daemon with Ctrl-C. These artifacts may
contain your prompts and tool data; keep the temporary directory private.

`BT_BIN` selects a specific CLI executable for development. Cursor may launch
hooks through a login shell that changes `PATH`, so use the absolute wrapper
path above.

## Capture behavior and available evidence

The plugin registers session, prompt, generic tool, subagent, compaction,
response and stop hooks. Specialized shell/MCP/file hooks capture
additional native data. Tab completions are outside agent tracing.

Each hook forwards the original JSON synchronously and suppresses `bt` output.
Permission hooks return valid allow responses even if `bt` is missing or fails;
prompt submission returns `continue: true`. The hooks never submit follow-up
messages or modify tool input. Cursor applies other installed policy hooks
normally. A ten-second native hook timeout bounds a stalled capture process.
Terminal events request daemon flushes; acknowledgement means durable capture,
while translation and delivery happen afterward.

Native hooks and transcripts are complementary. A transcript path can be null
or arrive late. Transcripts enrich observed messages without supplying absent
tool results, call IDs, usage, system prompts, or record timestamps. Reconstructed
LLM inputs and estimated boundaries are marked in trace metadata. Compaction
and subagent data remain limited to the evidence Cursor emits.

The tracing plugin registers no MCP server. The existing Braintrust MCP plugin
or editor extension can remain installed as an optional companion; choose one
MCP installation method to avoid duplicate MCP registration.

See [Cursor hook schemas](https://cursor.com/docs/hooks) and the monorepo's Cursor
evidence audit for the tested surfaces and current capability limitations.

Span metadata includes the selected model and parameters when Cursor exposes
those fields. Auto mode on the tested CLI reports `model: default`, without the
resolved model or provider. Native hook IDs and capture bookkeeping stay in the
durable journal. Turn token totals are marked `token_usage_scope: turn`; model
steps retain reconstruction and estimated-timing flags, and truncation flags
appear only when content was truncated.

The trace format follows the [Braintrust instrumentation specification](https://github.com/braintrustdata/braintrust-spec/blob/main/skills/instrumentation-spec/SKILL.md)
where native hooks provide the required data. Model outputs use OpenAI-style
choice arrays and tool calls; tool-result messages use matching call IDs.
Turn metrics include `tokens` when both prompt and completion counts are known.
Per-call usage and provider billing identity remain unavailable, so turn totals
are kept on their parent task spans and provider names are omitted.
