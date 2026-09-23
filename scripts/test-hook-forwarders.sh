#!/usr/bin/env bash
# Exercise direct hook execution with a fake bt CLI. This proves that raw stdin
# and each canonical source identity reach `bt trace hook` without a shell shim.

set -euo pipefail

DIST_DIR="${1:-dist}"
TEST_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TEST_DIR"; }
trap cleanup EXIT

cat > "$TEST_DIR/bt" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" > "$BT_CAPTURE_ARGS"
cat > "$BT_CAPTURE_STDIN"
exit "${BT_STUB_STATUS:-0}"
EOF
chmod +x "$TEST_DIR/bt"

PAYLOAD='{"session_id":"direct-test","hook_event_name":"SessionStart","message":"unchanged"}'

exercise() {
  local name="$1"
  local expected_args="$2"
  shift 2
  local args_file="$TEST_DIR/$name.args"
  local stdin_file="$TEST_DIR/$name.stdin"

  printf '%s' "$PAYLOAD" | env \
    PATH="$TEST_DIR:$PATH" \
    BT_CAPTURE_ARGS="$args_file" \
    BT_CAPTURE_STDIN="$stdin_file" \
    "$@"

  [[ "$(cat "$args_file")" == "$expected_args" ]]
  [[ "$(cat "$stdin_file")" == "$PAYLOAD" ]]
}

exercise claude 'trace hook --source claude-code' \
  bt trace hook --source claude-code
exercise codex 'trace hook --source codex' \
  bt trace hook --source codex
exercise grok 'trace hook --source grok --session-id-field sessionId --event-field hookEventName --transcript-path-field transcriptPath' \
  bt trace hook --source grok --session-id-field sessionId --event-field hookEventName --transcript-path-field transcriptPath
exercise antigravity 'trace hook --source antigravity --session-id-field conversationId --event Stop --transcript-path-field transcriptPath --flush-on-turn-end' \
  bt trace hook --source antigravity --session-id-field conversationId --event Stop --transcript-path-field transcriptPath --flush-on-turn-end

python3 - "$DIST_DIR" <<'PY'
import json
import sys
from pathlib import Path

dist = Path(sys.argv[1])

codex = json.loads((dist / "codex/plugins/trace-codex/hooks/hooks.json").read_text())["hooks"]
for groups in codex.values():
    for group in groups:
        for hook in group["hooks"]:
            assert hook["command"] == "bt trace hook --source codex"
            assert hook["commandWindows"] == "bt.exe trace hook --source codex"

grok = json.loads((dist / "grok/hooks/hooks.json").read_text())["hooks"]
for groups in grok.values():
    for group in groups:
        for hook in group["hooks"]:
            assert hook["command"] == "bt"
            assert hook["args"][:4] == ["trace", "hook", "--source", "grok"]

antigravity = json.loads((dist / "antigravity/hooks.json").read_text())["braintrust-antigravity-tracing"]
for groups in antigravity.values():
    for group in groups:
        for hook in group.get("hooks", [group]):
            assert hook["command"].startswith("bt trace hook --source antigravity ")
PY

echo "test: hook forwarders OK"
