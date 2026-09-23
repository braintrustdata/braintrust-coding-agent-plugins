#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

for file in plugin.json hooks.json README.md LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done

if command -v jq >/dev/null 2>&1; then
  jq empty "$TARGET_DIR/plugin.json" "$TARGET_DIR/hooks.json" >/dev/null \
    || fail "invalid JSON"
else
  python3 -m json.tool "$TARGET_DIR/plugin.json" >/dev/null || fail "invalid plugin.json"
  python3 -m json.tool "$TARGET_DIR/hooks.json" >/dev/null || fail "invalid hooks.json"
fi

python3 - "$TARGET_DIR/hooks.json" <<'PY' || fail "Antigravity hooks must invoke bt directly"
import json
import sys

with open(sys.argv[1]) as f:
    hooks = json.load(f)["braintrust-antigravity-tracing"]

expected = {
    "PostToolUse": "PostToolUse",
    "PreInvocation": "PreInvocation",
    "PostInvocation": "PostInvocation",
    "Stop": "Stop",
}
assert set(hooks) == set(expected)
for event, expected_event in expected.items():
    for group in hooks[event]:
        for hook in group.get("hooks", [group]):
            assert hook["type"] == "command"
            assert hook["command"] == (
                "bt trace hook --source antigravity --session-id-field conversationId "
                f"--event {expected_event} --transcript-path-field transcriptPath "
                "--flush-on-turn-end"
            )
PY
echo "validate: antigravity dist OK ($TARGET_DIR)"
