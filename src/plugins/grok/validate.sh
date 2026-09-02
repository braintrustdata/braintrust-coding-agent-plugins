#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail() { echo "validate: $*" >&2; exit 1; }

[[ -x "$SRC_DIR/local-dev.sh" ]] || fail "local-dev.sh is not executable"
bash -n "$SRC_DIR/local-dev.sh" || fail "local-dev.sh has invalid shell syntax"

for file in .grok-plugin/plugin.json hooks/hooks.json hooks/forward.sh README.md LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done

python3 - "$TARGET_DIR/LICENSE" <<'PY' || fail "invalid or placeholder license"
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text()
assert "Apache License" in text
assert "Version 2.0, January 2004" in text
assert "END OF TERMS AND CONDITIONS" in text
assert "placeholder" not in text.lower()
assert "todo" not in text.lower()
PY
[[ -x "$TARGET_DIR/hooks/forward.sh" ]] || fail "hook adapter is not executable"

python3 - "$TARGET_DIR/hooks/hooks.json" <<'PY' || fail "invalid Grok hooks"
import json
import sys

with open(sys.argv[1]) as f:
    hooks = json.load(f)["hooks"]

expected = {
    "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
    "PostToolUseFailure", "PermissionDenied", "Stop", "StopFailure",
    "StopCancelled", "Notification", "SubagentStart", "SubagentStop",
    "PreCompact", "PostCompact", "SessionEnd",
}
assert set(hooks) == expected
for event, groups in hooks.items():
    expected_timeout = 15 if event == "SessionEnd" else 5
    for group in groups:
        for hook in group["hooks"]:
            assert hook == {
                "type": "command",
                "command": 'bash "${GROK_PLUGIN_ROOT}/hooks/forward.sh"',
                "timeout": expected_timeout,
            }
PY

if command -v grok >/dev/null 2>&1; then
  grok plugin validate "$TARGET_DIR" >/dev/null || fail "Grok rejected plugin manifest"
else
  python3 -m json.tool "$TARGET_DIR/.grok-plugin/plugin.json" >/dev/null \
    || fail "invalid plugin manifest"
fi

TEST_LOG="$(mktemp -d)/grok-hook-data"
trap 'rm -rf "$(dirname "$TEST_LOG")"' EXIT
"$SRC_DIR/test/test_hook.sh" "$TARGET_DIR/hooks/forward.sh" "$TEST_LOG"
"$SRC_DIR/test/test_local_dev.sh"

echo "validate: grok dist OK ($TARGET_DIR)"
