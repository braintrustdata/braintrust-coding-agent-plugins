#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

for file in plugin.json hooks.json bin/antigravity-hook.sh README.md LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done
[[ -x "$TARGET_DIR/bin/antigravity-hook.sh" ]] || fail "hook adapter is not executable"

if command -v jq >/dev/null 2>&1; then
  jq empty "$TARGET_DIR/plugin.json" "$TARGET_DIR/hooks.json" >/dev/null \
    || fail "invalid JSON"
else
  python3 -m json.tool "$TARGET_DIR/plugin.json" >/dev/null || fail "invalid plugin.json"
  python3 -m json.tool "$TARGET_DIR/hooks.json" >/dev/null || fail "invalid hooks.json"
fi

"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test/test_hook.sh" \
  "$TARGET_DIR/bin/antigravity-hook.sh"
echo "validate: antigravity dist OK ($TARGET_DIR)"
