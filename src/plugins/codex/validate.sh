#!/usr/bin/env bash
#
# validate.sh — Sanity-check a built Codex dist tree in $1 before publishing.
#
# Fails (non-zero) on the mistakes that would ship a broken marketplace:
#   - missing marketplace manifest / plugin manifests / daemon shims / hooks
#   - malformed JSON in any manifest
#   - marketplace entries whose `source.path` does not exist in the tree
#
# Usage: validate.sh <TARGET_DIR>

set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

# jq if available (preferred), else python3 as a fallback JSON checker.
check_json() {
  if command -v jq >/dev/null 2>&1; then
    jq empty "$1" >/dev/null 2>&1 || fail "invalid JSON: $1"
  else
    python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$1" 2>/dev/null \
      || fail "invalid JSON: $1"
  fi
}

MARKETPLACE="$TARGET_DIR/.agents/plugins/marketplace.json"
[[ -f "$MARKETPLACE" ]] || fail "missing $MARKETPLACE"
check_json "$MARKETPLACE"

# Required files for each shipped plugin.
required=(
  "plugins/braintrust-codex-plugin/.codex-plugin/plugin.json"
  "plugins/braintrust-codex-plugin/.mcp.json"
  "plugins/braintrust-codex-plugin/skills/braintrust/SKILL.md"
  "plugins/trace-codex/.codex-plugin/plugin.json"
  "plugins/trace-codex/hooks/hooks.json"
  "plugins/trace-codex/bin/codex-hook.sh"
)
for rel in "${required[@]}"; do
  [[ -f "$TARGET_DIR/$rel" ]] || fail "missing $rel"
  case "$rel" in *.json) check_json "$TARGET_DIR/$rel";; esac
done

python3 - "$TARGET_DIR/plugins/trace-codex/hooks/hooks.json" <<'PY' \
  || fail "Codex hooks do not all use the daemon forwarders"
import json
import sys

with open(sys.argv[1]) as f:
    hooks = json.load(f)["hooks"]

expected_events = {
    "PermissionRequest", "PostCompact", "PostToolUse", "PreCompact",
    "PreToolUse", "SessionStart", "Stop", "SubagentStart", "SubagentStop",
    "UserPromptSubmit",
}
assert set(hooks) == expected_events
for definitions in hooks.values():
    for definition in definitions:
        for hook in definition["hooks"]:
            assert hook["type"] == "command"
            assert hook["command"] == 'bash "${PLUGIN_ROOT}/bin/codex-hook.sh"'
            assert hook["commandWindows"] == 'bash "${PLUGIN_ROOT}\\bin\\codex-hook.sh"'
PY

grep -Fq 'trace hook --source codex' \
  "$TARGET_DIR/plugins/trace-codex/bin/codex-hook.sh" \
  || fail "Codex Unix forwarder does not invoke bt trace hook with source codex"
python3 - "$TARGET_DIR/plugins/trace-codex/bin/codex-hook.sh" <<'PY' \
  || fail "Codex forwarder does not install bt before forwarding"
import sys

text = open(sys.argv[1]).read()
assert text.index("command -v bt") < text.index("curl -fsSL")
assert text.index("curl -fsSL") < text.index("trace hook --source codex")
PY
if find "$TARGET_DIR/plugins/trace-codex" -type f \( \
  -name 'package.json' -o -name 'pnpm-lock.yaml' -o -name 'tsconfig.json' -o \
  -name 'codex-hook-*' \) -print -quit | grep -q .; then
  fail "Codex tracing plugin still contains a legacy tracing runtime"
fi

# Every marketplace entry's source path must exist in the built tree.
if command -v jq >/dev/null 2>&1; then
  while IFS= read -r p; do
    [[ -d "$TARGET_DIR/$p" ]] || fail "marketplace source path not found: $p"
  done < <(jq -r '.plugins[].source.path' "$MARKETPLACE")
fi

echo "validate: codex dist OK ($TARGET_DIR)"
