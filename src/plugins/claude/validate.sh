#!/usr/bin/env bash
#
# validate.sh — Sanity-check a built Claude Code dist tree in $1 before publishing.
#
# Fails (non-zero) on the mistakes that would ship a broken marketplace:
#   - missing marketplace manifest / plugin manifests / hooks / skill
#   - malformed JSON in any manifest
#   - marketplace entries whose `source` path does not exist in the tree
#
# Usage: validate.sh <TARGET_DIR>

set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
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

MARKETPLACE="$TARGET_DIR/.claude-plugin/marketplace.json"
[[ -f "$MARKETPLACE" ]] || fail "missing $MARKETPLACE"
check_json "$MARKETPLACE"

# Required files for each shipped plugin.
required=(
  "plugins/braintrust/.claude-plugin/plugin.json"
  "plugins/braintrust/.mcp.json"
  "plugins/braintrust/skills/troubleshoot-braintrust-mcp/SKILL.md"
  "plugins/trace-claude-code/.claude-plugin/plugin.json"
  "plugins/trace-claude-code/hooks/hooks.json"
  "plugins/trace-claude-code/hooks/forward.sh"
)
for rel in "${required[@]}"; do
  [[ -f "$TARGET_DIR/$rel" ]] || fail "missing $rel"
  case "$rel" in *.json) check_json "$TARGET_DIR/$rel";; esac
done
python3 "$REPO_ROOT/scripts/render-hook-forwarders.py" claude --target-root "$TARGET_DIR" --check \
  || fail "Claude forwarder differs from the canonical template"

python3 "$REPO_ROOT/scripts/validate-hook-events.py" claude \
  "$TARGET_DIR/plugins/trace-claude-code/hooks/hooks.json" \
  || fail "Claude shipped hook events differ from AgentSpec"
python3 - "$TARGET_DIR/plugins/trace-claude-code/hooks/hooks.json" <<'PY' \
  || fail "Claude hooks do not all use the blocking daemon forwarder"
import json
import sys

with open(sys.argv[1]) as f:
    hooks = json.load(f)["hooks"]

for definitions in hooks.values():
    for definition in definitions:
        for hook in definition["hooks"]:
            assert hook["type"] == "command"
            assert hook["command"] == 'bash "${CLAUDE_PLUGIN_ROOT}/hooks/forward.sh"'
            assert hook["async"] is False
PY

grep -Fq 'trace hook --source claude-code --plugin-version' \
  "$TARGET_DIR/plugins/trace-claude-code/hooks/forward.sh" \
  || fail "Claude forwarder does not invoke bt trace hook with source claude-code"
python3 - "$TARGET_DIR/plugins/trace-claude-code/hooks/forward.sh" <<'PY' \
  || fail "Claude forwarder does not install bt before forwarding"
import sys

text = open(sys.argv[1]).read()
assert text.index("command -v bt") < text.index("curl -fsSL")
assert text.index("curl -fsSL") < text.index("trace hook --source claude-code")
PY

if find "$TARGET_DIR/plugins/trace-claude-code" -type f \( \
  -name 'common.sh' -o -name 'worker.sh' -o -name 'setup.sh' -o \
  -name 'package.json' -o -name 'pnpm-lock.yaml' \) -print -quit | grep -q .; then
  fail "Claude tracing plugin still contains a legacy tracing runtime"
fi

# Every marketplace entry's source path must exist in the built tree.
if command -v jq >/dev/null 2>&1; then
  while IFS= read -r p; do
    [[ -d "$TARGET_DIR/$p" ]] || fail "marketplace source path not found: $p"
  done < <(jq -r '.plugins[].source' "$MARKETPLACE")
fi

echo "validate: claude dist OK ($TARGET_DIR)"
