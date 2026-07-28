#!/usr/bin/env bash
#
# validate.sh — Sanity-check a built Codex dist tree in $1 before publishing.
#
# Fails (non-zero) on the mistakes that would ship a broken marketplace:
#   - missing marketplace manifest / plugin manifests / launcher / hooks
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
  "plugins/trace-codex/bin/codex-hook.cmd"
)
for rel in "${required[@]}"; do
  [[ -f "$TARGET_DIR/$rel" ]] || fail "missing $rel"
  case "$rel" in *.json) check_json "$TARGET_DIR/$rel";; esac
done

grep -q "'daemon','hook','--source','codex'" \
  "$TARGET_DIR/plugins/trace-codex/bin/codex-hook.cmd" \
  || fail "Codex Windows hook does not invoke bt daemon"

# Every marketplace entry's source path must exist in the built tree.
if command -v jq >/dev/null 2>&1; then
  while IFS= read -r p; do
    [[ -d "$TARGET_DIR/$p" ]] || fail "marketplace source path not found: $p"
  done < <(jq -r '.plugins[].source.path' "$MARKETPLACE")
fi

echo "validate: codex dist OK ($TARGET_DIR)"
