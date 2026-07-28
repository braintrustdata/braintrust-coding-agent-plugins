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
  "plugins/trace-claude-code/bin/claude-hook.sh"
  "plugins/trace-claude-code/bin/claude-hook.cmd"
)
for rel in "${required[@]}"; do
  [[ -f "$TARGET_DIR/$rel" ]] || fail "missing $rel"
  case "$rel" in *.json) check_json "$TARGET_DIR/$rel";; esac
done

grep -q "'daemon','hook','--source','claude-code'" \
  "$TARGET_DIR/plugins/trace-claude-code/bin/claude-hook.cmd" \
  || fail "Claude Windows hook does not invoke bt daemon"

# The Rust daemon is the only event processor. Shipping any of the legacy
# per-event shell processors would reintroduce two competing trace models.
legacy_hooks=(
  common.sh session_start.sh user_prompt_submit.sh user_prompt_expansion.sh
  post_tool_use.sh post_tool_use_failure.sh permission_denied.sh stop_hook.sh
  session_end.sh worker.sh
)
for hook in "${legacy_hooks[@]}"; do
  [[ ! -e "$TARGET_DIR/plugins/trace-claude-code/hooks/$hook" ]] \
    || fail "obsolete Claude processor was packaged: hooks/$hook"
done

# Every marketplace entry's source path must exist in the built tree.
if command -v jq >/dev/null 2>&1; then
  while IFS= read -r p; do
    [[ -d "$TARGET_DIR/$p" ]] || fail "marketplace source path not found: $p"
  done < <(jq -r '.plugins[].source' "$MARKETPLACE")
fi

echo "validate: claude dist OK ($TARGET_DIR)"
