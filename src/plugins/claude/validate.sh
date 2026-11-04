#!/usr/bin/env bash
#
# validate.sh — Sanity-check a built Claude Code dist tree in $1 before publishing.
#
# Fails (non-zero) on the mistakes that would ship a broken marketplace:
#   - missing marketplace manifest / plugin manifests / hooks
#   - an MCP plugin that ships anything beyond its hosted server connection
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
  "plugins/trace-claude-code/.claude-plugin/plugin.json"
  "plugins/trace-claude-code/.cursor-plugin/plugin.json"
  "plugins/trace-claude-code/hooks/hooks.json"
)
for rel in "${required[@]}"; do
  [[ -f "$TARGET_DIR/$rel" ]] || fail "missing $rel"
  case "$rel" in *.json) check_json "$TARGET_DIR/$rel";; esac
done

python3 - "$MARKETPLACE" "$TARGET_DIR/plugins" <<'PY' \
  || fail "Claude marketplace must contain exactly the MCP and tracing plugins"
import json
import sys
from pathlib import Path

with open(sys.argv[1]) as f:
    plugins = json.load(f)["plugins"]

assert [plugin["name"] for plugin in plugins] == ["braintrust", "trace-claude-code"]
for plugin in plugins:
    assert plugin["source"] == f"./plugins/{plugin['name']}"
assert sorted(path.name for path in Path(sys.argv[2]).iterdir() if path.is_dir()) == [
    "braintrust", "trace-claude-code"
]
PY

# The braintrust plugin is the Anthropic directory listing: a hosted MCP
# connection only, with no hooks, skills, commands, or local processes.
python3 - "$TARGET_DIR/plugins" <<'PY' || fail "Claude braintrust plugin breaks its MCP-only contract"
import json
import struct
import sys
from pathlib import Path

plugins = Path(sys.argv[1])
root = plugins / "braintrust"
assert not any(p.is_symlink() for p in root.rglob("*"))
files = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
assert files == {
    ".claude-plugin/plugin.json", ".claude-plugin/icon.png", ".mcp.json", "README.md", "LICENSE",
}, files

manifest = json.loads((root / ".claude-plugin/plugin.json").read_text())
assert manifest["name"] == "braintrust" and manifest["displayName"] == "Braintrust"
assert manifest["author"]["name"] == "Braintrust"
assert manifest["license"] == "MIT"
assert manifest["icon"] == "./.claude-plugin/icon.png"
for field in ["homepage", "repository", "documentationUrl", "supportUrl", "privacyPolicyUrl", "termsOfServiceUrl"]:
    assert manifest[field].startswith("https://"), field
assert not ({"hooks", "commands", "agents", "skills", "lspServers", "mcpServers"} & set(manifest))

tracing = json.loads((plugins / "trace-claude-code/.claude-plugin/plugin.json").read_text())
assert manifest["version"] == tracing["version"], "Claude plugins must share a release version"
marketplace = json.loads((plugins.parent / ".claude-plugin/marketplace.json").read_text())
assert marketplace["version"] == manifest["version"], "Claude catalog version must match its plugins"
assert not any("version" in entry for entry in marketplace["plugins"])

# No headers or credentials: Claude Code discovers OAuth from the server.
# Plugin-root .mcp.json is the server map itself, with no mcpServers wrapper.
assert json.loads((root / ".mcp.json").read_text()) == {
    "braintrust": {"type": "http", "url": "https://api.braintrust.dev/mcp"},
}

# Anthropic directory listing icon: square PNG, 512..2048px, under 2 MB.
data = (root / ".claude-plugin/icon.png").read_bytes()
assert data[:8] == b"\x89PNG\r\n\x1a\n" and data[12:16] == b"IHDR" and len(data) < 2 * 1024 * 1024
width, height = struct.unpack(">II", data[16:24])
assert width == height and 512 <= width <= 2048, (width, height)
PY

if find "$TARGET_DIR" -name '.mcp.json' ! -path "$TARGET_DIR/plugins/braintrust/.mcp.json" -print -quit | grep -q .; then
  fail "Claude dist contains an MCP configuration outside plugins/braintrust"
fi

python3 - "$TARGET_DIR/plugins/trace-claude-code/hooks/hooks.json" <<'PY' \
  || fail "Claude hooks do not all directly execute the bt daemon client"
import json
import sys

with open(sys.argv[1]) as f:
    hooks = json.load(f)["hooks"]

# Cursor prefers its own manifest when importing an installed Claude plugin.
# An explicit empty hook config prevents fallback to the Claude hook file.
from pathlib import Path
plugin = Path(sys.argv[1]).parent.parent
cursor = json.loads((plugin / '.cursor-plugin/plugin.json').read_text())
assert cursor['name'] == 'trace-claude-code'
assert cursor['hooks'] == {'version': 1, 'hooks': {}}

expected_events = {
    "PermissionDenied", "PostCompact", "PostToolUse", "PostToolUseFailure",
    "PreToolUse", "SessionEnd", "SessionStart", "Stop", "StopFailure",
    "SubagentStart", "SubagentStop", "UserPromptExpansion", "UserPromptSubmit",
}
assert set(hooks) == expected_events
for definitions in hooks.values():
    for definition in definitions:
        for hook in definition["hooks"]:
            assert hook["type"] == "command"
            assert hook["command"] == "bt"
            assert hook["args"] == ["trace", "hook", "--source", "claude-code"]
            assert hook["async"] is False
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
