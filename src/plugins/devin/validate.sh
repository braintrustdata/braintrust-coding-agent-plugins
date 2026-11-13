#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

for file in README.md \
  plugins/braintrust/.devin-plugin/plugin.json \
  plugins/braintrust/icon.png plugins/braintrust/README.md plugins/braintrust/LICENSE \
  plugins/braintrust-trace-devin/.devin-plugin/plugin.json \
  plugins/braintrust-trace-devin/hooks.json \
  plugins/braintrust-trace-devin/README.md plugins/braintrust-trace-devin/LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done

python3 - "$TARGET_DIR" <<'PYTHON' || fail "invalid Devin package"
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
packages = root / "plugins"
assert {path.name for path in packages.iterdir()} == {
    "braintrust", "braintrust-trace-devin"
}
for name in ("braintrust", "braintrust-trace-devin"):
    plugin = packages / name
    manifest = json.loads((plugin / ".devin-plugin/plugin.json").read_text())
    assert manifest["name"] == name
    assert 0 < len(manifest["shortDescription"]) <= 40
    assert manifest["description"].strip()
    for field in ("requiredPlugins", "optionalPlugins", "forbiddenPlugins"):
        assert field not in manifest, f"{name}: packages must stay independent"
    for path in ("plugin.json", ".mcp.json", "mcp.json", "hooks", "node_modules"):
        assert not (plugin / path).exists(), f"{name}: unexpected runtime or shadow configuration: {path}"

mcp = packages / "braintrust"
manifest = json.loads((mcp / ".devin-plugin/plugin.json").read_text())
assert manifest["logo"] == "icon.png"
assert (mcp / manifest["logo"]).read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
assert manifest["mcpServers"] == {
    "braintrust": {"url": "https://api.braintrust.dev/mcp"}
}
assert not (mcp / "hooks.json").exists(), "MCP access must not register tracing hooks"

trace = packages / "braintrust-trace-devin"
manifest = json.loads((trace / ".devin-plugin/plugin.json").read_text())
assert "mcpServers" not in manifest, "tracing must not install MCP"
assert json.loads((trace / "hooks.json").read_text()) == {}, "tracing hooks must remain inactive until implemented"

for path in (".devin-plugin", "plugin.json", ".mcp.json", "mcp.json", "hooks.json"):
    assert not (root / path).exists(), f"install a plugin subdirectory, not the build root: {path}"
for path in root.rglob("*"):
    assert not path.is_symlink(), f"unexpected symlink: {path}"
PYTHON

echo "validate: devin dist OK ($TARGET_DIR)"
