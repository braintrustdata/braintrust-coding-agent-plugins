#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

for file in .devin-plugin/plugin.json icon.png README.md LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done

python3 - "$TARGET_DIR" <<'PYTHON' || fail "invalid Devin package"
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
manifest = json.loads((root / ".devin-plugin/plugin.json").read_text())
assert manifest["name"] == "braintrust"
assert re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", manifest["version"])
assert 0 < len(manifest["shortDescription"]) <= 40
assert manifest["description"].strip()
assert manifest["logo"] == "icon.png"
assert (root / manifest["logo"]).read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
assert manifest["mcpServers"] == {
    "braintrust": {"url": "https://api.braintrust.dev/mcp"}
}
for path in ("plugin.json", ".mcp.json", "mcp.json", "hooks", "node_modules"):
    assert not (root / path).exists(), f"unexpected runtime or shadow configuration: {path}"
for path in root.rglob("*"):
    assert not path.is_symlink(), f"unexpected symlink: {path}"
PYTHON

echo "validate: devin dist OK ($TARGET_DIR)"
