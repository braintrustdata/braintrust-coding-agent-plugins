#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail() { echo "validate: $*" >&2; exit 1; }

for file in .cursor-plugin/plugin.json hooks/hooks.json README.md LICENSE; do
  [[ -f "$TARGET_DIR/$file" ]] || fail "missing $file"
done

python3 - "$TARGET_DIR" <<'PY' || fail "invalid Cursor plugin"
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
manifest = json.loads((root / '.cursor-plugin/plugin.json').read_text())
assert manifest['name'] == 'trace-cursor'
assert re.fullmatch(r'\d+\.\d+\.\d+', manifest['version'])
assert manifest['hooks'] == 'hooks/hooks.json'
assert 'mcpServers' not in manifest and not (root / 'mcp.json').exists()
config = json.loads((root / manifest['hooks']).read_text())
assert config['version'] == 1
expected = {
    'sessionStart', 'sessionEnd', 'beforeSubmitPrompt',
    'postToolUse', 'postToolUseFailure', 'subagentStop',
    'afterShellExecution', 'afterMCPExecution', 'afterFileEdit', 'preCompact',
    'stop', 'afterAgentResponse',
}
assert set(config['hooks']) == expected
for event, registrations in config['hooks'].items():
    assert registrations == [{
        'command': 'bt trace hook --source cursor --session-id-field conversation_id --event-field hook_event_name --transcript-path-field transcript_path --flush-on-turn-end --capture-timeout-ms 8000',
        'timeout': 10,
        'failClosed': False,
    }]
assert 'Apache License' in (root / 'LICENSE').read_text()
PY

"$SRC_DIR/test/test_capture.sh" "$TARGET_DIR"
echo "validate: cursor dist OK ($TARGET_DIR)"
