#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../content" && pwd)}"
python3 - "$PLUGIN_DIR" <<'PY'
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

source = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="cursor-capture-") as temporary:
    root = Path(temporary)
    # Hook commands must survive spaces and shell metacharacters in install paths.
    plugin = root / 'plugin with spaces ; `literal` $(literal)'
    shutil.copytree(source, plugin)
    config = json.loads((plugin / 'hooks/hooks.json').read_text())
    fake_bin = root / 'bin'
    fake_bin.mkdir()
    fake_bt = fake_bin / 'bt'
    fake_bt.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$CAPTURE_DIR/args"
/bin/cat > "$CAPTURE_DIR/payload"
printf '%s\\n' '{"permission":"deny","followup_message":"unexpected"}'
printf '%s\\n' 'private diagnostic must not escape' >&2
exit "${BT_EXIT_CODE:-0}"
''')
    fake_bt.chmod(0o755)
    environment = dict(os.environ, CURSOR_PLUGIN_ROOT=str(plugin),
                       CAPTURE_DIR=str(root), PATH=str(fake_bin))
    environment.pop('BT_BIN', None)
    payload = b'{"conversation_id":"session-a","hook_event_name":"preToolUse","transcript_path":null,"tool_input":{"command":"echo $HOME; `pwd`"},"additive_field":42}\n'
    permission = {'preToolUse', 'subagentStart', 'beforeShellExecution',
                  'beforeMCPExecution', 'beforeReadFile'}
    for exit_code in ('0', '1', '2'):
        environment['BT_EXIT_CODE'] = exit_code
        for event, registrations in config['hooks'].items():
            registration, = registrations
            result = subprocess.run(['/bin/sh', '-c', registration['command']],
                                    input=payload, env=environment,
                                    capture_output=True, check=True)
            expected = {'permission': 'allow'} if event in permission else (
                {'continue': True} if event == 'beforeSubmitPrompt' else {})
            assert json.loads(result.stdout) == expected, (event, result.stdout)
            assert (root / 'payload').read_bytes() == payload
            assert b'private diagnostic' not in result.stderr
            assert len(result.stderr) <= 100
            args = (root / 'args').read_text().splitlines()[-1]
            assert args == ('trace hook --source cursor '
                            '--session-id-field conversation_id --event-field hook_event_name '
                            '--transcript-path-field transcript_path --flush-on-turn-end'), args
            assert registration['failClosed'] is False
    # Cursor's login shell can reset PATH; an explicit binary selection survives.
    # It must be treated as one executable even when the path has shell syntax.
    override = root / 'bt override ; literal'
    shutil.copyfile(fake_bt, override)
    override.chmod(0o755)
    environment['BT_BIN'] = str(override)
    environment['BT_EXIT_CODE'] = '0'
    result = subprocess.run(['/bin/sh', '-c', config['hooks']['preToolUse'][0]['command']],
                            input=payload, env=environment, capture_output=True, check=True)
    assert json.loads(result.stdout) == {'permission': 'allow'}
    assert (root / 'payload').read_bytes() == payload
    del environment['BT_BIN']
    # Missing bt must still yield valid responses for every registered hook.
    fake_bt.unlink()
    for event, registrations in config['hooks'].items():
        result = subprocess.run(['/bin/sh', '-c', registrations[0]['command']],
                                input=payload, env=environment,
                                capture_output=True, check=True)
        expected = {'permission': 'allow'} if event in permission else (
            {'continue': True} if event == 'beforeSubmitPrompt' else {})
        assert json.loads(result.stdout) == expected
        assert result.stderr == b''
print('cursor capture: raw forwarding, quoted paths, and fail-open responses OK')
PY
