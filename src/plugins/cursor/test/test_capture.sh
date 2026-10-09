#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../content" && pwd)}"
python3 - "$PLUGIN_DIR" <<'PY'
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

source = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="cursor-capture-") as temporary:
    root = Path(temporary)
    config = json.loads((source / 'hooks/hooks.json').read_text())
    fake_bin = root / 'bin'
    fake_bin.mkdir()
    fake_bt = fake_bin / 'bt'
    fake_bt.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$CAPTURE_DIR/args"
/bin/cat > "$CAPTURE_DIR/payload"
case " $* " in
  *" --event beforeSubmitPrompt "*) printf '%s\\n' '{"continue":true}' ;;
  *" --event preToolUse "*|*" --event subagentStart "*) printf '%s\\n' '{"permission":"allow"}' ;;
  *) printf '%s\\n' '{}' ;;
esac
exit "${BT_EXIT_CODE:-0}"
''')
    fake_bt.chmod(0o755)
    environment = dict(os.environ, CAPTURE_DIR=str(root), PATH=str(fake_bin))
    payload = b'{"conversation_id":"session-a","hook_event_name":"postToolUse","transcript_path":null,"tool_input":{"command":"echo $HOME; `pwd`"},"additive_field":42}\n'
    # Cursor hook responses come from the CLI after capture. The protocol
    # response must still be present when capture itself fails.
    for exit_code in ('0', '1'):
        environment['BT_EXIT_CODE'] = exit_code
        for event, registrations in config['hooks'].items():
            registration, = registrations
            result = subprocess.run(registration['command'], shell=True,
                                    input=payload, env=environment,
                                    capture_output=True, executable='/bin/sh')
            assert result.returncode == int(exit_code), (event, result.returncode)
            expected_output = {
                'beforeSubmitPrompt': b'{"continue":true}\n',
                'preToolUse': b'{"permission":"allow"}\n',
                'subagentStart': b'{"permission":"allow"}\n',
            }.get(event, b'{}\n')
            assert result.stdout == expected_output, (event, result.stdout)
            assert (root / 'payload').read_bytes() == payload
            args = (root / 'args').read_text().splitlines()[-1]
            assert args == ('trace hook --source cursor '
                            f'--event {event} '
                            '--session-id-field conversation_id --event-field hook_event_name '
                            '--transcript-path-field transcript_path --flush-on-turn-end '
                            '--capture-timeout-ms 8000'), args
            assert registration['failClosed'] is False
            assert registration['timeout'] * 1000 > 8000
    for event, expected in (
        ('beforeSubmitPrompt', b'{"continue":true}\n'),
        ('preToolUse', b'{"permission":"allow"}\n'),
        ('subagentStart', b'{"permission":"allow"}\n'),
    ):
        result = subprocess.run([str(source / 'hooks/trace.sh'), event],
                                input=payload, env=environment,
                                capture_output=True)
        assert result.returncode == 0, (event, result.returncode)
        assert result.stdout == expected, (event, result.stdout)
    fake_bt.unlink()
    for event, registrations in config['hooks'].items():
        result = subprocess.run(registrations[0]['command'], shell=True,
                                input=payload, env=environment,
                                capture_output=True, executable='/bin/sh')
        assert result.returncode != 0, (event, result.returncode)
        assert registrations[0]['failClosed'] is False
print('cursor capture: portable command forwarding OK')
PY
