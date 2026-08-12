#!/bin/sh
# Thin, credential-free Antigravity hook adapter. Antigravity runs hooks from
# the directory containing hooks.json and requires a JSON response on stdout.
# Tracing is deliberately fail-open: a missing or unhealthy bt CLI must never
# interrupt the coding-agent loop.

event=${1:-}
bt_bin=${BT_BIN:-bt}

if [ -n "$event" ] && command -v "$bt_bin" >/dev/null 2>&1; then
  "$bt_bin" trace hook \
    --source antigravity \
    --session-id-field conversationId \
    --event "$event" \
    --transcript-path-field transcriptPath \
    --flush-on-turn-end \
    >/dev/null 2>&1 || :
fi

case "$event" in
  Stop) printf '{"decision":""}\n' ;;
  *) printf '{}\n' ;;
esac

exit 0
