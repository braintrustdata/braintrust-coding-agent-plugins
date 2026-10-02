#!/bin/sh
# Cursor consumes stdout as a policy response. Never expose bt output there.
# Keep the original native JSON on stdin; the daemon owns all interpretation.
BT_BIN="${BT_BIN:-bt}"
if command -v "$BT_BIN" >/dev/null 2>&1; then
  if ! "$BT_BIN" trace hook --source cursor \
    --session-id-field conversation_id --event-field hook_event_name \
    --transcript-path-field transcript_path --flush-on-turn-end \
    >/dev/null 2>/dev/null; then
    printf '%s\n' 'trace-cursor: event capture unavailable; continuing.' >&2
  fi
fi

# These schemas are required even when bt is absent or capture fails. An empty
# or malformed permission-hook response can block Cursor despite failClosed=false.
case "${1-}" in
  preToolUse|subagentStart|beforeShellExecution|beforeMCPExecution|beforeReadFile)
    printf '%s\n' '{"permission":"allow"}' ;;
  beforeSubmitPrompt)
    printf '%s\n' '{"continue":true}' ;;
  *)
    printf '%s\n' '{}' ;;
esac
exit 0
