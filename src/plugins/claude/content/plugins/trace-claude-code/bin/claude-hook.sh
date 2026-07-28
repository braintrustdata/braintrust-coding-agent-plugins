#!/bin/sh
# Thin, fail-open Claude Code hook shim for the shared Braintrust daemon.

set -u

log() { printf 'trace-claude-code: %s\n' "$1" >&2; }
truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1 | true | yes | on) return 0 ;;
    *) return 1 ;;
  esac
}

truthy "${TRACE_TO_BRAINTRUST:-false}" || exit 0

compatible_bt() {
  [ -n "${1:-}" ] && [ -x "$1" ] && "$1" daemon hook --help >/dev/null 2>&1
}

BT_BIN=$(command -v bt 2>/dev/null || true)
LOCAL_BT="${XDG_BIN_HOME:-$HOME/.local/bin}/bt"
if ! compatible_bt "$BT_BIN" && compatible_bt "$LOCAL_BT"; then
  BT_BIN="$LOCAL_BT"
fi
if ! compatible_bt "$BT_BIN"; then
  INSTALL_LOCK="${TMPDIR:-/tmp}/braintrust-bt-daemon-install-${UID:-user}"
  if command -v curl >/dev/null 2>&1 && mkdir "$INSTALL_LOCK" 2>/dev/null; then
    nohup sh -c 'curl -fsSL --max-time 20 https://bt.dev/cli/install.sh | sh; rmdir "$1"' \
      sh "$INSTALL_LOCK" </dev/null >/dev/null 2>&1 &
    log "a daemon-capable bt CLI is unavailable; started a background install or upgrade"
  else
    log "a daemon-capable bt CLI is unavailable; tracing disabled for this event"
  fi
  exit 0
fi

# Preserve the Claude plugin's project-name compatibility while bt owns auth.
if [ -z "${BRAINTRUST_PROJECT:-}" ] && [ -n "${BRAINTRUST_CC_PROJECT:-}" ]; then
  export BRAINTRUST_PROJECT="$BRAINTRUST_CC_PROJECT"
fi
if [ -z "${BRAINTRUST_DEFAULT_PROJECT:-}" ] && [ -n "${BRAINTRUST_PROJECT:-}" ]; then
  export BRAINTRUST_DEFAULT_PROJECT="$BRAINTRUST_PROJECT"
fi

PLUGIN_JSON="$(dirname "$0")/../.claude-plugin/plugin.json"
PLUGIN_VERSION=""
if [ -f "$PLUGIN_JSON" ]; then
  PLUGIN_VERSION=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$PLUGIN_JSON" | head -1)
fi

set -- daemon hook --source claude-code
[ -z "$PLUGIN_VERSION" ] || set -- "$@" --source-version "$PLUGIN_VERSION"
truthy "${BRAINTRUST_FLUSH_ON_TURN_END:-false}" && set -- "$@" --flush-on-turn-end
[ -z "${CC_PARENT_SPAN_ID:-}" ] || set -- "$@" --parent-span-id "$CC_PARENT_SPAN_ID"
[ -z "${CC_ROOT_SPAN_ID:-}" ] || set -- "$@" --root-span-id "$CC_ROOT_SPAN_ID"
[ -z "${BRAINTRUST_ADDITIONAL_METADATA:-}" ] \
  || set -- "$@" --additional-metadata "$BRAINTRUST_ADDITIONAL_METADATA"
[ -z "${CC_EXPERIMENT_ID:-}" ] || set -- "$@" --experiment-id "$CC_EXPERIMENT_ID"

INPUT=$(cat)
if [ -n "${BRAINTRUST_RECORD_DIR:-}" ] && command -v jq >/dev/null 2>&1; then
  mkdir -p "$BRAINTRUST_RECORD_DIR/transcripts" 2>/dev/null || true
  printf '%s' "$INPUT" | jq -c --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{hook:(.hook_event_name // ""),ts:$ts,payload:.}' \
    >>"$BRAINTRUST_RECORD_DIR/events.ndjson" 2>/dev/null || true
  for field in transcript_path agent_transcript_path; do
    transcript=$(printf '%s' "$INPUT" | jq -r --arg field "$field" '.[$field] // empty' 2>/dev/null)
    if [ -n "$transcript" ] && [ -f "$transcript" ]; then
      cp "$transcript" "$BRAINTRUST_RECORD_DIR/transcripts/$(basename "$transcript")" 2>/dev/null || true
    fi
  done
fi

printf '%s' "$INPUT" | "$BT_BIN" "$@" || log "bt daemon hook failed non-fatally"
exit 0
