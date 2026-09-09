#!/bin/bash
# Thin, fail-open bridge from Grok hooks to the shared Braintrust daemon.

PLUGIN_VERSION="0.1.0"

resolve_bt() {
  if [[ -n "${BT_BIN:-}" && -x "$BT_BIN" ]]; then
    printf '%s\n' "$BT_BIN"
    return 0
  fi
  if command -v bt >/dev/null 2>&1; then
    command -v bt
    return 0
  fi

  local candidate
  for candidate in \
    "${XDG_BIN_HOME:-${HOME:-}/.local/bin}/bt" \
    "${CARGO_HOME:-${HOME:-}/.cargo}/bin/bt"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

BT_BIN="$(resolve_bt || true)"
if [[ -z "$BT_BIN" ]]; then
  printf 'trace-grok: bt CLI not found; tracing skipped\n' >&2
  exit 0
fi

BT_ARGS=(
  trace hook
  --source grok
  --plugin-version "$PLUGIN_VERSION"
  --session-id-field sessionId
  --event-field hookEventName
  --transcript-path-field transcriptPath
)
if [[ -n "${GROK_VERSION:-}" ]]; then
  BT_ARGS+=(--source-version "$GROK_VERSION")
fi

"$BT_BIN" "${BT_ARGS[@]}" || true
exit 0
