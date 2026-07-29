#!/bin/sh
# Thin, fail-open Codex hook shim for the shared Braintrust daemon.

set -u

log() { printf 'trace-codex: %s\n' "$1" >&2; }
truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1 | true | yes | on) return 0 ;;
    *) return 1 ;;
  esac
}
compatible_bt() {
  [ -n "${1:-}" ] && [ -x "$1" ] && "$1" daemon hook --help >/dev/null 2>&1
}

# Build the eventual bt invocation before checking availability so a first-run
# event can be replayed after the background installer completes.
ROOT="${PLUGIN_ROOT:-$(CDPATH= cd "$(dirname "$0")/.." && pwd)}"
version=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
  "$ROOT/.codex-plugin/plugin.json" 2>/dev/null | head -1)

set -- daemon hook --source codex
[ -z "$version" ] || set -- "$@" --source-version "$version"
truthy "${BRAINTRUST_FLUSH_ON_TURN_END:-false}" && set -- "$@" --flush-on-turn-end
[ -z "${CODEX_PARENT_SPAN_ID:-}" ] || set -- "$@" --parent-span-id "$CODEX_PARENT_SPAN_ID"
[ -z "${CODEX_ROOT_SPAN_ID:-}" ] || set -- "$@" --root-span-id "$CODEX_ROOT_SPAN_ID"
[ -z "${BRAINTRUST_ADDITIONAL_METADATA:-}" ] \
  || set -- "$@" --additional-metadata "$BRAINTRUST_ADDITIONAL_METADATA"

BT_BIN=$(command -v bt 2>/dev/null || true)
LOCAL_BT="${XDG_BIN_HOME:-$HOME/.local/bin}/bt"
if ! compatible_bt "$BT_BIN" && compatible_bt "$LOCAL_BT"; then
  BT_BIN="$LOCAL_BT"
fi
if ! compatible_bt "$BT_BIN"; then
  BT_BIN=""
fi
if [ -z "$BT_BIN" ] && command -v curl >/dev/null 2>&1; then
  # Never install synchronously in a blocking hook. Securely spool this event,
  # then install and forward it in the detached child so a first SessionStart
  # does not lose hook-only source/permission metadata.
  umask 077
  pending=$(mktemp "${TMPDIR:-/tmp}/trace-codex-event.XXXXXX" 2>/dev/null || true)
  if [ -n "$pending" ] && cat >"$pending"; then
    nohup sh -c '
      pending=$1
      shift
      trap '"'"'rm -f "$pending"'"'"' 0
      curl -fsSL --max-time 20 https://bt.dev/cli/install.sh | sh >/dev/null 2>&1 || exit 0
      bt_bin=$(command -v bt 2>/dev/null || true)
      if [ -z "$bt_bin" ] && [ -x "${XDG_BIN_HOME:-$HOME/.local/bin}/bt" ]; then
        bt_bin="${XDG_BIN_HOME:-$HOME/.local/bin}/bt"
      fi
      [ -z "$bt_bin" ] || "$bt_bin" "$@" <"$pending" >/dev/null 2>&1
    ' trace-codex-install "$pending" "$@" </dev/null >/dev/null 2>&1 &
    log "a daemon-capable bt CLI is unavailable; queued this event behind a background install or upgrade"
  else
    [ -z "$pending" ] || rm -f "$pending"
    nohup sh -c 'curl -fsSL --max-time 20 https://bt.dev/cli/install.sh | sh' \
      </dev/null >/dev/null 2>&1 &
    log "a daemon-capable bt CLI is unavailable; started a background install or upgrade but could not queue this event"
  fi
  exit 0
fi
if [ -z "$BT_BIN" ]; then
  log "a daemon-capable bt CLI is unavailable; tracing disabled for this event"
  exit 0
fi

# The standalone Codex plugin has historically exposed BRAINTRUST_PROJECT,
# while bt's global project option uses BRAINTRUST_DEFAULT_PROJECT. Preserve
# the documented plugin contract without overriding an explicit bt default.
if [ -z "${BRAINTRUST_DEFAULT_PROJECT:-}" ] && [ -n "${BRAINTRUST_PROJECT:-}" ]; then
  export BRAINTRUST_DEFAULT_PROJECT="$BRAINTRUST_PROJECT"
fi

"$BT_BIN" "$@" || log "bt daemon hook failed non-fatally"
exit 0
