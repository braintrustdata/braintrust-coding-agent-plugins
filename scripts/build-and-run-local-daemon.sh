#!/usr/bin/env bash
# Build this checkout's translator and launch Claude with invocation-local hooks.
set +x
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"
PROJECT="local-daemon"
UPLOAD=false
DAEMON_PID=""
RUN_PID=""
RUN_DIR=""

fail() {
  printf 'local daemon: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage: build-and-run-local-daemon.sh [OPTIONS] [-- CLAUDE_ARGS...]

Build the local Rust daemon, then launch Claude in your current directory.
No plugin installation or Braintrust credentials are needed for local output.
Requires Bash (Git Bash on Windows), rustup/Cargo, and Claude on PATH.

  --project NAME    Upload to Braintrust instead of local NDJSON. Uses
                    BRAINTRUST_API_KEY, or securely prompts for it.
  --toolchain NAME  Rust toolchain (default: RUSTUP_TOOLCHAIN or 1.97.1).
                    Installed with rustup if missing.
  -h, --help        Show this help.

Examples:
  ./scripts/build-and-run-local-daemon.sh
  ./scripts/build-and-run-local-daemon.sh --project andrew-misc
  ./scripts/build-and-run-local-daemon.sh -- -p "Run a simple tool call"

The daemon has its own socket and data directory and stops when Claude exits.
Logs, journals, and local spans are retained in the printed private directory.
Saved bt login profiles are not used by the standalone development binary.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      [[ $# -ge 2 && -n "$2" ]] || fail '--project requires a name'
      PROJECT="$2"
      UPLOAD=true
      shift 2
      ;;
    --toolchain)
      [[ $# -ge 2 && -n "$2" ]] || fail '--toolchain requires a name'
      TOOLCHAIN="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    *) fail "unknown option: $1 (put Claude arguments after --)" ;;
  esac
done

command -v cargo >/dev/null 2>&1 || fail 'Cargo is required; install Rust from https://rustup.rs'
command -v rustup >/dev/null 2>&1 || fail 'rustup is required; install Rust from https://rustup.rs'
command -v "${CLAUDE_BIN:-claude}" >/dev/null 2>&1 || fail 'Claude is required on PATH (or set CLAUDE_BIN)'

if [[ "$UPLOAD" == true && -z "${BRAINTRUST_API_KEY:-}" ]]; then
  [[ -t 0 ]] || fail '--project requires BRAINTRUST_API_KEY when stdin is not a terminal'
  read -r -s -p 'Braintrust API key (not saved): ' BRAINTRUST_API_KEY
  printf '\n'
  [[ -n "$BRAINTRUST_API_KEY" ]] || fail 'Braintrust API key must not be empty'
  export BRAINTRUST_API_KEY
fi

if ! rustup run "$TOOLCHAIN" cargo --version >/dev/null 2>&1; then
  rustup toolchain install "$TOOLCHAIN" --profile minimal
fi
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/bt-daemon/target}"
printf '==> Building local daemon (%s)\n' "$TOOLCHAIN"
cargo "+$TOOLCHAIN" build --manifest-path "$REPO_ROOT/bt-daemon/Cargo.toml" \
  --locked --features cli --bin bt-daemon --target-dir "$TARGET_DIR"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
DAEMON_BIN="$TARGET_DIR/${CARGO_BUILD_TARGET:+$CARGO_BUILD_TARGET/}debug/bt-daemon"

RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bt-local.XXXXXX")"
NATIVE_DIR="$RUN_DIR"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    DAEMON_BIN="$DAEMON_BIN.exe"
    NATIVE_DIR="$(cygpath -m "$RUN_DIR")"
    export MSYS2_ARG_CONV_EXCL='*'
    export MSYS2_ENV_CONV_EXCL='BT_DAEMON_SOCKET;BT_DAEMON_DATA_DIR;BT_DAEMON_CONFIG'
    export BT_DAEMON_SOCKET="\\\\.\\pipe\\braintrust-bt-local-${RUN_DIR##*/}-$$"
    ;;
  *) export BT_DAEMON_SOCKET="$RUN_DIR/daemon.sock" ;;
esac
[[ -x "$DAEMON_BIN" ]] || fail "built daemon not found at $DAEMON_BIN; use a native CARGO_BUILD_TARGET"
export BT_DAEMON_DATA_DIR="$NATIVE_DIR/state"
export BT_DAEMON_CONFIG="$NATIVE_DIR/braintrust.json"
# This runner uses environment auth and an explicit project, never saved routes.
unset BRAINTRUST_PROFILE BRAINTRUST_DESTINATION

cleanup() {
  if [[ -n "$RUN_PID" ]] && kill -0 "$RUN_PID" 2>/dev/null; then
    # Let managed-run terminate Claude and flush before stopping its daemon.
    kill -INT "$RUN_PID" 2>/dev/null || true
    wait "$RUN_PID" 2>/dev/null || true
  fi
  if [[ -n "$DAEMON_PID" ]]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  printf '\nDaemon log: %s/daemon.log\n' "$NATIVE_DIR"
  printf 'Journals:   %s/state/journal/\n' "$NATIVE_DIR"
  if [[ "$UPLOAD" == false ]]; then
    printf 'Spans:      %s/state/spans/\n' "$NATIVE_DIR"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

DAEMON_ARGS=(serve --socket "$BT_DAEMON_SOCKET" --data-dir "$BT_DAEMON_DATA_DIR" --idle-timeout-secs 0)
if [[ "$UPLOAD" == false ]]; then
  DAEMON_ARGS+=(--debug-sink)
fi
"$DAEMON_BIN" "${DAEMON_ARGS[@]}" >"$RUN_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!
READY=false
for ((attempt = 0; attempt < 100; attempt++)); do
  kill -0 "$DAEMON_PID" 2>/dev/null || { cat "$RUN_DIR/daemon.log" >&2; fail 'daemon exited during startup'; }
  STATUS="$("$DAEMON_BIN" status --socket "$BT_DAEMON_SOCKET" --json 2>/dev/null || true)"
  if [[ "$STATUS" == *'"running":true'* ]]; then
    READY=true
    break
  fi
  sleep 0.1
done
[[ "$READY" == true ]] || fail "daemon did not become ready; see $RUN_DIR/daemon.log"

if [[ "$UPLOAD" == true ]]; then
  printf '==> Launching Claude; traces go to Braintrust project %s (tag: local-daemon)\n' "$PROJECT"
else
  printf '==> Launching Claude; local spans go to %s/state/spans/\n' "$NATIVE_DIR"
fi
# An explicit stdin preserves interactive input for the background child.
# Waiting in Bash lets INT/TERM traps run immediately rather than after Claude.
"$DAEMON_BIN" run --project "$PROJECT" --tag local-daemon claude -- "$@" <&0 &
RUN_PID=$!
set +e
wait "$RUN_PID"
RUN_STATUS=$?
set -e
RUN_PID=""
exit "$RUN_STATUS"
