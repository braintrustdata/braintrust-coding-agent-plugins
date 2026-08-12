#!/bin/sh
# Adapt the production `bt trace hook ...` invocation to the standalone
# bt-daemon test binary while retaining the exact plugin command contract.
set -eu

: "${BT_DAEMON_BIN:?set BT_DAEMON_BIN}"
: "${BT_DAEMON_SOCKET:?set BT_DAEMON_SOCKET}"

[ "${1:-}" = "trace" ]
[ "${2:-}" = "hook" ]
shift 2

exec "$BT_DAEMON_BIN" hook "$@" --socket "$BT_DAEMON_SOCKET" --no-spawn
