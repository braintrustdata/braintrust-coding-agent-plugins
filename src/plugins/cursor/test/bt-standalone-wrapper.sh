#!/bin/sh
# Adapt the production hook contract to the development-only daemon binary.
set -eu
: "${BT_DAEMON_BIN:?set BT_DAEMON_BIN}"
: "${BRAINTRUST_DAEMON_SOCKET:?set BRAINTRUST_DAEMON_SOCKET}"
[ "${1-}" = "trace" ]
[ "${2-}" = "hook" ]
shift 2
exec "$BT_DAEMON_BIN" hook "$@" --socket "$BRAINTRUST_DAEMON_SOCKET" --no-spawn
