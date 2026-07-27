#!/bin/sh
# Launcher for the trace-codex hook binary.
#
# hooks.json invokes this script (a fixed, platform-agnostic command). The
# platform-specific binaries ship in this plugin under bin/ as
# codex-hook-<os>-<arch> (committed to the distribution repo at release time);
# this launcher picks the one matching the host and execs it.
#
# (Older versions downloaded the binary from GitHub Releases to avoid committing
# it to VCS. The binaries now ship in the dist repo, so there is no network
# path and no version/tag parsing here.)
#
# Hard rule: never fail the Codex turn. Any problem logs to stderr and exits 0
# (Codex treats a 0 exit with no stdout as success).

set -u

log() { printf 'trace-codex launcher: %s\n' "$1" >&2; }

# PLUGIN_ROOT is set by Codex to the installed plugin directory. Fall back to
# this script's own parent so the launcher is runnable standalone.
SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
ROOT="${PLUGIN_ROOT:-$(dirname "$SCRIPT_DIR")}"

# Map uname output to our binary suffix (<os>-<arch>).
os=$(uname -s 2>/dev/null || echo unknown)
arch=$(uname -m 2>/dev/null || echo unknown)
case "$os" in
  Darwin) os_name=darwin ;;
  Linux) os_name=linux ;;
  *) log "unsupported OS '$os'; tracing disabled this session"; exit 0 ;;
esac
case "$arch" in
  arm64 | aarch64) arch_name=arm64 ;;
  x86_64 | amd64) arch_name=x64 ;;
  *) log "unsupported arch '$arch'; tracing disabled this session"; exit 0 ;;
esac

BIN="$ROOT/bin/codex-hook-$os_name-$arch_name"
if [ ! -x "$BIN" ]; then
  log "no binary at $BIN; tracing disabled this session"
  exit 0
fi

exec "$BIN" "$@"
