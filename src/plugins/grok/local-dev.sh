#!/usr/bin/env bash
# Build and install trace-grok, run the standalone development daemon, and
# launch Grok with hooks routed to that daemon. The production bt CLI is not
# required for this local translator development loop.
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DIST_DIR="$REPO_ROOT/dist/grok"
DEV_DIR="${GROK_LOCAL_DEV_DIR:-/tmp/braintrust-grok-local-dev-${UID:-user}-$$}"
CURRENT_LINK="/tmp/braintrust-grok-local-dev-${UID:-user}-current"
DAEMON_BIN="$REPO_ROOT/bt-daemon/target/debug/bt-daemon"
SOCKET="$DEV_DIR/daemon.sock"
DATA_DIR="$DEV_DIR/state"
CONFIG="$DEV_DIR/braintrust.json"
DAEMON_LOG="$DEV_DIR/daemon.log"
BT_WRAPPER="$DEV_DIR/bt"
GROK_HOME_DIR="$DEV_DIR/grok-home"
PLUGIN_NAME="trace-grok"
DAEMON_PID=""
REPORT_TO_BRAINTRUST=false
SKIP_PLUGIN_RELOAD=false
BRAINTRUST_PROJECT="${BRAINTRUST_DEFAULT_PROJECT:-grok-local-dev}"
GROK_ARGS=()
GROK_ARG_COUNT=0

fail() {
  printf 'grok local dev: %s\n' "$*" >&2
  exit 1
}

find_probe_journal() {
  local journal_dir="$1"
  local session_id="$2"
  local candidate
  for candidate in "$journal_dir"/grok--"$session_id"--*.ndjson; do
    [[ -s "$candidate" ]] || continue
    printf '%s\n' "$candidate"
    return 0
  done
  return 1
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --braintrust)
      REPORT_TO_BRAINTRUST=true
      shift
      ;;
    --project)
      [[ $# -ge 2 ]] || fail "--project requires a project name"
      BRAINTRUST_PROJECT="$2"
      shift 2
      ;;
    --no-reload)
      SKIP_PLUGIN_RELOAD=true
      shift
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: local-dev.sh [--braintrust] [--project NAME] [--no-reload] [-- GROK_ARGS...]

  --braintrust    Send translated spans to Braintrust instead of local NDJSON
  --project NAME  Braintrust project name (default: $BRAINTRUST_DEFAULT_PROJECT
                  or grok-local-dev)
  --no-reload     Inject hooks directly for this run; do not send /reload-plugins
USAGE
      exit 0
      ;;
    --)
      shift
      GROK_ARGS+=("$@")
      GROK_ARG_COUNT=$((GROK_ARG_COUNT + $#))
      break
      ;;
    *)
      GROK_ARGS+=("$1")
      GROK_ARG_COUNT=$((GROK_ARG_COUNT + 1))
      shift
      ;;
  esac
done

if [[ "$REPORT_TO_BRAINTRUST" == true && -z "${BRAINTRUST_API_KEY:-}" ]]; then
  fail "--braintrust requires BRAINTRUST_API_KEY in the environment"
fi

for command in cargo grok python3 rsync; do
  command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done

cleanup_prior_runs() {
  local old_dir socket pid process_command
  for old_dir in /tmp/braintrust-grok-local-dev-"${UID:-user}"-[0-9]*; do
    [[ -e "$old_dir" ]] || continue
    socket="$old_dir/daemon.sock"

    # Stop only processes that demonstrably own a prior run's socket. This also
    # cleans up production bt daemons left by older versions of this script.
    if [[ -S "$socket" ]] && command -v lsof >/dev/null 2>&1; then
      while IFS= read -r pid; do
        [[ "$pid" =~ ^[0-9]+$ ]] || continue
        process_command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
        if [[ "$process_command" == *"$socket"* ]]; then
          kill "$pid" 2>/dev/null || true
        fi
      done < <(lsof -t "$socket" 2>/dev/null || true)
    elif [[ -f "$old_dir/daemon.pid" ]]; then
      pid="$(<"$old_dir/daemon.pid")"
      if [[ "$pid" =~ ^[0-9]+$ ]]; then
        process_command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
        if [[ "$process_command" == *"$socket"* ]]; then
          kill "$pid" 2>/dev/null || true
        fi
      fi
    fi
    rm -rf -- "$old_dir"
  done
  rm -f -- "$CURRENT_LINK"
}

printf '==> Cleaning prior Grok local-development runs\n'
cleanup_prior_runs

printf '==> Building and validating the Grok plugin\n'
"$SCRIPT_DIR/build.sh" "$DIST_DIR"
"$SCRIPT_DIR/validate.sh" "$DIST_DIR"

printf '==> Building the standalone development daemon\n'
if cargo +1.92.0 --version >/dev/null 2>&1; then
  cargo +1.92.0 build \
    --manifest-path "$REPO_ROOT/bt-daemon/Cargo.toml" \
    --features cli \
    --bin bt-daemon
else
  cargo build \
    --manifest-path "$REPO_ROOT/bt-daemon/Cargo.toml" \
    --features cli \
    --bin bt-daemon
fi

installed_source="$(grok plugin list --json | python3 -c '
import json, sys
for plugin in json.load(sys.stdin):
    if plugin.get("name") == "trace-grok":
        print(plugin.get("source", ""))
        break
')"

if [[ -z "$installed_source" ]]; then
  printf '==> Installing %s from %s\n' "$PLUGIN_NAME" "$DIST_DIR"
  grok plugin install "$DIST_DIR" --trust
else
  installed_real="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$installed_source")"
  dist_real="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$DIST_DIR")"
  if [[ "$installed_real" != "$dist_real" ]]; then
    fail "$PLUGIN_NAME is already installed from $installed_source; uninstall it with 'grok plugin uninstall $PLUGIN_NAME --confirm' before using this script"
  fi
  printf '==> Refreshing the existing local plugin installation\n'
  grok plugin update "$PLUGIN_NAME"
fi
grok plugin enable "$PLUGIN_NAME" >/dev/null
installed_path="$(grok plugin list --json | python3 -c '
import json, sys
for plugin in json.load(sys.stdin):
    if plugin.get("name") == "trace-grok":
        print(plugin.get("path", ""))
        break
')"
[[ -n "$installed_path" ]] || fail "could not locate the installed $PLUGIN_NAME plugin"

rm -rf "$DEV_DIR"
mkdir -p "$DEV_DIR"
ln -sfn "$DEV_DIR" "$CURRENT_LINK"
python3 - "$CONFIG" "$BRAINTRUST_PROJECT" "$REPORT_TO_BRAINTRUST" <<'PY'
import json
import sys

path, project, report_to_braintrust = sys.argv[1:]
with open(path, "w") as f:
    json.dump(
        {
            "trace_to_braintrust": True,
            "route": {
                "auth": {"source": "environment"},
                "destination": {
                    "type": "project_logs",
                    "project_name": project,
                },
                "flush_mode": (
                    "flush_on_turn_end"
                    if report_to_braintrust == "true"
                    else "fire_and_forget"
                ),
            },
        },
        f,
        indent=2,
    )
    f.write("\n")
PY

# Isolate Grok's own plugin registry so unrelated Claude-compatible plugins
# cannot start a production bt daemon on the development socket.
mkdir -p "$GROK_HOME_DIR"
cp "${GROK_HOME:-$HOME/.grok}/auth.json" "$GROK_HOME_DIR/auth.json"
ln -s "$installed_path" "$GROK_HOME_DIR/trace-grok"
if [[ "$SKIP_PLUGIN_RELOAD" == true ]]; then
  cat >"$GROK_HOME_DIR/config.toml" <<'TOML'
[plugins]
disabled = ["trace-grok", "trace-claude-code"]

[compat.claude]
hooks = false

[compat.cursor]
hooks = false
TOML
else
  cat >"$GROK_HOME_DIR/config.toml" <<TOML
[plugins]
paths = ["$GROK_HOME_DIR/trace-grok"]
enabled = ["trace-grok"]
disabled = ["trace-claude-code"]

[compat.claude]
hooks = false

[compat.cursor]
hooks = false
TOML
fi

cat >"$BT_WRAPPER" <<SH
#!/usr/bin/env bash
if [[ "\${1:-}" != "trace" || "\${2:-}" != "hook" ]]; then
  printf 'local Grok bt wrapper only supports: bt trace hook\\n' >&2
  exit 2
fi
shift 2
exec "$DAEMON_BIN" hook "\$@" --socket "$SOCKET" --no-spawn
SH
chmod +x "$BT_WRAPPER"

installed_hooks="$installed_path/hooks/hooks.json"
dist_hooks="$DIST_DIR/hooks/hooks.json"
installed_forward="$installed_path/hooks/forward.sh"
dist_forward="$DIST_DIR/hooks/forward.sh"
original_hooks="$DEV_DIR/hooks.json.original"
original_dist_hooks="$DEV_DIR/dist-hooks.json.original"
original_forward="$DEV_DIR/forward.sh.original"
original_dist_forward="$DEV_DIR/dist-forward.sh.original"

# Grok's local-source refresh can leave an interrupted development run's
# temporary env injection in the installed copy. Reset it from the freshly
# built artifact before taking backups or applying this run's injection.
if [[ "$installed_hooks" != "$dist_hooks" ]]; then
  cp "$dist_hooks" "$installed_hooks"
fi
if [[ "$installed_forward" != "$dist_forward" ]]; then
  cp "$dist_forward" "$installed_forward"
fi
cp "$installed_hooks" "$original_hooks"
cp "$dist_hooks" "$original_dist_hooks"
cp "$installed_forward" "$original_forward"
cp "$dist_forward" "$original_dist_forward"

restore_hooks() {
  if [[ -f "$original_hooks" && -n "$installed_hooks" ]]; then
    cp "$original_hooks" "$installed_hooks" 2>/dev/null || true
  fi
  if [[ -f "$original_dist_hooks" ]]; then
    cp "$original_dist_hooks" "$dist_hooks" 2>/dev/null || true
  fi
  if [[ -f "$original_forward" ]]; then
    cp "$original_forward" "$installed_forward" 2>/dev/null || true
  fi
  if [[ -f "$original_dist_forward" ]]; then
    cp "$original_dist_forward" "$dist_forward" 2>/dev/null || true
  fi
}

cleanup() {
  if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  restore_hooks
}
trap cleanup EXIT INT TERM

# Grok's hook runner intentionally does not pass arbitrary parent-process
# environment variables through. Add the isolated local route explicitly to
# this installed copy, then restore the original file when the session exits.
python3 - "$installed_hooks" "$dist_hooks" "$BT_WRAPPER" "$DAEMON_BIN" "$SOCKET" "$CONFIG" <<'PY'
import json
import sys

installed_path, dist_path, bt_bin, daemon_bin, socket, config = sys.argv[1:]
local_env = {
    "BT_BIN": bt_bin,
    "BT_DAEMON_BIN": daemon_bin,
    "BT_DAEMON_SOCKET": socket,
    "BT_DAEMON_CONFIG": config,
}
for path in (installed_path, dist_path):
    with open(path) as f:
        document = json.load(f)
    for groups in document["hooks"].values():
        for group in groups:
            for handler in group["hooks"]:
                handler["env"] = {**handler.get("env", {}), **local_env}
    with open(path, "w") as f:
        json.dump(document, f, indent=2)
        f.write("\n")
PY

# Pin the adapter itself to the local wrapper. This remains effective even when
# Grok strips parent-process variables, and patching both copies survives its
# local-source refresh during /reload-plugins.
python3 - "$installed_forward" "$dist_forward" "$BT_WRAPPER" "$DEV_DIR/grok-adapter.stderr" <<'PY'
import shlex
import sys

for path in sys.argv[1:3]:
    with open(path) as f:
        lines = f.readlines()
    lines.insert(1, f"exec 2>>{shlex.quote(sys.argv[4])}\n")
    lines.insert(1, f"export BT_BIN={shlex.quote(sys.argv[3])}\n")
    with open(path, "w") as f:
        f.writelines(lines)
PY

if [[ "$SKIP_PLUGIN_RELOAD" == true ]]; then
  # Grok 1.0.13 does not activate installed plugin hooks at process startup.
  # For a clean demo, install this artifact's hooks into the isolated run home,
  # where Grok discovers them as trusted global hooks without a slash command.
  direct_hooks_dir="$GROK_HOME_DIR/hooks"
  direct_forward="$direct_hooks_dir/forward.sh"
  mkdir -p "$direct_hooks_dir"
  cp "$dist_forward" "$direct_forward"
  chmod +x "$direct_forward"
  python3 - "$dist_hooks" "$direct_hooks_dir/braintrust.json" "$direct_forward" <<'PY'
import json
import shlex
import sys

source, destination, forward = sys.argv[1:]
with open(source) as f:
    document = json.load(f)
for groups in document["hooks"].values():
    for group in groups:
        for handler in group["hooks"]:
            handler["command"] = f"bash {shlex.quote(forward)}"
with open(destination, "w") as f:
    json.dump(document, f, indent=2)
    f.write("\n")
PY
fi

if [[ "$REPORT_TO_BRAINTRUST" == true ]]; then
  printf '==> Starting the standalone daemon with Braintrust project %s\n' "$BRAINTRUST_PROJECT"
  DAEMON_ARGS=(serve)
else
  printf '==> Starting the standalone daemon with the local debug sink\n'
  DAEMON_ARGS=(serve --debug-sink)
fi
DAEMON_ARGS+=(
  --socket "$SOCKET"
  --data-dir "$DATA_DIR"
  --idle-timeout-secs 0
)
RUST_LOG="${GROK_DAEMON_RUST_LOG:-bt_daemon::translate::grok=debug}" \
BT_DAEMON_CONFIG="$CONFIG" \
  "$DAEMON_BIN" "${DAEMON_ARGS[@]}" >"$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!
printf '%s\n' "$DAEMON_PID" >"$DEV_DIR/daemon.pid"

for _ in {1..100}; do
  [[ -S "$SOCKET" ]] && break
  kill -0 "$DAEMON_PID" 2>/dev/null || {
    cat "$DAEMON_LOG" >&2
    fail "daemon exited before creating its socket"
  }
  sleep 0.05
done
[[ -S "$SOCKET" ]] || fail "timed out waiting for daemon socket"

# Prove the plugin adapter and local daemon are connected before Grok starts.
PROBE_SESSION="grok-local-dev-probe"
PROBE_JOURNAL=""
printf '%s' "{\"hookEventName\":\"local_dev_probe\",\"sessionId\":\"$PROBE_SESSION\"}" | \
  BT_BIN="$BT_WRAPPER" \
  BT_DAEMON_BIN="$DAEMON_BIN" \
  BT_DAEMON_SOCKET="$SOCKET" \
  BT_DAEMON_CONFIG="$CONFIG" \
  "$DIST_DIR/hooks/forward.sh"
for _ in {1..100}; do
  PROBE_JOURNAL="$(find_probe_journal "$DATA_DIR/journal" "$PROBE_SESSION" || true)"
  [[ -n "$PROBE_JOURNAL" ]] && break
  sleep 0.05
done
if [[ -z "$PROBE_JOURNAL" || ! -s "$PROBE_JOURNAL" ]]; then
  cat "$DAEMON_LOG" >&2
  fail "hook-to-daemon probe was not journaled"
fi
if ! python3 - "$PROBE_JOURNAL" "$PROBE_SESSION" <<'PY'
import json
import sys

path, session_id = sys.argv[1:]
with open(path) as f:
    records = [json.loads(line) for line in f if line.strip()]
assert any(
    record.get("source") == "grok"
    and record.get("session_id") == session_id
    and record.get("event") == "local_dev_probe"
    for record in records
)
PY
then
  cat "$DAEMON_LOG" >&2
  fail "hook-to-daemon probe journal was invalid"
fi
printf '==> Hook-to-daemon probe captured successfully\n'

if [[ "$SKIP_PLUGIN_RELOAD" == true ]]; then
  printf '\nGrok will start now with tracing hooks injected for this run.\n'
else
  printf '\nGrok will start now and automatically run /reload-plugins. After the reload\n'
  printf 'finishes, send prompts or invoke tools.\n'
fi
printf 'Exit Grok to print the daemon log.\n'
printf 'For a second live view, run in another terminal:\n'
printf '  tail -f %q/daemon.log\n\n' "$CURRENT_LINK"

GROK_COMMAND=(grok --leader-socket "$DEV_DIR/grok-leader.sock")
if ((GROK_ARG_COUNT > 0)); then
  GROK_COMMAND+=("${GROK_ARGS[@]}")
fi
if [[ "$SKIP_PLUGIN_RELOAD" == false ]]; then
  GROK_COMMAND+=("/reload-plugins")
fi

set +e
BT_BIN="$BT_WRAPPER" \
BT_DAEMON_BIN="$DAEMON_BIN" \
BT_DAEMON_SOCKET="$SOCKET" \
BT_DAEMON_CONFIG="$CONFIG" \
GROK_HOME="$GROK_HOME_DIR" \
GROK_CLAUDE_HOOKS_ENABLED=false \
GROK_CURSOR_HOOKS_ENABLED=false \
GROK_HOOK_DEBUG=1 \
GROK_HOOKS_LOG="$DEV_DIR/grok-hooks.log" \
  "${GROK_COMMAND[@]}"
grok_status=$?
set -e

cleanup
DAEMON_PID=""
trap - EXIT INT TERM

printf '\n==> Daemon log (%s)\n' "$DAEMON_LOG"
cat "$DAEMON_LOG"
printf '\nRaw journals are under: %s/journal\n' "$DATA_DIR"
if [[ "$REPORT_TO_BRAINTRUST" == true ]]; then
  printf 'Translated spans were sent to Braintrust project: %s\n' "$BRAINTRUST_PROJECT"
  printf 'Inspect them with: bt view logs --project %q\n' "$BRAINTRUST_PROJECT"
else
  printf 'Translated spans are under: %s/spans\n' "$DATA_DIR"
fi

exit "$grok_status"
