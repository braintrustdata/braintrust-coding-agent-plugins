#!/bin/bash
# Configure Claude Code to use the shared Braintrust tracing daemon.

set -euo pipefail

echo "Braintrust Claude Code tracing setup"
echo

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/bin/claude-hook.sh" ] || {
    echo "Missing bin/claude-hook.sh" >&2
    exit 1
}
command -v jq >/dev/null 2>&1 || {
    echo "jq is required to update .claude/settings.local.json" >&2
    exit 1
}

BT_BIN="$(command -v bt 2>/dev/null || true)"
LOCAL_BT="${XDG_BIN_HOME:-$HOME/.local/bin}/bt"
if ! [ -n "$BT_BIN" ] || ! "$BT_BIN" daemon hook --help >/dev/null 2>&1; then
    if [ -x "$LOCAL_BT" ] && "$LOCAL_BT" daemon hook --help >/dev/null 2>&1; then
        BT_BIN="$LOCAL_BT"
    else
        command -v curl >/dev/null 2>&1 || {
            echo "curl is required to install or upgrade bt" >&2
            exit 1
        }
        echo "Installing the daemon-capable bt CLI..."
        curl -fsSL https://bt.dev/cli/install.sh | bash
        BT_BIN="$LOCAL_BT"
    fi
fi

"$BT_BIN" daemon hook --help >/dev/null 2>&1 || {
    echo "The installed bt CLI does not support 'bt daemon hook'." >&2
    exit 1
}

if [ -z "${BRAINTRUST_API_KEY:-}" ] && ! "$BT_BIN" status --json >/dev/null 2>&1; then
    echo "Authenticate with Braintrust:"
    "$BT_BIN" auth login
fi

read -r -p "Braintrust project for traces [claude-code]: " PROJECT_NAME
PROJECT_NAME="${PROJECT_NAME:-claude-code}"

mkdir -p .claude
SETTINGS_FILE=".claude/settings.local.json"
if [ -f "$SETTINGS_FILE" ]; then
    jq --arg project "$PROJECT_NAME" '
      .env = (.env // {}) + {
        TRACE_TO_BRAINTRUST: "true",
        BRAINTRUST_CC_PROJECT: $project
      }
    ' "$SETTINGS_FILE" >"$SETTINGS_FILE.tmp"
else
    jq -n --arg project "$PROJECT_NAME" '{
      env: {
        TRACE_TO_BRAINTRUST: "true",
        BRAINTRUST_CC_PROJECT: $project
      }
    }' >"$SETTINGS_FILE.tmp"
fi
mv "$SETTINGS_FILE.tmp" "$SETTINGS_FILE"

echo
echo "Tracing enabled in $SETTINGS_FILE"
echo "Project: $PROJECT_NAME"
echo "Daemon status: $BT_BIN daemon status"
