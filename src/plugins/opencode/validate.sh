#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/../../.." && pwd)"
SOURCE_DIR="$PLUGIN_DIR/content"
fail() { echo "validate: $*" >&2; exit 1; }

TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
[[ -f "$TARGET_DIR/package.json" ]] || fail "missing package.json"
[[ -f "$TARGET_DIR/dist/index.mjs" ]] || fail "missing production entrypoint"
[[ -f "$TARGET_DIR/dist/index.d.mts" ]] || fail "missing production declarations"
[[ -f "$TARGET_DIR/dist/tracing.mjs" ]] || fail "missing trace-only managed entrypoint"
[[ -f "$TARGET_DIR/dist/tracing.d.mts" ]] || fail "missing trace-only declarations"

(cd "$SOURCE_DIR" && pnpm install --frozen-lockfile)
(cd "$SOURCE_DIR" && pnpm run check && pnpm test && pnpm run build && pnpm run smoke)
node "$REPO_ROOT/scripts/prepare-js-daemon-client.mjs" opencode --check

node "$REPO_ROOT/scripts/validate-npm-artifact.mjs" opencode "$TARGET_DIR"
(cd "$TARGET_DIR" && pnpm publish --dry-run --ignore-scripts --no-git-checks >/dev/null)

for removed in client.ts event-processor.ts replay.ts span-queue.ts span-sink.ts; do
  [[ ! -e "$TARGET_DIR/src/$removed" ]] || fail "old JavaScript tracing runtime remains: src/$removed"
done
if grep -R -n -E "fetch\(|/v1/|/btql|apikey/login|from [\"']\.\./tools" "$TARGET_DIR/src/tracing"; then
  fail "daemon tracing performs API access or imports the tools runtime"
fi
if grep -R -n -E \
  "fetch\(|https?://api\.|/v1/|/btql|apikey/login|BRAINTRUST_API_(KEY|URL)|BRAINTRUST_APP_URL" \
  "$TARGET_DIR/src"; then
  fail "OpenCode package contains direct Braintrust API or credential handling"
fi
if grep -R -n -E "bun:test|bun run|bun install|packageManager.*bun" "$TARGET_DIR" \
  --exclude-dir=node_modules --exclude-dir=dist; then
  fail "OpenCode package still depends on Bun tooling"
fi
grep -q 'BtCliToolsClient' "$TARGET_DIR/src/tools/index.ts" \
  || fail "data-access tools no longer delegate to bt"
grep -q '"--prefer-profile"' "$TARGET_DIR/src/tools/bt-cli.ts" \
  || fail "bt tool delegation does not prefer managed profiles"
echo "validate: OpenCode npm package OK ($TARGET_DIR)"
