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

(cd "$SOURCE_DIR" && pnpm install --frozen-lockfile)
(cd "$SOURCE_DIR" && pnpm run check && pnpm test && pnpm run build && pnpm run smoke)
node "$REPO_ROOT/scripts/prepare-js-daemon-client.mjs" opencode --check

pack_json="$(cd "$TARGET_DIR" && pnpm pack --dry-run --json)"
node -e '
  const parsed = JSON.parse(process.argv[1])
  const result = Array.isArray(parsed) ? parsed[0] : parsed
  const files = new Set(result.files.map((file) => file.path))
  for (const required of ["dist/index.mjs", "dist/index.d.mts", "README.md", "LICENSE"]) {
    if (!files.has(required)) throw new Error(`package omits ${required}`)
  }
  for (const file of files) {
    if (file.startsWith("src/") || file.endsWith("daemon-client.ts")) {
      throw new Error(`package exposes generated source: ${file}`)
    }
  }
' "$pack_json"
(cd "$SOURCE_DIR" && pnpm run publish:dry-run >/dev/null)

for removed in client.ts tracing.ts event-processor.ts replay.ts span-queue.ts span-sink.ts; do
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
node -e '
  const manifest = require(process.argv[1])
  for (const field of ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]) {
    if (manifest[field]?.braintrust) process.exit(1)
  }
' "$TARGET_DIR/package.json" || fail "OpenCode package still depends on the Braintrust JavaScript SDK"

echo "validate: OpenCode npm package OK ($TARGET_DIR)"
