#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"

[[ -f "$TARGET_DIR/package.json" ]] || fail "missing package.json"
[[ -f "$TARGET_DIR/dist/index.mjs" ]] || fail "missing production entrypoint"
tarball="$(find "$TARGET_DIR" -maxdepth 1 -name 'braintrust-pi-extension-*.tgz' -print -quit)"
[[ -n "$tarball" ]] || fail "missing npm tarball"

(cd "$TARGET_DIR" && pnpm install --frozen-lockfile && pnpm run check && pnpm test && pnpm run smoke && pnpm run pack)
pack_check_dir="$(mktemp -d)"
(cd "$TARGET_DIR" && pnpm pack --pack-destination "$pack_check_dir" >/dev/null)

contents="$(tar -tzf "$tarball")"
for required in package/dist/index.mjs package/dist/index.d.mts package/README.md package/LICENSE; do
  grep -q "^${required}$" <<<"$contents" || fail "tarball omits $required"
done
if grep -q '^package/src/' <<<"$contents"; then fail "tarball contains local tracing source"; fi
node -e '
  const p = require(process.argv[1]);
  for (const field of ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]) {
    if (p[field]?.braintrust) process.exit(1);
  }
' "$TARGET_DIR/package.json" || fail "Braintrust SDK remains in Pi package dependencies"
for removed in client.ts legacy-processor.ts state.ts types.ts utils.ts; do
  [[ ! -e "$TARGET_DIR/src/$removed" ]] || fail "legacy tracing source remains: src/$removed"
done
if grep -RInE \
  "from [\"']braintrust[\"']|initLogger|startSpan|updateSpan|BRAINTRUST_API_KEY|api_key|state_dir|parent_span_id|root_span_id" \
  "$TARGET_DIR/src"; then
  fail "Pi source still contains local tracing, credential, or span-management logic"
fi
if grep -Eq '(^|[ /])braintrust@|^[[:space:]]+braintrust:' "$TARGET_DIR/pnpm-lock.yaml"; then
  fail "Braintrust SDK remains in the Pi lockfile"
fi

echo "validate: Pi npm package OK ($TARGET_DIR)"
