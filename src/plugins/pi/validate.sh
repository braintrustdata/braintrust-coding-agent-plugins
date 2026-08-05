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
node -e 'const p=require(process.argv[1]); if (p.dependencies?.braintrust) process.exit(1)' "$TARGET_DIR/package.json" \
  || fail "Braintrust SDK remains a Pi runtime dependency"
if rg -n "from [\"']braintrust[\"']|from [\"']\.\/(client|state)[\"']" "$TARGET_DIR/src/index.ts"; then
  fail "Pi daemon adapter imports the Braintrust SDK or local span persistence"
fi

echo "validate: Pi npm package OK ($TARGET_DIR)"
