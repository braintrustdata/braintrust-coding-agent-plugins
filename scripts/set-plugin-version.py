#!/usr/bin/env python3
"""Set a release version in each of an agent's per-plugin manifests.

Versioning is per-plugin: every plugin under an agent carries its own
.<agent>-plugin/plugin.json with a `version` field. A release stamps the given
version into each of those manifests. The marketplace manifest is NOT touched.

Only the version value is rewritten (surgical regex), so the manifests keep
their exact formatting.

Usage: set-plugin-version.py <agent> <version>   (leading 'v' is stripped)
"""
import glob
import os
import re
import sys

# agent -> glob of its per-plugin manifests (relative to repo root)
MANIFEST_GLOBS = {
    "claude": "src/plugins/claude/content/plugins/*/.claude-plugin/plugin.json",
    "codex": "src/plugins/codex/content/plugins/*/.codex-plugin/plugin.json",
}

VERSION_RE = re.compile(r'("version"\s*:\s*")[^"]*(")')


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: set-plugin-version.py <agent> <version>")
    agent, version = sys.argv[1], sys.argv[2].lstrip("v")
    pattern = MANIFEST_GLOBS.get(agent)
    if not pattern:
        sys.exit(f"unknown agent '{agent}' (known: {', '.join(MANIFEST_GLOBS)})")

    manifests = sorted(glob.glob(pattern))
    if not manifests:
        sys.exit(f"no plugin manifests found for '{agent}' ({pattern})")

    for path in manifests:
        text = open(path).read()
        new_text, n = VERSION_RE.subn(rf"\g<1>{version}\g<2>", text, count=1)
        if n == 0:
            sys.exit(f"no version field in {path}")
        with open(path, "w") as f:
            f.write(new_text)
        print(f"set {os.path.relpath(path)} version -> {version}")


if __name__ == "__main__":
    main()
