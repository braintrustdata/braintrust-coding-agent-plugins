#!/usr/bin/env python3
"""Check shipped marketplace hooks against the canonical Rust AgentSpec events."""

import argparse
import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
CONSTANTS = {"claude": "CLAUDE_HOOK_EVENTS", "codex": "CODEX_HOOK_EVENTS"}


def canonical_events(agent: str) -> set[str]:
    source = (REPO_ROOT / "bt-daemon/src/lib.rs").read_text()
    name = CONSTANTS[agent]
    match = re.search(
        rf"const\s+{name}:\s*&\[&str\]\s*=\s*&\[(.*?)\];", source, re.DOTALL
    )
    if not match:
        raise SystemExit(f"could not find canonical {name} in bt-daemon/src/lib.rs")
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("agent", choices=CONSTANTS)
    parser.add_argument("hooks_json", type=Path)
    args = parser.parse_args()

    shipped = set(json.loads(args.hooks_json.read_text())["hooks"])
    canonical = canonical_events(args.agent)
    if shipped != canonical:
        missing = sorted(canonical - shipped)
        extra = sorted(shipped - canonical)
        raise SystemExit(
            f"{args.agent} hook event drift: missing={missing or '[]'} extra={extra or '[]'}"
        )
    print(f"Verified {args.agent} hook events ({len(shipped)})")


if __name__ == "__main__":
    main()
