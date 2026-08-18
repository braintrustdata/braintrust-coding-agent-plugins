#!/usr/bin/env python3
"""Render thin marketplace hook scripts from one canonical shell template."""

import argparse
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = REPO_ROOT / "src/runtime/hook-forwarder/forward.sh.tmpl"
SPECS = {
    "claude": {
        "source": "claude-code",
        "plugin_name": "trace-claude-code",
        "manifest": "plugins/trace-claude-code/.claude-plugin/plugin.json",
        "forwarder": "plugins/trace-claude-code/hooks/forward.sh",
    },
    "codex": {
        "source": "codex",
        "plugin_name": "trace-codex",
        "manifest": "plugins/trace-codex/.codex-plugin/plugin.json",
        "forwarder": "plugins/trace-codex/bin/codex-hook.sh",
    },
}


def render(agent: str, root: Path, check: bool) -> None:
    spec = SPECS[agent]
    manifest_path = root / spec["manifest"]
    version = json.loads(manifest_path.read_text())["version"]
    contents = TEMPLATE.read_text()
    replacements = {
        "@SOURCE@": spec["source"],
        "@PLUGIN_NAME@": spec["plugin_name"],
        "@PLUGIN_VERSION@": version,
    }
    for token, value in replacements.items():
        contents = contents.replace(token, value)

    destination = root / spec["forwarder"]
    if check:
        if not destination.exists() or destination.read_text() != contents:
            raise SystemExit(f"generated hook forwarder differs: {destination}")
        print(f"Verified {agent} hook forwarder")
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(contents)
    destination.chmod(0o755)
    print(f"Rendered {agent} hook forwarder -> {destination}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("agent", choices=[*SPECS, "all"])
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--target-root",
        type=Path,
        help="render/check one agent distribution root instead of checked-in content",
    )
    args = parser.parse_args()
    agents = list(SPECS) if args.agent == "all" else [args.agent]
    if args.target_root is not None and len(agents) != 1:
        parser.error("--target-root requires one agent")
    for agent in agents:
        root = (
            args.target_root.resolve()
            if args.target_root is not None
            else REPO_ROOT / "src/plugins" / agent / "content"
        )
        render(agent, root, args.check)


if __name__ == "__main__":
    main()
