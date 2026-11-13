"""Validate the standalone hosted MCP distribution before publishing."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
import stat
import tempfile
import zipfile


FILES = {"plugin.json", "mcp_config.json", "README.md", "LICENSE", "assets/icon.png"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique_keys)


def validate(root):
    root = Path(root)
    paths = [p for p in root.rglob("*") if ".git" not in p.relative_to(root).parts]
    require(not any(p.is_symlink() for p in paths), "symlinks are not allowed")
    actual = {p.relative_to(root).as_posix() for p in paths if p.is_file()}
    require(actual == FILES, f"package file allowlist mismatch: {actual ^ FILES}")
    manifest = read_json(root / "plugin.json")
    expected = {
        "$schema", "name", "displayName", "version", "description", "logo",
        "suggestedPrompts", "author", "homepage", "repository", "license", "keywords",
    }
    require(set(manifest) == expected, "unexpected or missing manifest fields")
    require(manifest["$schema"] == "https://antigravity.google/schemas/v1/plugin.json", "wrong schema")
    require(manifest["name"] == "braintrust" and manifest["displayName"] == "Braintrust", "wrong plugin identity")
    require(isinstance(manifest["version"], str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", manifest["version"]), "invalid version")
    for key in ("description", "homepage", "repository", "license"):
        require(isinstance(manifest[key], str) and manifest[key].strip(), f"missing {key}")
    require(manifest["homepage"].startswith("https://") and manifest["repository"].startswith("https://"), "URLs must use HTTPS")
    require(manifest["license"] == "MIT", "wrong license")
    require(manifest["author"] == {
        "name": "Braintrust", "email": "support@braintrust.dev", "url": "https://www.braintrust.dev/",
    }, "wrong publisher")
    prompts = manifest["suggestedPrompts"]
    require(isinstance(prompts, list) and 1 <= len(prompts) <= 3 and
            all(isinstance(p, str) and p.strip() for p in prompts), "invalid starter prompts")
    keywords = manifest["keywords"]
    require(isinstance(keywords, list) and keywords and
            all(isinstance(k, str) and k.strip() for k in keywords), "invalid keywords")
    require(manifest["logo"] == "assets/icon.png", "logo must reference the bundled icon")
    icon = (root / manifest["logo"]).read_bytes()
    require(icon[:8] == b"\x89PNG\r\n\x1a\n" and len(icon) >= 24, "invalid PNG icon")
    width, height = struct.unpack(">II", icon[16:24])
    require(width == height and width >= 128, "icon must be square and at least 128px")
    require(read_json(root / "mcp_config.json") == {
        "mcpServers": {"braintrust": {"serverUrl": "https://api.braintrust.dev/mcp"}},
    }, "MCP must use the hosted endpoint with automatic OAuth discovery")
    for name in ("README.md", "LICENSE"):
        require((root / name).read_text().strip(), f"empty {name}")
    return manifest


def validate_zip(path):
    with zipfile.ZipFile(path) as archive, tempfile.TemporaryDirectory() as temp:
        infos = archive.infolist()
        require(len(infos) == len(FILES) and {i.filename for i in infos} == FILES,
                "ZIP must contain only the MCP package at archive root")
        for info in infos:
            require(stat.S_IFMT(info.external_attr >> 16) == stat.S_IFREG, "ZIP entry must be a regular file")
            require(info.file_size <= 5 * 1024 * 1024, "oversized ZIP entry")
        archive.extractall(temp)  # Every path is in the exact file allowlist.
        return validate(temp)


def build_zip(root, output):
    root, output = Path(root), Path(output)
    manifest = validate(root)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"braintrust-antigravity-{manifest['version']}.zip"
    with tempfile.TemporaryDirectory(dir=output) as temp:
        staged = Path(temp) / path.name
        with zipfile.ZipFile(staged, "w", compression=zipfile.ZIP_STORED) as archive:
            for name in sorted(FILES):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                archive.writestr(info, (root / name).read_bytes())
        validate_zip(staged)
        staged.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(".zip.sha256").write_text(f"{digest}  {path.name}\n")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "validate-zip", "zip"])
    parser.add_argument("path", type=Path)
    parser.add_argument("output", type=Path, nargs="?")
    args = parser.parse_args()
    try:
        if args.command == "zip":
            require(args.output is not None, "zip requires an output directory")
            print(build_zip(args.path, args.output))
        else:
            {"validate": validate, "validate-zip": validate_zip}[args.command](args.path)
            print(f"validate: Antigravity MCP {args.command} OK ({args.path})")
    except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"validate: {error}\n")


if __name__ == "__main__":
    main()
