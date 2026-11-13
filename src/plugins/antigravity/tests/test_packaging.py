"""Exercise MCP isolation, archive contents, and actual release workflow scripts."""
import glob
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile


REPO = Path(__file__).resolve().parents[4]
PLUGIN = REPO / "src/plugins/antigravity"
spec = importlib.util.spec_from_file_location("antigravity_mcp_package", PLUGIN / "mcp_package.py")
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


def run_step(workflow, name):
    text = (REPO / ".github/workflows" / workflow).read_text()
    tail = text.split(f"- name: {name}\n", 1)[1].split("        run: |\n", 1)[1]
    lines = []
    for line in tail.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line[10:])
    return "\n".join(lines)


class PackagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / "dist"
        shutil.copytree(PLUGIN / "content", self.dist)
        self.public = self.dist / "plugins/braintrust"

    def edit(self, path, fn):
        data = json.loads(path.read_text())
        fn(data)
        path.write_text(json.dumps(data))

    def test_reproducible_mcp_only_zip_and_unchanged_tracing(self):
        before = (self.dist / "hooks.json").read_bytes()
        first = package.build_zip(self.public, self.root / "one")
        for path in self.public.rglob("*"):
            os.utime(path, (1700000000, 1700000000))
        second = package.build_zip(self.public, self.root / "two")
        self.assertEqual(first.read_bytes(), second.read_bytes())
        with zipfile.ZipFile(first) as archive:
            self.assertEqual(set(archive.namelist()), package.FILES)
        self.assertEqual(package.validate_zip(first)["name"], "braintrust")
        self.assertEqual((self.dist / "hooks.json").read_bytes(), before)

    def test_runtime_hooks_and_credentials_rejected(self):
        for name in ["hooks.json", "credentials.json", "scripts/runtime.sh", ".antigravity-plugin/plugin.json"]:
            with self.subTest(name=name):
                target = self.public / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("{}")
                with self.assertRaisesRegex(ValueError, "allowlist"):
                    package.build_zip(self.public, self.root / "output")
                target.unlink()

    def test_auth_and_transport_overrides_rejected(self):
        path = self.public / "mcp_config.json"
        original = path.read_text()
        for field, value in [
            ("headers", {"Authorization": "Bearer test"}),
            ("oauth", {"clientSecret": "test"}),
            ("command", "node"),
            ("serverUrl", "https://{host}/mcp"),
        ]:
            with self.subTest(field=field):
                path.write_text(original)
                self.edit(path, lambda d: d["mcpServers"]["braintrust"].update({field: value}))
                with self.assertRaisesRegex(ValueError, "OAuth discovery"):
                    package.validate(self.public)

    def test_logo_escape_and_invalid_version_rejected(self):
        path = self.public / "plugin.json"
        original = path.read_text()
        for mutation in [lambda d: d.update(logo="../icon.png"), lambda d: d.update(version="latest")]:
            path.write_text(original)
            self.edit(path, mutation)
            with self.assertRaises(ValueError):
                package.validate(self.public)

    def test_symlinks_and_duplicate_keys_rejected(self):
        path = self.public / "mcp_config.json"
        original = path.read_text()
        path.write_text(original.replace('"serverUrl":', '"serverUrl": "https://elsewhere.invalid", "serverUrl":'))
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            package.validate(self.public)
        path.unlink()
        path.symlink_to(self.dist / "plugin.json")
        with self.assertRaisesRegex(ValueError, "symlinks"):
            package.validate(self.public)

    def test_zip_extra_entries_rejected(self):
        archive = package.build_zip(self.public, self.root / "output")
        with zipfile.ZipFile(archive, "a") as target:
            target.writestr("hooks.json", "{}")
        with self.assertRaisesRegex(ValueError, "archive root"):
            package.validate_zip(archive)

    def prepare_source(self):
        source = self.root / "src/plugins/antigravity/content"
        source.parent.mkdir(parents=True)
        shutil.copytree(self.dist, source)
        (self.root / "scripts").mkdir()
        shutil.copyfile(REPO / "scripts/set-plugin-version.py", self.root / "scripts/set-plugin-version.py")
        return source

    def test_release_preparation_tracks_nested_manifest(self):
        source = self.prepare_source()
        tracing = (source / "plugin.json").read_bytes()
        subprocess.run([sys.executable, "scripts/set-plugin-version.py", "antigravity", "v9.8.7"],
                       cwd=self.root, check=True, capture_output=True)
        self.assertEqual(package.validate(source / "plugins/braintrust")["version"], "9.8.7")
        self.assertEqual((source / "plugin.json").read_bytes(), tracing)
        output = self.root / "outputs"
        env = dict(os.environ, REQUESTED_VERSION="v9.8.7", PLUGIN="antigravity",
                   SOURCE_REF="refs/heads/main", GITHUB_OUTPUT=str(output))
        subprocess.run(["bash", "-euc", run_step("release.yml", "Validate release request")],
                       cwd=self.root, env=env, check=True, capture_output=True)
        patterns = output.read_text().split("paths<<EOF\n", 1)[1].split("EOF\n")[0].splitlines()
        selected = {str(Path(p).relative_to(self.root)) for pattern in patterns for p in glob.glob(str(self.root / pattern))}
        self.assertEqual(selected, {"src/plugins/antigravity/content/plugins/braintrust/plugin.json"})

    def test_publication_verifies_reviewed_version(self):
        source = self.prepare_source()
        current = package.validate(source / "plugins/braintrust")["version"]
        marker = self.root / ".github/release-versions/antigravity"
        marker.parent.mkdir(parents=True)
        marker.write_text(current + "\n")
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        script = run_step("_release.yml", "Stamp or verify reviewed release versions")
        for version, success in [("9.8.7", False), (current, True)]:
            with self.subTest(version=version):
                # Reset the manifest changed by the rejected candidate.
                subprocess.run(["git", "checkout", "--", "."], cwd=self.root, check=True)
                env = dict(os.environ, PLUGIN="antigravity", VERSION=version, RECORD="true")
                result = subprocess.run(["bash", "-euc", script], cwd=self.root, env=env, capture_output=True)
                self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_release_attachment_retry_and_checksum_verification(self):
        archive = package.build_zip(self.public, self.root / "upload")
        script = run_step("_release.yml", "Attach Antigravity MCP-only ZIP to distribution release")
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        gh = bin_dir / "gh"
        gh.write_text('#!/bin/bash\nset -eu\ntest "$1 $2" = "release upload"\ntest "$3" = "v$VERSION"\ntest "$4" = "braintrust-antigravity-$VERSION.zip"\ntest "$8" = "--clobber"\nprintf "%s\\n" "$*" >> "$ATTACH_LOG"\n')
        gh.chmod(0o755)
        env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"],
                   VERSION=package.validate(self.public)["version"],
                   DIST_REPO="braintrustdata/braintrust-antigravity-plugin", ATTACH_LOG=str(self.root / "log"))
        for _ in range(2):
            subprocess.run(["bash", "-euc", script], cwd=archive.parent, env=env, check=True, capture_output=True)
        self.assertEqual(len((self.root / "log").read_text().splitlines()), 2)
        archive.write_bytes(b"tampered")
        result = subprocess.run(["bash", "-euc", script], cwd=archive.parent, env=env, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len((self.root / "log").read_text().splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
