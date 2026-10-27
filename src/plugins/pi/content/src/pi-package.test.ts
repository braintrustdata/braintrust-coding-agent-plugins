import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadPiPackageMetadata, resolvePiPackage } from "./pi-package.ts";

let root: string;

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value));
}

function installPi(directory: string, manifest: Record<string, unknown>): string {
  const packageDirectory = join(root, directory);
  writeJson(join(packageDirectory, "package.json"), manifest);
  const cli = join(packageDirectory, "dist", "cli.js");
  mkdirSync(dirname(cli), { recursive: true });
  writeFileSync(cli, "");
  return cli;
}

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "pi-package-test-"));
});

afterEach(() => {
  rmSync(root, { recursive: true, force: true });
});

const PI = "@earendil-works/pi-coding-agent";

describe("loadPiPackageMetadata", () => {
  // Each case is one way Pi can be installed or embedded; the lookup must
  // find the running Pi without resolving its import-only package exports.
  it.each<[string, () => (string | undefined)[], ReturnType<typeof loadPiPackageMetadata>]>([
    [
      "an npm install run through its node_modules/.bin symlink",
      () => {
        const bin = join(root, "node_modules", ".bin", "pi");
        mkdirSync(dirname(bin), { recursive: true });
        symlinkSync(installPi(`node_modules/${PI}`, { name: PI, version: "1.0.0" }), bin);
        return [bin];
      },
      { name: PI, version: "1.0.0", configDir: ".pi" },
    ],
    [
      "a fork with its own name and config directory",
      () => [
        installPi("fork", {
          name: "@acme/agent",
          version: "3.2.1",
          piConfig: { configDir: ".acme" },
        }),
      ],
      { name: "@acme/agent", version: "3.2.1", configDir: ".acme" },
    ],
    [
      "a PI_PACKAGE_DIR override",
      () => {
        installPi("store/pi", { name: PI, version: "1.0.0" });
        return [join(root, "store", "pi", "package.json"), join(root, "store", "bin")];
      },
      { name: PI, version: "1.0.0", configDir: ".pi" },
    ],
    [
      "a Bun-compiled binary with package.json beside it",
      () => {
        writeJson(join(root, "pi-linux-x64", "package.json"), { name: PI, version: "1.0.0" });
        return [undefined, "/$bunfs/root/pi", join(root, "pi-linux-x64", "pi")];
      },
      { name: PI, version: "1.0.0", configDir: ".pi" },
    ],
    [
      "an SDK application that depends on Pi",
      () => {
        writeJson(join(root, "app", "package.json"), { name: "app", version: "9.9.9" });
        const pi = installPi(`app/node_modules/${PI}`, { name: PI, version: "0.99.2" });
        return [join(root, "app", "main.js"), pi];
      },
      { name: PI, version: "0.99.2", configDir: ".pi" },
    ],
    [
      "no discoverable Pi",
      () => [join(root, "missing.js")],
      { name: undefined, version: undefined, configDir: ".pi" },
    ],
  ])("finds %s", (_name, setup, expected) => {
    expect(loadPiPackageMetadata(setup())).toEqual(expected);
  });

  it("prefers OMP's running manifest over an auto-installed upstream peer", () => {
    const host = installPi("omp", {
      name: "@oh-my-pi/pi-coding-agent",
      version: "18.12.6",
      bin: { omp: "dist/cli.js" },
    });
    const peer = installPi(`plugins/node_modules/${PI}`, { name: PI, version: "1.4.0" });
    expect(loadPiPackageMetadata([host, peer])).toEqual({
      name: "@oh-my-pi/pi-coding-agent",
      version: "18.12.6",
      configDir: ".omp",
    });
  });

  it("reports the embedded Pi for an SDK application's own CLI", () => {
    const app = installPi("app", {
      name: "acme-agent",
      version: "0.3.0",
      bin: { acme: "dist/cli.js" },
      dependencies: { [PI]: "^1.4.0" },
    });
    const pi = installPi(`app/node_modules/${PI}`, { name: PI, version: "1.4.0" });
    expect(loadPiPackageMetadata([app, pi])).toEqual({
      name: PI,
      version: "1.4.0",
      configDir: ".pi",
    });
  });

  it("finds a compiled OMP host beside its executable before the upstream peer", () => {
    writeJson(join(root, "omp-release", "package.json"), {
      name: "@oh-my-pi/pi-coding-agent",
      version: "18.12.6",
    });
    const peer = installPi(`plugins/node_modules/${PI}`, { name: PI, version: "1.4.0" });
    expect(
      loadPiPackageMetadata(["/$bunfs/root/cli.ts", join(root, "omp-release", "omp"), peer]),
    ).toMatchObject({ name: "@oh-my-pi/pi-coding-agent", version: "18.12.6", configDir: ".omp" });
  });

  it("trusts the explicit PI_PACKAGE_DIR manifest for a custom host", () => {
    writeJson(join(root, "custom", "package.json"), { name: "@acme/host", version: "3.2.1" });
    const peer = installPi(`plugins/node_modules/${PI}`, { name: PI, version: "1.4.0" });
    expect(loadPiPackageMetadata([join(root, "custom", "package.json"), peer])).toMatchObject({
      name: "@acme/host",
      version: "3.2.1",
    });
  });

  it("resolves the installed Pi development dependency through ESM conditions", () => {
    const metadata = loadPiPackageMetadata([resolvePiPackage()]);
    expect(metadata.version).toMatch(/^\d+\.\d+\.\d+/);
  });
});
