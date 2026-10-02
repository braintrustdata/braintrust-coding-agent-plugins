import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadPiPackageMetadata, type PiPackageLookup } from "./pi-package.ts";

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

const unresolvable = () => {
  throw new Error("not exported");
};

/** Look Pi up without consulting this test process's own Node executable. */
function lookup(options: PiPackageLookup) {
  return loadPiPackageMetadata({
    executable: join(root, "missing", "node"),
    resolvePackage: unresolvable,
    ...options,
  });
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
  it.each<[string, () => PiPackageLookup, ReturnType<typeof loadPiPackageMetadata>]>([
    [
      "an npm install run through its node_modules/.bin symlink",
      () => {
        const bin = join(root, "node_modules", ".bin", "pi");
        mkdirSync(dirname(bin), { recursive: true });
        symlinkSync(installPi(`node_modules/${PI}`, { name: PI, version: "1.0.0" }), bin);
        return { entryScript: bin };
      },
      { version: "1.0.0", configDir: ".pi" },
    ],
    [
      "a fork with its own name and config directory",
      () => ({
        entryScript: installPi("fork", {
          name: "@acme/agent",
          version: "3.2.1",
          piConfig: { configDir: ".acme" },
        }),
      }),
      { version: "3.2.1", configDir: ".acme" },
    ],
    [
      "a PI_PACKAGE_DIR override",
      () => {
        installPi("store/pi", { name: PI, version: "1.0.0" });
        return { packageDir: join(root, "store", "pi"), entryScript: join(root, "store", "bin") };
      },
      { version: "1.0.0", configDir: ".pi" },
    ],
    [
      "a Bun-compiled binary with package.json beside it",
      () => {
        writeJson(join(root, "pi-linux-x64", "package.json"), { name: PI, version: "1.0.0" });
        return { entryScript: "/$bunfs/root/pi", executable: join(root, "pi-linux-x64", "pi") };
      },
      { version: "1.0.0", configDir: ".pi" },
    ],
    [
      "an SDK application that depends on Pi",
      () => {
        writeJson(join(root, "app", "package.json"), { name: "app", version: "9.9.9" });
        const pi = installPi(`app/node_modules/${PI}`, { name: PI, version: "0.99.2" });
        return { entryScript: join(root, "app", "main.js"), resolvePackage: () => pi };
      },
      { version: "0.99.2", configDir: ".pi" },
    ],
    [
      "no discoverable Pi",
      () => ({ entryScript: join(root, "missing.js") }),
      { version: undefined, configDir: ".pi" },
    ],
  ])("finds %s", (_name, setup, expected) => {
    expect(lookup(setup())).toEqual(expected);
  });

  it("resolves the installed Pi development dependency through ESM conditions", () => {
    const metadata = loadPiPackageMetadata({ entryScript: join(root, "missing.js") });
    expect(metadata.version).toMatch(/^\d+\.\d+\.\d+/);
  });
});
