import { readFileSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PI_PACKAGE = "@earendil-works/pi-coding-agent";
const PI_PACKAGE_NAMES = new Set([PI_PACKAGE, "@mariozechner/pi-coding-agent"]);

interface PiPackageManifest {
  name?: unknown;
  version?: unknown;
  piConfig?: {
    configDir?: unknown;
  };
}

export interface PiPackageMetadata {
  version?: string;
  configDir: string;
}

/**
 * Read Pi's package metadata without importing its root runtime barrel. New Pi
 * releases may add optional entrypoints to that barrel whose dependencies are
 * irrelevant to extensions, so loading it just for VERSION/CONFIG_DIR_NAME can
 * make an otherwise compatible extension fail during module initialization.
 *
 * The running CLI is authoritative, located the way Pi locates itself: an
 * extension installed in its own directory may not share Pi's node_modules.
 * Module resolution covers SDK hosts, where the entry script belongs to the
 * embedding application.
 */
export function loadPiPackageMetadata(candidates: (string | undefined)[]): PiPackageMetadata {
  let manifest: PiPackageManifest | undefined;
  for (const candidate of candidates) {
    manifest = findPiManifest(candidate);
    if (manifest) break;
  }
  return {
    version: typeof manifest?.version === "string" ? manifest.version : undefined,
    configDir:
      typeof manifest?.piConfig?.configDir === "string" ? manifest.piConfig.configDir : ".pi",
  };
}

function findPiManifest(entry: string | undefined): PiPackageManifest | undefined {
  if (!entry) return undefined;
  let directory = dirname(realPath(entry));
  while (true) {
    const manifest = readManifest(join(directory, "package.json"));
    if (
      manifest &&
      ((typeof manifest.name === "string" && PI_PACKAGE_NAMES.has(manifest.name)) ||
        typeof manifest.piConfig === "object")
    ) {
      return manifest;
    }
    const parent = dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
}

function readManifest(path: string): PiPackageManifest | undefined {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as PiPackageManifest;
  } catch {
    return undefined;
  }
}

function realPath(path: string): string {
  try {
    return realpathSync(path);
  } catch {
    return path;
  }
}

function tryResolve(resolve: () => string | undefined): string | undefined {
  try {
    return resolve();
  } catch {
    return undefined;
  }
}

/**
 * Resolve Pi's package entry from this extension's location. Pi supplies the
 * extension API at runtime, so a failed lookup must not prevent loading.
 */
export function resolvePiPackage(): string | undefined {
  // Pi's package exports only an `import` condition, which CommonJS
  // resolution rejects; keep it as a fallback for older or forked releases.
  return (
    tryResolve(() => fileURLToPath(import.meta.resolve(PI_PACKAGE))) ??
    tryResolve(() => createRequire(import.meta.url).resolve(PI_PACKAGE))
  );
}

let runningPi: PiPackageMetadata | undefined;

/** The running Pi's metadata, read once per process. */
export function runningPiPackage(): PiPackageMetadata {
  const packageDir = process.env.PI_PACKAGE_DIR;
  return (runningPi ??= loadPiPackageMetadata([
    // Nix and Guix packages set PI_PACKAGE_DIR.
    packageDir && join(packageDir, "package.json"),
    // For the pi CLI, the script Node is running is a file inside Pi's package.
    process.argv[1],
    // A Bun-compiled Pi binary ships its package.json beside the executable.
    process.execPath,
    resolvePiPackage(),
  ]));
}
