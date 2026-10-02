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

export interface PiPackageLookup {
  /** Pi's `PI_PACKAGE_DIR` override, used by Nix and Guix packages. */
  packageDir?: string;
  /** The script Node is running; for the pi CLI, a file inside Pi's package. */
  entryScript?: string;
  /** A Bun-compiled Pi binary ships its package.json beside the executable. */
  executable?: string;
  /** Resolve Pi's package entry from this extension's location. */
  resolvePackage?: () => string | undefined;
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
export function loadPiPackageMetadata({
  packageDir = process.env.PI_PACKAGE_DIR,
  entryScript = process.argv[1],
  executable = process.execPath,
  resolvePackage = resolvePiPackage,
}: PiPackageLookup = {}): PiPackageMetadata {
  const manifest =
    findPiManifest(packageDir && join(packageDir, "package.json")) ??
    findPiManifest(entryScript && realPath(entryScript)) ??
    findPiManifest(executable) ??
    findPiManifest(tryResolve(resolvePackage));
  return {
    version: typeof manifest?.version === "string" ? manifest.version : undefined,
    configDir:
      typeof manifest?.piConfig?.configDir === "string" ? manifest.piConfig.configDir : ".pi",
  };
}

function findPiManifest(entry: string | undefined): PiPackageManifest | undefined {
  if (!entry) return undefined;
  let directory = dirname(entry);
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
    // Pi supplies the extension API at runtime. Metadata discovery is useful
    // for diagnostics but must not prevent the extension from loading.
    return undefined;
  }
}

function resolvePiPackage(): string | undefined {
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
  return (runningPi ??= loadPiPackageMetadata());
}
