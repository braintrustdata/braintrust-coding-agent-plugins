import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

interface PiPackageManifest {
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
 */
export function loadPiPackageMetadata(): PiPackageMetadata {
  try {
    const entry = createRequire(import.meta.url).resolve("@earendil-works/pi-coding-agent");
    let directory = dirname(entry);
    while (true) {
      try {
        const manifest = JSON.parse(
          readFileSync(join(directory, "package.json"), "utf8"),
        ) as PiPackageManifest;
        return {
          version: typeof manifest.version === "string" ? manifest.version : undefined,
          configDir:
            typeof manifest.piConfig?.configDir === "string" ? manifest.piConfig.configDir : ".pi",
        };
      } catch {
        const parent = dirname(directory);
        if (parent === directory) break;
        directory = parent;
      }
    }
  } catch {
    // Pi supplies the extension API at runtime. Metadata discovery is useful
    // for diagnostics but must not prevent the extension from loading.
  }
  return { configDir: ".pi" };
}
