// Read the package version at build/bundle time so package.json remains the
// single source of truth for span origin provenance.
import manifest from "../package.json" with { type: "json" }

export const PLUGIN_VERSION: string = manifest.version
