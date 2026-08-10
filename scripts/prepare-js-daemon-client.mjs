#!/usr/bin/env node

import { mkdir, readFile, stat, writeFile } from "node:fs/promises"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)))
const source = join(repoRoot, "src/runtime/js-daemon-client/src/index.ts")
const destinations = {
  opencode: join(repoRoot, "src/plugins/opencode/content/src/runtime/daemon-client.ts"),
  pi: join(repoRoot, "src/plugins/pi/content/src/runtime/daemon-client.ts"),
}

const args = process.argv.slice(2)
const check = args.includes("--check")
const target = args.find((arg) => !arg.startsWith("--"))

if (!target || !["opencode", "pi", "all"].includes(target)) {
  console.error("usage: prepare-js-daemon-client.mjs <opencode|pi|all> [--check]")
  process.exit(2)
}

const exists = async (path) => {
  try {
    await stat(path)
    return true
  } catch (error) {
    if (error?.code === "ENOENT") return false
    throw error
  }
}

const selected = target === "all" ? Object.entries(destinations) : [[target, destinations[target]]]
const sourceContents = await readFile(source)

for (const [name, destination] of selected) {
  const packageRoot = join(repoRoot, "src/plugins", name, "content")
  if (!(await exists(packageRoot))) {
    if (target === "all") continue
    throw new Error(`plugin content directory does not exist: ${packageRoot}`)
  }

  if (check) {
    let generated
    try {
      generated = await readFile(destination)
    } catch (error) {
      if (error?.code === "ENOENT") {
        throw new Error(`generated daemon client is missing: ${destination}`)
      }
      throw error
    }
    if (!sourceContents.equals(generated)) {
      throw new Error(`generated daemon client differs from the canonical source: ${destination}`)
    }
    console.log(`Verified ${name} daemon client`)
    continue
  }

  await mkdir(dirname(destination), { recursive: true })
  await writeFile(destination, sourceContents)
  console.log(`Prepared ${name} daemon client`)
}
