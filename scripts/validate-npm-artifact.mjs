#!/usr/bin/env node
// Validate the exact npm artifact assembled under dist, without rebuilding it.
import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"
import { resolve } from "node:path"

const [agent, targetArg] = process.argv.slice(2)
if (!agent || !targetArg || !["pi", "opencode"].includes(agent)) {
  throw new Error("usage: validate-npm-artifact.mjs <pi|opencode> <TARGET_DIR>")
}
const target = resolve(targetArg)
const packOutput = execFileSync("pnpm", ["pack", "--dry-run", "--json"], {
  cwd: target,
  encoding: "utf8",
})
const parsed = JSON.parse(packOutput)
const result = Array.isArray(parsed) ? parsed[0] : parsed
const files = new Set(result.files.map((file) => file.path))
const required = ["dist/index.mjs", "dist/index.d.mts", "README.md", "LICENSE"]
if (agent === "opencode") required.push("dist/tracing.mjs", "dist/tracing.d.mts")
for (const file of required) {
  if (!files.has(file)) throw new Error(`package omits ${file}`)
}
for (const file of files) {
  if (file.startsWith("src/") || file.endsWith("daemon-client.ts")) {
    throw new Error(`package exposes generated source: ${file}`)
  }
}

const manifest = JSON.parse(readFileSync(resolve(target, "package.json"), "utf8"))
if (agent === "opencode" && manifest.exports?.["./tracing"]?.import !== "./dist/tracing.mjs") {
  throw new Error("OpenCode package does not export its trace-only managed entrypoint")
}
if (agent === "opencode") {
  const pending = ["dist/tracing.mjs"]
  const visited = new Set()
  let tracingSource = ""
  while (pending.length > 0) {
    const file = pending.pop()
    if (visited.has(file)) continue
    visited.add(file)
    const source = readFileSync(resolve(target, file), "utf8")
    tracingSource += `\n${source}`
    for (const match of source.matchAll(/from\s+["'](\.\/[^"']+\.mjs)["']/g)) {
      pending.push(`dist/${match[1].slice(2)}`)
    }
  }
  for (const marker of ["braintrust_list_projects", "braintrust_query_logs", "--prefer-profile"]) {
    if (tracingSource.includes(marker)) {
      throw new Error(`OpenCode trace-only entrypoint bundles data tools: ${marker}`)
    }
  }
}
for (const field of ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]) {
  if (manifest[field]?.braintrust) throw new Error("package depends on the Braintrust JavaScript SDK")
}
console.log(`Validated ${agent} npm artifact (${files.size} files)`)
