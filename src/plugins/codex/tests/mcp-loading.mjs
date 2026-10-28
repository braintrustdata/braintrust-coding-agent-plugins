// Exercise Codex's MCP parser, which plugin add/list alone does not validate.
// This reads installed plugin metadata; it never signs in or calls MCP tools.
import assert from "node:assert/strict"
import { cp, mkdtemp, mkdir, readFile, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { createInterface } from "node:readline"
import test from "node:test"
import {
  execFileBackground,
  spawnBackground,
} from "../../../runtime/js-daemon-client/src/background-process.ts"

const distribution = resolve(process.argv[2] ?? "dist/codex")
const codex = process.env.CODEX_BIN || "codex"

async function readPlugin(env, cwd, marketplace) {
  const child = spawnBackground(codex, ["app-server", "--stdio"], {
    env,
    cwd,
    stdio: ["pipe", "pipe", "pipe"],
  })
  const pending = new Map()
  let stderr = ""
  child.stderr.setEncoding("utf8").on("data", (data) => { stderr += data })
  const lines = createInterface({ input: child.stdout })
  const fail = (error) => {
    for (const request of pending.values()) request.reject(error)
    pending.clear()
  }
  child.on("error", fail)
  const exited = new Promise((done) => child.once("close", () => {
    fail(new Error(`Codex app-server exited: ${stderr}`))
    done()
  }))
  lines.on("line", (line) => {
    try {
      const message = JSON.parse(line)
      const request = pending.get(message.id)
      if (request) {
        pending.delete(message.id)
        if (message.error) request.reject(new Error(JSON.stringify(message.error)))
        else request.resolve(message.result)
      }
    } catch (error) { fail(error) }
  })
  let id = 0
  async function call(method, params) {
    const requestId = ++id
    let timer
    try {
      return await new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`Timeout: ${method}\n${stderr}`)), 10_000)
        pending.set(requestId, { resolve, reject })
        child.stdin.write(JSON.stringify({ id: requestId, method, params }) + "\n")
      })
    } finally {
      clearTimeout(timer)
      pending.delete(requestId)
    }
  }
  try {
    await call("initialize", {
      clientInfo: { name: "braintrust_mcp_loading_test", version: "1.0.0" },
      capabilities: { experimentalApi: true },
    })
    child.stdin.write(JSON.stringify({ method: "initialized", params: {} }) + "\n")
    const result = await call("plugin/read", {
      marketplacePath: join(marketplace, ".agents/plugins/marketplace.json"),
      pluginName: "braintrust",
    })
    return result.plugin
  } finally {
    const killTimer = setTimeout(() => child.kill("SIGKILL"), 1000)
    killTimer.unref()
    child.kill()
    await exited
    clearTimeout(killTimer)
    lines.close()
  }
}

test("installed Braintrust MCP server loads in Codex", { timeout: 40_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "braintrust-mcp-loading-"))
  try {
    const marketplace = join(root, "marketplace")
    const home = join(root, "codex")
    await mkdir(home)
    await cp(distribution, marketplace, { recursive: true })
    const env = { ...process.env, CODEX_HOME: home }
    // Separate profile and cwd prevent loading user credentials or repo hooks.
    const options = { env, cwd: root, encoding: "utf8", timeout: 10_000 }
    await execFileBackground(codex, ["plugin", "marketplace", "add", marketplace], options)
    const install = await execFileBackground(codex, [
      "plugin", "add", "braintrust@braintrust-codex-plugins", "--json",
    ], options)
    const cached = JSON.parse(install.stdout).installedPath
    const config = JSON.parse(await readFile(join(cached, "mcp.json"), "utf8"))
    assert.equal(config.mcpServers.braintrust.type, "streamable-http")
    const plugin = await readPlugin(env, root, marketplace)
    assert.equal(plugin.summary.installed, true)
    assert.deepEqual(plugin.mcpServers, ["braintrust"], "MCP entry was dropped by Codex's parser")
    assert.deepEqual(plugin.hooks, [])
    assert.deepEqual(plugin.apps, [])
    const listed = await execFileBackground(codex, [
      "plugin", "list", "--marketplace", "braintrust-codex-plugins", "--json",
    ], options)
    assert.deepEqual(JSON.parse(listed.stdout).installed.map((p) => p.pluginId), [
      "braintrust@braintrust-codex-plugins",
    ])
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
