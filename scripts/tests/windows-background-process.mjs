#!/usr/bin/env node

// From the repository root, after the OpenCode package dependencies and generated
// clients are prepared: node scripts/tests/windows-background-process.mjs
// Requires Windows, Node 24+, and rustc on PATH; never contacts a daemon or API.
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, readFile, rm } from "node:fs/promises"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import { promisify } from "node:util"

const execFileAsync = promisify(execFile)
const script = fileURLToPath(import.meta.url)
const repoRoot = dirname(dirname(dirname(script)))
const json = async (file) => JSON.parse(await readFile(file, "utf8"))

async function exercise(directory) {
  const { DaemonClient, BtCliToolsClient, spawnBackground } = await import(
    pathToFileURL(join(directory, "consumers.mjs")).href
  )
  const probe = join(directory, "console probe.exe")
  const deadline = setTimeout(() => {
    console.error("Windows background-process exercise exceeded 15s deadline")
    process.exit(1)
  }, 15_000)
  deadline.unref()

  delete process.env.BT_CONSOLE_PROBE_REPORT
  delete process.env.BT_CONSOLE_PROBE_MODE
  try {
    assert.deepEqual(await json(join(directory, "native-parent.json")), { console_window: false })
    const parent = await execFileAsync(probe, ["--check-parent", String(process.pid)], {
      windowsHide: true,
      timeout: 5_000,
    })
    assert.deepEqual(JSON.parse(parent.stdout), { attached: false, error: 6 },
      "JavaScript exercise must have no console before invoking production consumers")

    // Unsuppressed control uses the same pipe-backed launch as execFileBackground.
    // Without a parent console, Windows must allocate one for this console binary.
    const control = await execFileAsync(probe, ["projects", "list", "--json", "--no-input"], {
      windowsHide: false,
      timeout: 5_000,
    })
    assert.equal(JSON.parse(control.stdout)[0].console_window, true,
      "negative control failed: GetConsoleWindow did not detect an unsuppressed console")

    process.env.BT_EXECUTABLE = probe
    process.env.BT_CONSOLE_PROBE_REPORT = join(directory, "cli success.json")
    delete process.env.BT_CONSOLE_PROBE_MODE
    const tools = new BtCliToolsClient({
      projectName: "café 雪",
      tracingEnabled: false,
      enableTools: true,
      debug: false,
      route: { destination: { type: "project_logs", project_name: "unused" } },
    })
    assert.deepEqual(await tools.listProjects(), [
      { id: "probe", name: "café 雪", console_window: false },
    ], "real OpenCode default runner must preserve JSON/UTF-8 without allocating a console")
    assert.deepEqual(await json(process.env.BT_CONSOLE_PROBE_REPORT), {
      console_window: false, exit_code: 0,
    })

    process.env.BT_CONSOLE_PROBE_REPORT = join(directory, "cli failure.json")
    process.env.BT_CONSOLE_PROBE_MODE = "fail"
    await assert.rejects(tools.listProjects(), /bt CLI command failed:.*intentional probe failure: café 雪/s)
    assert.deepEqual(await json(process.env.BT_CONSOLE_PROBE_REPORT), {
      console_window: false, exit_code: 23,
    }, "nonzero CLI status must reject with its meaningful stderr intact")
    delete process.env.BT_CONSOLE_PROBE_MODE

    // DaemonClient's production startup uses detached + ignored stdio + unref.
    // No server is needed: failed status connects force that path, and the native
    // executable records its console state independently of ignored output.
    const daemonReport = join(directory, "daemon startup.json")
    process.env.BT_CONSOLE_PROBE_REPORT = daemonReport
    const warnings = []
    const client = new DaemonClient({
      source: "opencode",
      btExecutable: probe,
      startArguments: ["--daemon"],
      socketPath: `\\\\.\\pipe\\bt-console-probe-${process.pid}-${Date.now()}`,
      connectAttempts: 100,
      connectDelayMs: 20,
      requestTimeoutMs: 1_000,
      warn: (message) => warnings.push(message),
    })
    try {
      assert.equal(await client.status(), undefined)
      assert.deepEqual(await json(daemonReport), { console_window: false, exit_code: 0 },
        "real DaemonClient startup must not allocate a console")
      assert.equal(warnings.some((message) => message.startsWith("start:")), false,
        "daemon fixture must have launched successfully")
    } finally {
      await client.close()
    }

    // Exercise piped input/output as well: forcing the policy must not alter
    // streams, UTF-8, argument boundaries, or opt callers into shell parsing.
    process.env.BT_CONSOLE_PROBE_REPORT = join(directory, "stream echo.json")
    const input = '{"message":"café 雪 & | < > ^ % !"}\n'
    const child = spawnBackground(probe, ["--echo"], {
      stdio: "pipe", windowsHide: false, shell: true, timeout: 5_000,
    })
    try {
      const result = await new Promise((resolve, reject) => {
        let stdout = ""
        let stderr = ""
        child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk })
        child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk })
        child.once("error", reject)
        child.once("close", (code) => resolve({ code, stdout, stderr }))
        child.stdin.on("error", reject)
        child.stdin.end(input)
      })
      assert.deepEqual(result, { code: 0, stdout: input, stderr: "probe stderr: café 雪\n" })
      assert.deepEqual(await json(process.env.BT_CONSOLE_PROBE_REPORT), {
        console_window: false, exit_code: 0,
      })
    } finally {
      if (child.exitCode === null) child.kill()
    }
    console.log("PASS: detached native/Node parents, console-positive control, DaemonClient startup, OpenCode success/failure, and piped UTF-8 I/O")
  } finally {
    clearTimeout(deadline)
  }
}

async function run() {
  assert.equal(process.platform, "win32", "This is a real Windows console test, not a platform mock")
  assert.ok(Number(process.versions.node.split(".")[0]) >= 24, "Node 24+ is required")
  const directory = await mkdtemp(join(tmpdir(), "braintrust console probe "))
  try {
    // Use the package's installed bundler to resolve source TypeScript imports;
    // do not expose private consumers through the published package API.
    const require = createRequire(join(repoRoot, "src/plugins/opencode/content/package.json"))
    const { build } = require("vite-plus")
    await build({
      configFile: false,
      root: repoRoot,
      logLevel: "warn",
      build: {
        ssr: join(repoRoot, "scripts/tests/windows-background-consumers.ts"),
        outDir: directory,
        emptyOutDir: false,
        minify: false,
        rollupOptions: { output: { entryFileNames: "consumers.mjs" } },
      },
    })
    const probe = join(directory, "console probe.exe")
    await execFileAsync("rustc", [
      "--edition=2021", join(repoRoot, "scripts/tests/windows-console-probe.rs"), "-o", probe,
    ], { windowsHide: true, timeout: 60_000 })
    let failure
    try {
      await execFileAsync(probe, ["--launch", directory, process.execPath, script, "--exercise", directory], {
        windowsHide: true,
        timeout: 40_000,
        env: {
          ...process.env,
          BT_CONSOLE_PROBE_MODE: "",
          BT_CONSOLE_PROBE_REPORT: "",
        },
      })
    } catch (error) {
      failure = error
    }
    for (const [name, stream] of [["exercise.stdout", process.stdout], ["exercise.stderr", process.stderr]]) {
      try {
        stream.write(await readFile(join(directory, name), "utf8"))
      } catch (error) {
        if (error.code !== "ENOENT") throw error
      }
    }
    if (failure) throw failure
  } finally {
    await rm(directory, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 })
  }
}

try {
  if (process.argv[2] === "--exercise") await exercise(process.argv[3])
  else await run()
} catch (error) {
  console.error(error)
  process.exitCode = 1
}
