import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { mkdtempSync, rmSync } from "node:fs"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { describe, test } from "node:test"
import {
  DaemonClient,
  daemonSocketPath,
  resolveDaemonTraceSettings,
} from "../src/index.ts"

describe("daemonSocketPath", () => {
  test("prefers the explicit environment override", () => {
    assert.equal(daemonSocketPath({ BT_DAEMON_SOCKET: "/tmp/custom.sock" }), "/tmp/custom.sock")
  })

  test("matches the Unix runtime-directory contract", () => {
    if (process.platform === "win32") return
    assert.equal(
      daemonSocketPath({ XDG_RUNTIME_DIR: "/run/user/1" }),
      "/run/user/1/braintrust/daemon.sock",
    )
  })

  test("documents the Windows identity hash contract", () => {
    const identity = "ACME\\alice"
    assert.equal(createHash("sha256").update(identity).digest("hex").slice(0, 16).length, 16)
  })
})

describe("resolveDaemonTraceSettings", () => {
  const persistent = {
    trace_to_braintrust: true,
    route: {
      auth: { profile: "global" },
      destination: { type: "project_logs", project_name: "global-project" },
    },
  }

  test("keeps one agent's persistent selection without a managed run", () => {
    assert.deepEqual(resolveDaemonTraceSettings(persistent, {}), persistent)
  })

  test("uses an invocation selection without mutating persistent settings", () => {
    const invocation = {
      trace_to_braintrust: true,
      route: {
        auth: { profile: "run" },
        destination: { type: "project_logs", project_name: "run-project" },
      },
    }
    assert.deepEqual(
      resolveDaemonTraceSettings(persistent, {
        BT_TRACE_INVOCATION_SETTINGS: JSON.stringify(invocation),
      }),
      invocation,
    )
    assert.equal(persistent.route.auth.profile, "global")
  })

  test("does not fall back when invocation settings are malformed", () => {
    assert.deepEqual(
      resolveDaemonTraceSettings(persistent, { BT_TRACE_INVOCATION_SETTINGS: "{" }),
      { trace_to_braintrust: false },
    )
  })
})

test("serializes initialize, events, flush, and status over one connection", async () => {
  const temp = mkdtempSync(join(tmpdir(), "bt-js-client-"))
  const endpoint = process.platform === "win32"
    ? `\\\\.\\pipe\\bt-js-client-${process.pid}-${Date.now()}`
    : join(temp, "daemon.sock")
  const methods: string[] = []
  const eventParams: Array<Record<string, unknown>> = []
  const server = createServer((socket) => {
    socket.setEncoding("utf8")
    let input = ""
    socket.on("data", (chunk: string) => {
      input += chunk
      while (input.includes("\n")) {
        const newline = input.indexOf("\n")
        const request = JSON.parse(input.slice(0, newline))
        input = input.slice(newline + 1)
        methods.push(request.method)
        if (request.method === "event.log") eventParams.push(request.params)
        const result = request.method === "initialize"
          ? { protocol_version: 1, capabilities: { sources: ["opencode"] } }
          : request.method === "event.log"
            ? { accepted: true }
            : request.method === "session.flush"
              ? { flushed: true, pending: 0 }
              : { daemon_version: "test", uptime_ms: 1, sessions: [] }
        socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result })}\n`)
      }
    })
  })
  await new Promise<void>((resolve, reject) => server.listen(endpoint, resolve).once("error", reject))
  const client = new DaemonClient({
    source: "opencode",
    pluginVersion: "1.0.0",
    managedRunId: "run-123",
    socketPath: endpoint,
  })
  const envelope = (name: string) => ({
    source: "opencode",
    session_id: "session",
    event: name,
    ts_ms: Date.now(),
    payload: {},
    route: { destination: {}, span_plugins: ["plugin.mjs"] },
  })
  assert.deepEqual(await Promise.all([client.log(envelope("one")), client.log(envelope("two"))]), [
    true,
    true,
  ])
  assert.equal(await client.flush("session"), true)
  assert.equal((await client.status("session"))?.daemon_version, "test")
  assert.deepEqual(methods, [
    "initialize",
    "event.log",
    "event.log",
    "session.flush",
    "status.get",
  ])
  assert.deepEqual(eventParams.map((params) => params.plugin_version), ["1.0.0", "1.0.0"])
  assert.deepEqual(eventParams.map((params) => params.managed_run_id), ["run-123", "run-123"])
  await client.close()
  await new Promise<void>((resolve) => server.close(() => resolve()))
  rmSync(temp, { recursive: true, force: true })
})

test("fails open when the daemon and bt executable are absent", async () => {
  const warnings: string[] = []
  const endpoint = process.platform === "win32"
    ? `\\\\.\\pipe\\bt-js-client-missing-${process.pid}-${Date.now()}`
    : join(tmpdir(), `bt-js-client-missing-${process.pid}-${Date.now()}.sock`)
  const client = new DaemonClient({
    source: "opencode",
    socketPath: endpoint,
    btExecutable: `bt-does-not-exist-${Date.now()}`,
    connectAttempts: 1,
    connectDelayMs: 1,
    warn: (message) => warnings.push(message),
  })
  assert.equal(
    await client.log({
      source: "opencode",
      session_id: "session",
      event: "session.created",
      ts_ms: Date.now(),
      payload: {},
    }),
    false,
  )
  assert.ok(warnings.length > 0)
  await client.close()
})
