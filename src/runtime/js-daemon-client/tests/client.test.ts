import assert from "node:assert/strict"
import { mkdtempSync, rmSync } from "node:fs"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { describe, test } from "node:test"
import {
  claimManagedTracingInstance,
  DaemonClient,
  daemonSocketPath,
  resolveDaemonTraceSettings,
} from "../src/index.ts"

describe("managed adapter claims", () => {
  test("deduplicates only the same source and managed run", () => {
    const target = {} as typeof globalThis
    assert.equal(claimManagedTracingInstance("pi", {}, target), true)
    assert.equal(claimManagedTracingInstance("pi", {}, target), true)
    const env = { BT_TRACE_MANAGED_RUN_ID: "run-1" }
    assert.equal(claimManagedTracingInstance("pi", env, target), true)
    assert.equal(claimManagedTracingInstance("pi", env, target), false)
    assert.equal(claimManagedTracingInstance("opencode", env, target), true)
    assert.equal(
      claimManagedTracingInstance("pi", { BT_TRACE_MANAGED_RUN_ID: "run-2" }, target),
      true,
    )
  })
})

describe("daemonSocketPath", () => {
  test("prefers the explicit environment override", () => {
    assert.equal(
      daemonSocketPath({ BRAINTRUST_DAEMON_SOCKET: "/tmp/custom.sock" }),
      "/tmp/custom.sock",
    )
  })

  test("still honors the deprecated BT_DAEMON_SOCKET name", () => {
    assert.equal(daemonSocketPath({ BT_DAEMON_SOCKET: "/tmp/old.sock" }), "/tmp/old.sock")
    assert.equal(
      daemonSocketPath({ BRAINTRUST_DAEMON_SOCKET: "/tmp/new.sock", BT_DAEMON_SOCKET: "/tmp/old.sock" }),
      "/tmp/new.sock",
    )
  })

  test("matches the Unix runtime-directory contract", () => {
    if (process.platform === "win32") return
    assert.equal(
      daemonSocketPath({ XDG_RUNTIME_DIR: "/run/user/1" }),
      "/run/user/1/braintrust/daemon.sock",
    )
  })

  // Shared vectors with bt-daemon/src/paths.rs; both sides must derive the
  // same per-user pipe or Windows hooks silently miss the daemon.
  test("matches the daemon's Windows named-pipe contract", () => {
    assert.equal(
      daemonSocketPath({ USERDOMAIN: "ACME", USERNAME: "alice" }, "win32"),
      "\\\\.\\pipe\\braintrust-bt-daemon-d2c39c8776cb1638",
    )
    assert.equal(
      daemonSocketPath({}, "win32"),
      "\\\\.\\pipe\\braintrust-bt-daemon-a9253dc8529dd214",
    )
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

test("pipelines ordered events before receiving daemon acknowledgements", async () => {
  const temp = mkdtempSync(join(tmpdir(), "bt-js-pipeline-"))
  const endpoint = process.platform === "win32"
    ? `\\\\.\\pipe\\bt-js-pipeline-${process.pid}-${Date.now()}`
    : join(temp, "daemon.sock")
  const received: string[] = []
  const pending: Array<{ id: number; socket: import("node:net").Socket }> = []
  const server = createServer((socket) => {
    socket.setEncoding("utf8")
    let input = ""
    socket.on("data", (chunk: string) => {
      input += chunk
      while (input.includes("\n")) {
        const newline = input.indexOf("\n")
        const request = JSON.parse(input.slice(0, newline))
        input = input.slice(newline + 1)
        if (request.method === "initialize") {
          setTimeout(() => socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result: { protocol_version: 1, capabilities: { sources: ["opencode"] } } })}\n`), 100)
        } else if (request.method === "session.flush") {
          received.push("flush")
          socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result: { flushed: true } })}\n`)
        } else {
          received.push(request.params.event)
          pending.push({ id: request.id, socket })
          if (pending.length === 3) {
            for (const item of pending) item.socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: item.id, result: { accepted: true } })}\n`)
          }
        }
      }
    })
  })
  await new Promise<void>((resolve, reject) => server.listen(endpoint, resolve).once("error", reject))
  const client = new DaemonClient({ source: "opencode", socketPath: endpoint, requestTimeoutMs: 2000 })
  const event = (name: string) => ({ source: "opencode", session_id: "session", event: name, ts_ms: 1, payload: {} })
  const first = client.logPipelined(event("one"))
  await new Promise((resolve) => setTimeout(resolve, 20))
  const second = client.logPipelined(event("two"))
  const third = client.logPipelined(event("three"))
  const flush = client.flush("session")
  assert.deepEqual(await Promise.all([first, second, third]), [true, true, true])
  assert.equal(await flush, true)
  assert.deepEqual(received, ["one", "two", "three", "flush"])
  await client.close()
  await new Promise<void>((resolve) => server.close(() => resolve()))
  rmSync(temp, { recursive: true, force: true })
})

test("retries pipelined events in order after a lost connection", async () => {
  const temp = mkdtempSync(join(tmpdir(), "bt-js-retry-"))
  const endpoint = process.platform === "win32" ? `\\\\.\\pipe\\bt-js-retry-${process.pid}-${Date.now()}` : join(temp, "daemon.sock")
  const received: string[] = []
  let connections = 0
  const server = createServer((socket) => {
    const connection = ++connections
    socket.setEncoding("utf8")
    let input = ""
    socket.on("data", (chunk: string) => {
      input += chunk
      while (input.includes("\n")) {
        const newline = input.indexOf("\n")
        const request = JSON.parse(input.slice(0, newline))
        input = input.slice(newline + 1)
        if (request.method === "initialize") {
          socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result: { protocol_version: 1, capabilities: { sources: ["opencode"] } } })}\n`)
        } else if (connection === 1) {
          socket.destroy()
          return
        } else {
          received.push(request.params.event)
          socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result: { accepted: true } })}\n`)
        }
      }
    })
  })
  await new Promise<void>((resolve, reject) => server.listen(endpoint, resolve).once("error", reject))
  const client = new DaemonClient({ source: "opencode", socketPath: endpoint, requestTimeoutMs: 2000 })
  const event = (name: string) => ({ source: "opencode", session_id: "session", event: name, ts_ms: 1, payload: {} })
  assert.deepEqual(await Promise.all([client.logPipelined(event("one")), client.logPipelined(event("two")), client.logPipelined(event("three"))]), [true, true, true])
  assert.deepEqual(received, ["one", "two", "three"])
  await client.close()
  await new Promise<void>((resolve) => server.close(() => resolve()))
  rmSync(temp, { recursive: true, force: true })
})

test("pipelined events do not report success when daemon initialization is incompatible", async () => {
  const temp = mkdtempSync(join(tmpdir(), "bt-js-version-"))
  const endpoint = process.platform === "win32" ? `\\\\.\\pipe\\bt-js-version-${process.pid}-${Date.now()}` : join(temp, "daemon.sock")
  let eventsReceived = 0
  const server = createServer((socket) => {
    // The client intentionally closes this connection after rejecting the
    // incompatible handshake; a pending server write may then fail on Windows.
    socket.on("error", () => {})
    socket.setEncoding("utf8")
    let input = ""
    socket.on("data", (chunk: string) => {
      input += chunk
      while (input.includes("\n")) {
        const newline = input.indexOf("\n")
        const request = JSON.parse(input.slice(0, newline))
        input = input.slice(newline + 1)
        if (request.method === "event.log") eventsReceived++
        const result = request.method === "initialize" ? { protocol_version: 99, capabilities: { sources: ["opencode"] } } : { accepted: true }
        socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result })}\n`)
      }
    })
  })
  await new Promise<void>((resolve, reject) => server.listen(endpoint, resolve).once("error", reject))
  const warnings: string[] = []
  const client = new DaemonClient({ source: "opencode", socketPath: endpoint, connectAttempts: 1, connectDelayMs: 1, requestTimeoutMs: 500, warn: (warning) => warnings.push(warning) })
  assert.equal(await client.logPipelined({ source: "opencode", session_id: "s", event: "test", ts_ms: 1, payload: {} }), false)
  assert.ok(warnings.length > 0)
  assert.equal(eventsReceived, 0)
  await client.close()
  await new Promise<void>((resolve) => server.close(() => resolve()))
  rmSync(temp, { recursive: true, force: true })
})
