import { spawn } from "node:child_process"
import { createHash } from "node:crypto"
import { createConnection, type Socket } from "node:net"
import { homedir } from "node:os"
import { join } from "node:path"

export const DAEMON_PROTOCOL_VERSION = 1

export interface DaemonSessionRoute {
  auth?: {
    /** Immutable saved-profile identifier. Preferred over legacy `profile`. */
    profile_id?: string
    /** Mutable display name retained to read settings written before profile IDs. */
    profile?: string
    org_name?: string
  }
  destination: unknown
  flush_mode?: "fire_and_forget" | "flush_on_turn_end"
  additional_metadata?: Record<string, unknown>
  span_plugins?: string[]
}

export interface DaemonTraceSettings {
  trace_to_braintrust?: boolean
  route?: DaemonSessionRoute
}

const MANAGED_INSTANCE_CLAIMS = Symbol.for("braintrust.coding-agent.managed-instance-claims")

/** Keep one live forwarding adapter when a managed run loads both its
 * persistent plugin and its invocation-local plugin. Unmanaged plugin
 * instances are deliberately unaffected. */
export function claimManagedTracingInstance(
  source: string,
  env: NodeJS.ProcessEnv = process.env,
  target: typeof globalThis = globalThis,
): boolean {
  const managedRunId = env.BT_TRACE_MANAGED_RUN_ID
  if (!managedRunId) return true
  const globals = target as typeof globalThis & {
    [MANAGED_INSTANCE_CLAIMS]?: Set<string>
  }
  const claims = (globals[MANAGED_INSTANCE_CLAIMS] ??= new Set<string>())
  const key = `${source}:${managedRunId}`
  if (claims.has(key)) return false
  claims.add(key)
  return true
}

/**
 * Apply the invocation-only selection created by `bt trace run` without
 * mutating or falling back to an agent's persistent configuration.
 */
export function resolveDaemonTraceSettings(
  persistent: DaemonTraceSettings,
  env: NodeJS.ProcessEnv = process.env,
): DaemonTraceSettings {
  const raw = env.BT_TRACE_INVOCATION_SETTINGS
  if (raw === undefined) return persistent
  try {
    const invocation = JSON.parse(raw) as DaemonTraceSettings
    if (
      invocation.trace_to_braintrust !== true ||
      !invocation.route ||
      invocation.route.destination === undefined
    ) {
      return { trace_to_braintrust: false }
    }
    return invocation
  } catch {
    return { trace_to_braintrust: false }
  }
}

export interface DaemonEnvelope {
  source: string
  source_version?: string
  plugin_version?: string
  session_id: string
  event: string
  ts_ms: number
  managed_run_id?: string
  payload: unknown
  plugin_env?: Record<string, string>
  route?: DaemonSessionRoute
}

export interface DaemonSessionStatus {
  session_id: string
  source: string
  queued: number
  spans_emitted: number
  permalink?: string
  last_error?: string
}

export interface DaemonStatus {
  daemon_version: string
  uptime_ms: number
  sessions: DaemonSessionStatus[]
}

export interface DaemonClientOptions {
  source: string
  pluginVersion?: string
  socketPath?: string
  btExecutable?: string
  startArguments?: string[]
  connectAttempts?: number
  connectDelayMs?: number
  requestTimeoutMs?: number
  managedRunId?: string
  warn?: (message: string) => void
}

interface RpcResponse {
  jsonrpc: "2.0"
  id: number
  result?: unknown
  error?: { code: number; message: string; data?: unknown }
}

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
  timer: ReturnType<typeof setTimeout>
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export function daemonSocketPath(env: NodeJS.ProcessEnv = process.env): string {
  if (env.BT_DAEMON_SOCKET) return env.BT_DAEMON_SOCKET
  if (process.platform === "win32") {
    const identity = `${env.USERDOMAIN ?? ""}\\${env.USERNAME ?? ""}`
    const suffix = createHash("sha256").update(identity).digest("hex").slice(0, 16)
    return `\\\\.\\pipe\\braintrust-bt-daemon-${suffix}`
  }
  if (env.XDG_RUNTIME_DIR) return join(env.XDG_RUNTIME_DIR, "braintrust", "daemon.sock")
  return join(env.HOME ?? env.USERPROFILE ?? homedir(), ".braintrust", "run", "daemon.sock")
}

export class DaemonClient {
  private readonly options: Required<
    Pick<
      DaemonClientOptions,
      "source" | "btExecutable" | "connectAttempts" | "connectDelayMs" | "requestTimeoutMs"
    >
  > &
    DaemonClientOptions
  private socket?: Socket
  private input = ""
  private nextId = 1
  private pending = new Map<number, PendingRequest>()
  private connecting?: Promise<void>
  private queue: Promise<unknown> = Promise.resolve()
  private warned = new Set<string>()

  constructor(options: DaemonClientOptions) {
    this.options = {
      btExecutable: "bt",
      connectAttempts: 50,
      connectDelayMs: 20,
      requestTimeoutMs: 10_000,
      managedRunId: process.env.BT_TRACE_MANAGED_RUN_ID,
      ...options,
    }
  }

  async log(envelope: DaemonEnvelope): Promise<boolean> {
    return this.serial(async () => {
      const event = {
        ...envelope,
        ...(envelope.plugin_env
          ? { plugin_env: envelope.plugin_env }
          : envelope.route?.span_plugins?.length
            ? {
                plugin_env: Object.fromEntries(
                  Object.entries(process.env).filter(
                    (entry): entry is [string, string] => entry[1] !== undefined,
                  ),
                ),
              }
            : {}),
        ...(this.options.pluginVersion ? { plugin_version: this.options.pluginVersion } : {}),
        ...(this.options.managedRunId ? { managed_run_id: this.options.managedRunId } : {}),
      }
      try {
        const result = (await this.request("event.log", event)) as { accepted?: boolean }
        return result.accepted === true
      } catch (error) {
        this.disconnect(error)
        try {
          const result = (await this.request("event.log", event)) as { accepted?: boolean }
          return result.accepted === true
        } catch (retryError) {
          this.warnOnce(`event:${String(retryError)}`)
          return false
        }
      }
    })
  }

  async flush(sessionId: string, timeoutMs = 10_000): Promise<boolean> {
    return this.serial(async () => {
      try {
        const result = (await this.request("session.flush", {
          session_id: sessionId,
          timeout_ms: timeoutMs,
        })) as { flushed?: boolean }
        return result.flushed === true
      } catch (error) {
        this.warnOnce(`flush:${String(error)}`)
        return false
      }
    })
  }

  async status(sessionId?: string): Promise<DaemonStatus | undefined> {
    return this.serial(async () => {
      try {
        return (await this.request(
          "status.get",
          sessionId ? { session_id: sessionId } : {},
        )) as DaemonStatus
      } catch (error) {
        this.warnOnce(`status:${String(error)}`)
        return undefined
      }
    })
  }

  async close(): Promise<void> {
    await this.queue
    this.disconnect(new Error("daemon client closed"))
  }

  private serial<T>(operation: () => Promise<T>): Promise<T> {
    const next = this.queue.then(operation, operation)
    this.queue = next.then(
      () => undefined,
      () => undefined,
    )
    return next
  }

  private async request(method: string, params: unknown): Promise<unknown> {
    await this.ensureConnected()
    const socket = this.socket
    if (!socket || socket.destroyed) throw new Error("daemon socket is unavailable")

    const id = this.nextId++
    const response = new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`daemon request timed out: ${method}`))
      }, this.options.requestTimeoutMs)
      this.pending.set(id, { resolve, reject, timer })
    })
    socket.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`)
    return response
  }

  private async ensureConnected(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return
    if (!this.connecting) {
      this.connecting = this.connectWithStartup().finally(() => {
        this.connecting = undefined
      })
    }
    return this.connecting
  }

  private async connectWithStartup(): Promise<void> {
    try {
      await this.connectOnce()
    } catch {
      this.startDaemon()
      let lastError: unknown
      for (let attempt = 0; attempt < this.options.connectAttempts; attempt++) {
        await sleep(this.options.connectDelayMs)
        try {
          await this.connectOnce()
          return
        } catch (error) {
          lastError = error
        }
      }
      throw new Error(`could not connect to Braintrust tracing daemon: ${String(lastError)}`)
    }
  }

  private connectOnce(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = createConnection(this.options.socketPath ?? daemonSocketPath())
      const onError = (error: Error) => {
        socket.destroy()
        reject(error)
      }
      socket.once("error", onError)
      socket.once("connect", async () => {
        socket.off("error", onError)
        this.attach(socket)
        try {
          const result = (await this.requestOnConnectedSocket("initialize", {
            protocol_version: DAEMON_PROTOCOL_VERSION,
            client: {
              source: this.options.source,
              plugin_version: this.options.pluginVersion,
              pid: process.pid,
            },
          })) as {
            protocol_version: number
            capabilities?: { sources?: string[] }
          }
          if (result.protocol_version !== DAEMON_PROTOCOL_VERSION) {
            throw new Error(`unsupported daemon protocol ${result.protocol_version}`)
          }
          if (!result.capabilities?.sources?.includes(this.options.source)) {
            throw new Error(`daemon does not support ${this.options.source}; update bt`)
          }
          resolve()
        } catch (error) {
          this.disconnect(error)
          reject(error)
        }
      })
    })
  }

  private requestOnConnectedSocket(method: string, params: unknown): Promise<unknown> {
    const socket = this.socket
    if (!socket) return Promise.reject(new Error("daemon socket is unavailable"))
    const id = this.nextId++
    const response = new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`daemon request timed out: ${method}`))
      }, this.options.requestTimeoutMs)
      this.pending.set(id, { resolve, reject, timer })
    })
    socket.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`)
    return response
  }

  private attach(socket: Socket): void {
    this.socket = socket
    this.input = ""
    socket.setEncoding("utf8")
    socket.on("data", (chunk: string) => this.onData(chunk))
    socket.on("error", (error) => this.disconnect(error))
    socket.on("close", () => this.disconnect(new Error("daemon connection closed")))
  }

  private onData(chunk: string): void {
    this.input += chunk
    while (true) {
      const newline = this.input.indexOf("\n")
      if (newline < 0) return
      const line = this.input.slice(0, newline)
      this.input = this.input.slice(newline + 1)
      if (!line) continue
      let response: RpcResponse
      try {
        response = JSON.parse(line) as RpcResponse
      } catch {
        continue
      }
      const pending = this.pending.get(response.id)
      if (!pending) continue
      this.pending.delete(response.id)
      clearTimeout(pending.timer)
      if (response.error) pending.reject(new Error(response.error.message))
      else pending.resolve(response.result)
    }
  }

  private startDaemon(): void {
    try {
      const child = spawn(
        this.options.btExecutable,
        this.options.startArguments ?? ["trace", "daemon"],
        { detached: true, stdio: "ignore", windowsHide: true },
      )
      child.once("error", (error) => this.warnOnce(`start:${String(error)}`))
      child.unref()
    } catch (error) {
      this.warnOnce(`start:${String(error)}`)
    }
  }

  private disconnect(reason: unknown): void {
    const socket = this.socket
    this.socket = undefined
    if (socket && !socket.destroyed) socket.destroy()
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(reason instanceof Error ? reason : new Error(String(reason)))
    }
    this.pending.clear()
  }

  private warnOnce(message: string): void {
    if (this.warned.has(message)) return
    this.warned.add(message)
    this.options.warn?.(message)
  }
}
