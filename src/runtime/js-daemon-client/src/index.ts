import { spawnBackground } from "./background-process.ts"
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

/** A pipelined event that has not been acknowledged yet. */
interface OutboxEntry {
  params: Record<string, unknown>
  /** Delivery failures attributed to this event; it is dropped at the limit. */
  failures: number
  /** Connection the event was last written to; any other socket needs a resend. */
  socket?: Socket
  settle: (accepted: boolean) => void
}

/** Initial attempt plus one retry, matching `log()`. */
const MAX_EVENT_FAILURES = 2

export function daemonSocketPath(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string {
  // BT_DAEMON_SOCKET is the deprecated name, still set by older `bt trace run`.
  const explicit = env.BRAINTRUST_DAEMON_SOCKET || env.BT_DAEMON_SOCKET
  if (explicit) return explicit
  if (platform === "win32") {
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
  private orderedEventPipeline = false
  private queue: Promise<unknown> = Promise.resolve()
  private outbox: OutboxEntry[] = []
  private outboxConnecting = false
  private inFlightEvents = new Set<Promise<boolean>>()
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
    return this.serial(() => this.sendEvent(envelope))
  }

  /**
   * Submit events in call order without waiting for earlier acknowledgements.
   *
   * All pipelined events share one ordered outbox. When a delivery fails, the
   * failed event and everything written after it are resent in call order on
   * the next connection, so the daemon never journals a later event ahead of
   * an earlier one. Against daemons without `ordered_event_pipeline`, the
   * outbox keeps one event in flight at a time.
   */
  logPipelined(envelope: DaemonEnvelope): Promise<boolean> {
    const request = new Promise<boolean>((settle) => {
      this.outbox.push({ params: this.eventParams(envelope), failures: 0, settle })
    })
    this.inFlightEvents.add(request)
    void request.finally(() => this.inFlightEvents.delete(request))
    this.pumpOutbox()
    return request
  }

  private eventParams(envelope: DaemonEnvelope): Record<string, unknown> {
    return {
      ...envelope,
      ...(this.options.pluginVersion ? { plugin_version: this.options.pluginVersion } : {}),
      ...(this.options.managedRunId ? { managed_run_id: this.options.managedRunId } : {}),
    }
  }

  private pumpOutbox(): void {
    if (this.outboxConnecting || this.outbox.length === 0) return
    const socket = this.socket
    if (!socket || socket.destroyed || this.connecting) {
      this.outboxConnecting = true
      this.ensureConnected().then(
        () => {
          this.outboxConnecting = false
          this.pumpOutbox()
        },
        (error: unknown) => {
          this.outboxConnecting = false
          // Nothing reached a daemon; every queued event used one attempt. Iterate
          // a copy because exhausted entries leave the outbox.
          for (const entry of this.outbox.slice()) this.recordFailure(entry, error)
          this.pumpOutbox()
        },
      )
      return
    }
    for (let index = 0; index < this.outbox.length; index++) {
      const entry = this.outbox[index]!
      if (entry.socket === socket) {
        if (!this.orderedEventPipeline) return
        continue
      }
      if (!this.orderedEventPipeline && entry !== this.outbox[0]) return
      entry.socket = socket
      let request: Promise<unknown>
      try {
        request = this.requestOnConnectedSocket("event.log", entry.params)
      } catch (error) {
        // An unsendable event must not disconnect or block the rest of the outbox.
        this.removeEntry(entry)
        entry.settle(false)
        this.warnOnce(`event:${String(error)}`)
        index--
        continue
      }
      request.then(
        (result) => {
          if ((result as { accepted?: boolean } | undefined)?.accepted === true) {
            this.removeEntry(entry)
            entry.settle(true)
            this.pumpOutbox()
          } else {
            // The daemon rejects while quiescing and then closes the connection.
            this.onOutboxFailure(entry, socket, new Error("daemon did not accept event"))
          }
        },
        (error: unknown) => this.onOutboxFailure(entry, socket, error),
      )
    }
  }

  private onOutboxFailure(entry: OutboxEntry, socket: Socket, error: unknown): void {
    if (entry.socket !== socket || !this.outbox.includes(entry)) return
    // Invalidate every write on this connection before removing its failed head.
    // Followers' rejection callbacks must not charge the newly promoted head.
    for (const queued of this.outbox) {
      if (queued.socket === socket) queued.socket = undefined
    }
    if (this.outbox[0] === entry) this.recordFailure(entry, error)
    // Drop the connection so nothing written after the failure is processed
    // ahead of the resend.
    if (this.socket === socket) this.disconnect(error)
    this.pumpOutbox()
  }

  private recordFailure(entry: OutboxEntry, error: unknown): void {
    entry.failures++
    entry.socket = undefined
    if (entry.failures < MAX_EVENT_FAILURES) return
    this.removeEntry(entry)
    this.warnOnce(`event:${String(error)}`)
    entry.settle(false)
  }

  private removeEntry(entry: OutboxEntry): void {
    const index = this.outbox.indexOf(entry)
    if (index >= 0) this.outbox.splice(index, 1)
  }

  private async sendEvent(envelope: DaemonEnvelope): Promise<boolean> {
    const event = this.eventParams(envelope)
    try {
      const result = (await this.request("event.log", event)) as { accepted?: boolean }
      return result.accepted === true
    } catch (error) {
      if (this.socket) this.disconnect(error)
      try {
        const result = (await this.request("event.log", event)) as { accepted?: boolean }
        return result.accepted === true
      } catch (retryError) {
        this.warnOnce(`event:${String(retryError)}`)
        return false
      }
    }
  }

  async flush(sessionId: string, timeoutMs = 10_000): Promise<boolean> {
    return this.serial(async () => {
      await Promise.all(this.inFlightEvents)
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
    await Promise.all(this.inFlightEvents)
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
    if (this.connecting) return this.connecting
    if (this.socket && !this.socket.destroyed) return
    this.connecting = this.connectWithStartup().finally(() => {
      this.connecting = undefined
    })
    return this.connecting
  }

  private async connectWithStartup(): Promise<void> {
    try {
      await this.connectOnce()
    } catch {
      this.startDaemon()
      let lastError: unknown
      for (let attempt = 0; attempt < this.options.connectAttempts; attempt++) {
        await new Promise((resolve) => setTimeout(resolve, this.options.connectDelayMs))
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
            capabilities?: { sources?: string[]; ordered_event_pipeline?: boolean }
          }
          if (result.protocol_version !== DAEMON_PROTOCOL_VERSION) {
            throw new Error(`unsupported daemon protocol ${result.protocol_version}`)
          }
          if (!result.capabilities?.sources?.includes(this.options.source)) {
            throw new Error(`daemon does not support ${this.options.source}; update bt`)
          }
          this.orderedEventPipeline = result.capabilities?.ordered_event_pipeline === true
          resolve()
        } catch (error) {
          if (this.socket === socket) this.disconnect(error)
          reject(error)
        }
      })
    })
  }

  private requestOnConnectedSocket(method: string, params: unknown): Promise<unknown> {
    const socket = this.socket
    if (!socket) return Promise.reject(new Error("daemon socket is unavailable"))
    const id = this.nextId++
    // Serialize before registering a request, so a throw cannot orphan its timer.
    const message = JSON.stringify({ jsonrpc: "2.0", id, method, params })
    const response = new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`daemon request timed out: ${method}`))
      }, this.options.requestTimeoutMs)
      this.pending.set(id, { resolve, reject, timer })
    })
    socket.write(`${message}\n`)
    return response
  }

  private attach(socket: Socket): void {
    this.socket = socket
    this.input = ""
    socket.setEncoding("utf8")
    socket.on("data", (chunk: string) => this.onData(chunk))
    socket.on("error", (error) => {
      if (this.socket === socket) this.disconnect(error)
    })
    socket.on("close", () => {
      if (this.socket === socket) this.disconnect(new Error("daemon connection closed"))
    })
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
      const child = spawnBackground(
        this.options.btExecutable,
        this.options.startArguments ?? ["trace", "daemon"],
        { detached: true, stdio: "ignore" },
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
