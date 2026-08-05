/**
 * File logger for recording all plugin I/O.
 *
 * Writes newline-delimited JSON (NDJSON) records to a file so that real
 * session data can be captured and used as test fixtures or examined to
 * diagnose tracing bugs.
 *
 * Each record is a JSON object on a single line with the shape:
 *   { ts, kind, hook?, event_type?, data }
 *
 * Records are appended synchronously so the order always matches the order
 * hooks fired, which makes them easier to replay.
 */

import * as fs from "node:fs"
import * as os from "node:os"
import * as path from "node:path"

export type LogKind =
  | "config" // resolved config at startup (api key redacted)
  | "event" // raw OpenCode event received by the event hook
  | "chat.message.input" // messageInput arg to chat.message hook
  | "chat.message.output" // output arg to chat.message hook
  | "chat.system.input" // input arg to experimental.chat.system.transform
  | "chat.system.output" // output arg to experimental.chat.system.transform
  | "tool.before.input" // toolInput arg to tool.execute.before
  | "tool.before.output" // output arg to tool.execute.before
  | "tool.after.input" // toolInput arg to tool.execute.after
  | "tool.after.result" // result arg to tool.execute.after
  | "span" // SpanData sent to Braintrust (after serialization)
  | "error" // any error caught in a hook

export interface FileLogRecord {
  ts: string // ISO timestamp
  kind: LogKind
  hook?: string // which hook fired (event, chat.message, tool.execute.before, …)
  event_type?: string // for kind==="event", the event.type value
  session_id?: string // session ID when available
  data: unknown // the raw payload
}

/**
 * Resolve the log file path.
 *
 * If `logToFile` is "true", "auto", or "1" (case-insensitive) the default
 * path is used: ~/.local/share/opencode/braintrust-io-<date>.ndjson
 *
 * Otherwise the value is treated as a literal file path.
 */
export function resolveLogFilePath(logToFile: string): string {
  const normalized = logToFile.toLowerCase()
  if (normalized === "true" || normalized === "auto" || normalized === "1") {
    const date = new Date().toISOString().slice(0, 10) // YYYY-MM-DD
    return path.join(os.homedir(), ".local", "share", "opencode", `braintrust-io-${date}.ndjson`)
  }
  return logToFile
}

/**
 * A lightweight append-only file logger.
 *
 * Records are written synchronously (appendFileSync) so that even if the
 * process exits abruptly all completed hook calls are captured.  The
 * performance cost is negligible for the low-frequency events we log.
 */
export class FileLogger {
  private filePath: string
  private ready = false

  constructor(logToFile: string) {
    this.filePath = resolveLogFilePath(logToFile)
    this.ensureDir()
  }

  private ensureDir(): void {
    try {
      const dir = path.dirname(this.filePath)
      fs.mkdirSync(dir, { recursive: true })
      this.ready = true
    } catch {
      // If we can't create the directory, logging silently does nothing
      this.ready = false
    }
  }

  /** Write a single record to the log file. Never throws. */
  write(
    kind: LogKind,
    data: unknown,
    extras?: Partial<Omit<FileLogRecord, "ts" | "kind" | "data">>,
  ): void {
    if (!this.ready) return
    try {
      const record: FileLogRecord = {
        ts: new Date().toISOString(),
        kind,
        ...extras,
        data,
      }
      const line = `${JSON.stringify(record)}\n`
      fs.appendFileSync(this.filePath, line, "utf-8")
    } catch {
      // Never let logging errors propagate into hooks
    }
  }

  /** Convenience: log an event hook call */
  logEvent(event: { type: string; properties: unknown }, sessionId?: string): void {
    this.write("event", event.properties, {
      hook: "event",
      event_type: event.type,
      session_id: sessionId,
    })
  }

  /** Convenience: log a chat.message hook input + output pair */
  logChatMessage(messageInput: unknown, output: unknown, sessionId?: string): void {
    this.write("chat.message.input", messageInput, { hook: "chat.message", session_id: sessionId })
    this.write("chat.message.output", output, { hook: "chat.message", session_id: sessionId })
  }

  /** Convenience: log an experimental.chat.system.transform hook input + output pair */
  logChatSystem(input: unknown, output: unknown, sessionId?: string): void {
    this.write("chat.system.input", input, {
      hook: "experimental.chat.system.transform",
      session_id: sessionId,
    })
    this.write("chat.system.output", output, {
      hook: "experimental.chat.system.transform",
      session_id: sessionId,
    })
  }

  /** Convenience: log a tool.execute.before hook args */
  logToolBefore(toolInput: unknown, output: unknown, sessionId?: string): void {
    this.write("tool.before.input", toolInput, {
      hook: "tool.execute.before",
      session_id: sessionId,
    })
    this.write("tool.before.output", output, { hook: "tool.execute.before", session_id: sessionId })
  }

  /** Convenience: log a tool.execute.after hook args */
  logToolAfter(toolInput: unknown, result: unknown, sessionId?: string): void {
    this.write("tool.after.input", toolInput, { hook: "tool.execute.after", session_id: sessionId })
    this.write("tool.after.result", result, { hook: "tool.execute.after", session_id: sessionId })
  }

  /** Log the resolved config (with api key redacted) */
  logConfig(config: Record<string, unknown>): void {
    const safe = { ...config }
    if (safe.apiKey) safe.apiKey = "[REDACTED]"
    this.write("config", safe)
  }

  /** Log an error caught in a hook */
  logError(hook: string, error: unknown, sessionId?: string): void {
    this.write(
      "error",
      {
        message: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      },
      {
        hook,
        session_id: sessionId,
      },
    )
  }

  getFilePath(): string {
    return this.filePath
  }
}

/** Create a FileLogger if logToFile is configured, otherwise return null. */
export function createFileLogger(logToFile: string | undefined): FileLogger | null {
  if (!logToFile) return null
  const normalized = logToFile.toLowerCase()
  // Treat falsy-looking values as disabled
  if (normalized === "false" || normalized === "0" || normalized === "") return null
  return new FileLogger(logToFile)
}
