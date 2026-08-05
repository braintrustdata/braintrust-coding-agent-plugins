/**
 * SpanQueue - bounded async queue for non-blocking span delivery to Braintrust
 *
 * Spans are enqueued synchronously (fire-and-forget from the caller's perspective)
 * and drained one-at-a-time by a background worker. When the queue is full the
 * newest span is dropped and an error is logged.
 */

import type { SpanData } from "./client"
import type { SpanSink } from "./span-sink"

export const DEFAULT_QUEUE_SIZE = 1024
const FLUSH_TIMEOUT_MS = 10_000

export type LogFn = (msg: string, data?: unknown) => void

export class SpanQueue {
  private queue: SpanData[] = []
  private maxSize: number
  private sink: SpanSink
  private log: LogFn
  private running = false
  // biome-ignore lint/correctness/noUnusedPrivateClassMembers: intentionally held to prevent GC of background worker promise
  private workerPromise: Promise<void> | null = null
  // Notify the worker that a new item is available
  private notify: (() => void) | null = null

  constructor(sink: SpanSink, maxSize = DEFAULT_QUEUE_SIZE, log: LogFn = () => {}) {
    this.sink = sink
    this.maxSize = maxSize
    this.log = log
  }

  /**
   * Start the background worker. Safe to call multiple times.
   */
  start(): void {
    if (this.running) return
    this.running = true
    this.workerPromise = this._worker()
  }

  /**
   * Enqueue a span for delivery. Returns true if accepted, false if dropped (queue full).
   * Never throws, never awaits network I/O.
   */
  enqueue(span: SpanData): boolean {
    if (this.queue.length >= this.maxSize) {
      this.log("SpanQueue full, dropping span", {
        queueSize: this.queue.length,
        maxSize: this.maxSize,
        spanId: span.span_id,
      })
      return false
    }
    this.queue.push(span)
    // Wake the worker if it's waiting
    this.notify?.()
    return true
  }

  /**
   * Drain all queued spans (up to flushTimeoutMs). Call before process exit.
   */
  async flush(timeoutMs = FLUSH_TIMEOUT_MS): Promise<void> {
    if (this.queue.length === 0) return

    const deadline = Date.now() + timeoutMs
    while (this.queue.length > 0 && Date.now() < deadline) {
      const span = this.queue.shift()!
      try {
        await this.sink.insertSpan(span)
      } catch (e) {
        this.log("SpanQueue flush: error inserting span", { error: String(e) })
      }
    }

    if (this.queue.length > 0) {
      this.log("SpanQueue flush: timed out, dropping remaining spans", {
        remaining: this.queue.length,
      })
    }
  }

  /**
   * Stop the background worker and flush remaining spans.
   */
  async stop(timeoutMs = FLUSH_TIMEOUT_MS): Promise<void> {
    this.running = false
    // Wake the worker so it can exit its wait loop
    this.notify?.()
    // Drain whatever is left synchronously within the timeout
    await this.flush(timeoutMs)
  }

  /**
   * Number of spans currently waiting in the queue.
   */
  get size(): number {
    return this.queue.length
  }

  private async _worker(): Promise<void> {
    while (this.running || this.queue.length > 0) {
      if (this.queue.length === 0) {
        // Wait for a notification or a short poll interval
        await new Promise<void>((resolve) => {
          this.notify = resolve
          // Also wake up periodically in case notify was missed
          setTimeout(resolve, 100)
        })
        this.notify = null
        continue
      }

      const span = this.queue.shift()!
      try {
        await this.sink.insertSpan(span)
      } catch (e) {
        this.log("SpanQueue worker: error inserting span", {
          error: String(e),
          spanId: span.span_id,
        })
      }
    }
  }
}
