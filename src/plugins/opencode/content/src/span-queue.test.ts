/**
 * Tests for SpanQueue - bounded async span delivery queue
 */

import { afterEach, describe, expect, it } from "bun:test"
import type { SpanData } from "./client"
import { DEFAULT_QUEUE_SIZE, SpanQueue } from "./span-queue"
import type { SpanSink } from "./span-sink"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeSpan(id: string, isMerge = false): SpanData {
  return {
    id,
    span_id: id,
    root_span_id: "root",
    span_attributes: { name: `span-${id}`, type: "task" },
    _is_merge: isMerge || undefined,
  }
}

class TrackingSink implements SpanSink {
  inserted: SpanData[] = []
  errors: string[] = []
  /** If set, the next N inserts will throw */
  failNext = 0
  /** Artificial delay per insert in ms */
  delayMs = 0

  async insertSpan(span: SpanData): Promise<string | undefined> {
    if (this.delayMs > 0) {
      await new Promise((r) => setTimeout(r, this.delayMs))
    }
    if (this.failNext > 0) {
      this.failNext--
      throw new Error("simulated insert failure")
    }
    this.inserted.push(span)
    return span.span_id
  }

  getSpans() {
    return [...this.inserted]
  }
}

function makeQueue(
  sink: SpanSink,
  maxSize = 8,
  log?: (msg: string, data?: unknown) => void,
): SpanQueue {
  return new SpanQueue(sink, maxSize, log)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SpanQueue", () => {
  const queues: SpanQueue[] = []

  // Stop all queues created in tests so background workers don't leak
  afterEach(async () => {
    for (const q of queues) {
      await q.stop(100)
    }
    queues.length = 0
  })

  function track(q: SpanQueue): SpanQueue {
    queues.push(q)
    return q
  }

  describe("enqueue", () => {
    it("accepts spans up to the max size", () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 4))

      expect(q.enqueue(makeSpan("a"))).toBe(true)
      expect(q.enqueue(makeSpan("b"))).toBe(true)
      expect(q.enqueue(makeSpan("c"))).toBe(true)
      expect(q.enqueue(makeSpan("d"))).toBe(true)
      expect(q.size).toBe(4)
    })

    it("drops newest span when queue is full and returns false", () => {
      const sink = new TrackingSink()
      const dropped: string[] = []
      const q = track(
        makeQueue(sink, 2, (msg, data) => {
          if (msg.includes("dropping")) dropped.push((data as { spanId: string }).spanId)
        }),
      )

      expect(q.enqueue(makeSpan("a"))).toBe(true)
      expect(q.enqueue(makeSpan("b"))).toBe(true)
      // Queue is now full; c should be dropped
      expect(q.enqueue(makeSpan("c"))).toBe(false)
      expect(q.size).toBe(2)
      expect(dropped).toEqual(["c"])
    })

    it("does not drop when queue has room after draining", async () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 2))
      q.start()

      q.enqueue(makeSpan("a"))
      q.enqueue(makeSpan("b"))

      // Wait for worker to drain
      await q.flush(500)

      // Now there is room again
      expect(q.enqueue(makeSpan("c"))).toBe(true)
    })
  })

  describe("delivery", () => {
    it("delivers all enqueued spans in order", async () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 8))
      q.start()

      q.enqueue(makeSpan("1"))
      q.enqueue(makeSpan("2"))
      q.enqueue(makeSpan("3"))

      await q.stop(1000)

      expect(sink.inserted.map((s) => s.span_id)).toEqual(["1", "2", "3"])
    })

    it("continues delivering after a transient insert error", async () => {
      const sink = new TrackingSink()
      sink.failNext = 1 // First insert will throw
      const errors: string[] = []
      const q = track(makeQueue(sink, 8, (msg) => errors.push(msg)))
      q.start()

      q.enqueue(makeSpan("fail"))
      q.enqueue(makeSpan("ok"))

      await q.stop(1000)

      // "fail" was dropped by the error, "ok" made it through
      expect(sink.inserted.map((s) => s.span_id)).toEqual(["ok"])
      expect(errors.some((e) => e.includes("error inserting span"))).toBe(true)
    })
  })

  describe("flush", () => {
    it("synchronously drains all pending spans", async () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 8))
      // Do NOT call q.start() — background worker not running

      q.enqueue(makeSpan("x"))
      q.enqueue(makeSpan("y"))

      await q.flush(1000)

      expect(sink.inserted.map((s) => s.span_id)).toEqual(["x", "y"])
      expect(q.size).toBe(0)
    })

    it("stops after the timeout and reports remaining drops", async () => {
      const sink = new TrackingSink()
      sink.delayMs = 500 // each insert takes 500ms
      const warnings: string[] = []
      const q = track(makeQueue(sink, 8, (msg) => warnings.push(msg)))

      q.enqueue(makeSpan("slow1"))
      q.enqueue(makeSpan("slow2"))
      q.enqueue(makeSpan("slow3"))

      // Give it only 600ms — enough for one insert but not three
      await q.flush(600)

      expect(sink.inserted.length).toBeLessThan(3)
      expect(warnings.some((w) => w.includes("timed out"))).toBe(true)
    })

    it("is a no-op when queue is empty", async () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 8))
      // Should resolve immediately without error
      await q.flush(100)
      expect(sink.inserted).toHaveLength(0)
    })
  })

  describe("stop", () => {
    it("drains remaining spans on stop", async () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 8))
      q.start()

      q.enqueue(makeSpan("a"))
      q.enqueue(makeSpan("b"))

      await q.stop(1000)

      expect(sink.inserted.map((s) => s.span_id)).toEqual(["a", "b"])
    })
  })

  describe("size", () => {
    it("reports current queue depth", () => {
      const sink = new TrackingSink()
      const q = track(makeQueue(sink, 8))

      expect(q.size).toBe(0)
      q.enqueue(makeSpan("a"))
      expect(q.size).toBe(1)
      q.enqueue(makeSpan("b"))
      expect(q.size).toBe(2)
    })
  })

  describe("DEFAULT_QUEUE_SIZE", () => {
    it("is 1024", () => {
      expect(DEFAULT_QUEUE_SIZE).toBe(1024)
    })
  })

  describe.skip("non-blocking delivery", () => {
    it("hook returns before the span is delivered to a slow sink", async () => {
      const SINK_DELAY_MS = 200

      // A sink that takes 200ms per insert
      const sink = new TrackingSink()
      sink.delayMs = SINK_DELAY_MS

      const q = track(makeQueue(sink, 8))
      q.start()

      const before = Date.now()
      // enqueue is synchronous — should return in << SINK_DELAY_MS
      q.enqueue(makeSpan("slow"))
      const elapsed = Date.now() - before

      expect(elapsed).toBeLessThan(SINK_DELAY_MS)
      // Span has not been delivered yet
      expect(sink.inserted).toHaveLength(0)

      // After waiting, the span is delivered
      await q.stop(1000)
      expect(sink.inserted).toHaveLength(1)
    })

    it("hook calling enqueue does not block even when sink is consistently slow", async () => {
      const SINK_DELAY_MS = 100
      const SPAN_COUNT = 5

      const sink = new TrackingSink()
      sink.delayMs = SINK_DELAY_MS

      const q = track(makeQueue(sink, 8))
      q.start()

      const before = Date.now()
      for (let i = 0; i < SPAN_COUNT; i++) {
        q.enqueue(makeSpan(String(i)))
      }
      const elapsed = Date.now() - before

      // All 5 enqueues should be near-instant, well under one sink delay
      expect(elapsed).toBeLessThan(SINK_DELAY_MS)

      // Spans are eventually all delivered
      await q.stop(2000)
      expect(sink.inserted).toHaveLength(SPAN_COUNT)
    })
  })
})
