/**
 * SpanSink interface - abstracts span storage for testing
 */

import type { SpanData } from "./client"

/**
 * Interface for receiving spans - allows testing without real API calls
 */
export interface SpanSink {
  insertSpan(span: SpanData): Promise<string | undefined>
  getSpans?(): SpanData[]
}

/**
 * In-memory span collector for testing
 */
export class TestSpanCollector implements SpanSink {
  private spans: SpanData[] = []

  async insertSpan(span: SpanData): Promise<string | undefined> {
    // Handle merge operations
    if (span._is_merge) {
      const existingIndex = this.spans.findIndex((s) => s.span_id === span.span_id)
      if (existingIndex >= 0) {
        // Merge the span data
        const existing = this.spans[existingIndex]
        this.spans[existingIndex] = {
          ...existing,
          ...span,
          metadata: { ...existing.metadata, ...span.metadata },
          metrics: { ...existing.metrics, ...span.metrics },
        }
        return span.span_id
      }
    }

    this.spans.push(span)
    return span.span_id
  }

  getSpans(): SpanData[] {
    return [...this.spans]
  }

  clear(): void {
    this.spans = []
  }
}

/**
 * Represents a span in a tree structure for testing assertions
 */
export interface SpanTree {
  span_id: string
  root_span_id: string
  name?: string
  type?: "task" | "llm" | "tool" | "function" | "eval" | "score"
  input?: unknown
  output?: unknown
  error?: string
  metrics?: {
    start?: number
    end?: number
    prompt_tokens?: number
    completion_tokens?: number
    tokens?: number
    prompt_cached_tokens?: number
    prompt_cache_creation_tokens?: number
    reasoning_tokens?: number
  }
  metadata?: Record<string, unknown>
  children: SpanTree[]
}

/**
 * Convert flat spans to a tree structure based on span_parents
 */
export function spansToTree(spans: SpanData[]): SpanTree | null {
  if (spans.length === 0) return null

  // Find the root span (no parents or parent is itself)
  const rootSpan = spans.find(
    (s) => !s.span_parents || s.span_parents.length === 0 || s.span_parents[0] === s.span_id,
  )

  if (!rootSpan) return null

  // Build a map of span_id -> children
  const childrenMap = new Map<string, SpanData[]>()
  for (const span of spans) {
    if (span.span_parents && span.span_parents.length > 0) {
      const parentId = span.span_parents[0]
      if (parentId !== span.span_id) {
        const children = childrenMap.get(parentId) || []
        children.push(span)
        childrenMap.set(parentId, children)
      }
    }
  }

  // Recursively build tree
  function buildNode(span: SpanData): SpanTree {
    const children = childrenMap.get(span.span_id) || []
    // Sort children by start time for consistent ordering
    // Use original array index as tiebreaker to preserve insertion order
    const indexedChildren = children.map((c, _i) => ({ span: c, originalIndex: spans.indexOf(c) }))
    indexedChildren.sort((a, b) => {
      const aStart = a.span.metrics?.start || 0
      const bStart = b.span.metrics?.start || 0
      if (aStart !== bStart) return aStart - bStart
      return a.originalIndex - b.originalIndex
    })
    const sortedChildren = indexedChildren.map((ic) => ic.span)

    return {
      span_id: span.span_id,
      root_span_id: span.root_span_id,
      name: span.span_attributes?.name,
      type: span.span_attributes?.type,
      input: span.input,
      output: span.output,
      error: span.error,
      metrics: span.metrics,
      metadata: span.metadata as Record<string, unknown>,
      children: sortedChildren.map(buildNode),
    }
  }

  return buildNode(rootSpan)
}
