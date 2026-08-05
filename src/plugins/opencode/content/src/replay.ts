/**
 * Replay a LOG_TO_FILE NDJSON log through EventProcessor.
 *
 * Reads a file produced by FileLogger and feeds each record into an
 * EventProcessor in order, producing the same SpanData that would have been
 * sent to Braintrust during the live session.  The result can be fed into
 * spansToTree / assertTreeMatches for fixture-based tests.
 *
 * Records consumed per kind:
 *   event              → processEvent({ type: event_type, properties: data })
 *   chat.message.input → buffered; flushed when chat.message.output arrives
 *   chat.message.output → flushes buffered input via processChatMessageRaw
 *   tool.before.input  → processToolExecuteBefore(sessionID, callID, args)
 *   tool.after.input   → buffered; flushed when tool.after.result arrives
 *   tool.after.result  → flushes buffered input via processToolExecuteAfter
 *
 * All other kinds (tool.before.output, config, error) are skipped.
 */

import * as fs from "node:fs"
import type { SpanData } from "./client"
import type { EventProcessorConfig } from "./event-processor"
import { EventProcessor } from "./event-processor"
import type { FileLogRecord } from "./file-logger"
import type { SpanTree } from "./span-sink"
import { type SpanSink, spansToTree, TestSpanCollector } from "./span-sink"

// ---------------------------------------------------------------------------
// Internal types mirroring the raw hook argument shapes
// ---------------------------------------------------------------------------

interface RawChatMessageInput {
  sessionID?: string
  model?: { providerID?: string; modelID?: string } | string
}

interface RawChatMessageOutput {
  parts?: Array<{ type: string; text?: string }>
}

interface RawChatSystemInput {
  sessionID?: string
}

interface RawChatSystemOutput {
  system?: string[]
}

interface RawToolInput {
  tool?: string
  sessionID?: string
  callID?: string
  args?: unknown
}

interface RawToolResult {
  output?: unknown
  title?: string
  metadata?: unknown
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseRecords(filePath: string): FileLogRecord[] {
  const content = fs.readFileSync(filePath, "utf-8")
  const records: FileLogRecord[] = []
  for (const line of content.split("\n")) {
    const trimmed = line.trim()
    if (!trimmed) continue
    try {
      records.push(JSON.parse(trimmed) as FileLogRecord)
    } catch {
      // Skip malformed lines (e.g. partial write at process exit)
    }
  }
  return records
}

function parseModel(
  model: RawChatMessageInput["model"],
): { providerID?: string; modelID?: string } | undefined {
  if (!model) return undefined
  if (typeof model === "string") {
    const slash = model.indexOf("/")
    if (slash === -1) return { providerID: model }
    return { providerID: model.slice(0, slash), modelID: model.slice(slash + 1) }
  }
  return model
}

// ---------------------------------------------------------------------------
// Core replay function
// ---------------------------------------------------------------------------

/**
 * Replay a log file into a SpanSink and return the collected spans.
 *
 * @param filePath  Path to the NDJSON file written by FileLogger.
 * @param config    EventProcessorConfig (projectName is required).
 * @param sink      Optional custom SpanSink; defaults to a new TestSpanCollector.
 */
export async function replayLogFile(
  filePath: string,
  config: EventProcessorConfig,
  sink?: SpanSink,
): Promise<SpanData[]> {
  const collector = sink ?? new TestSpanCollector()
  const processor = new EventProcessor(collector, config)

  const records = parseRecords(filePath)

  // Pending state for adjacent record pairs
  let pendingChatInput: {
    sessionID: string
    model?: { providerID?: string; modelID?: string }
  } | null = null
  let pendingChatSystemSessionID: string | null = null
  let pendingToolBeforeInput: RawToolInput | null = null
  let pendingToolAfterInput: RawToolInput | null = null

  for (const record of records) {
    switch (record.kind) {
      case "event": {
        if (!record.event_type) break
        // Cast through unknown — the SDK Event union is discriminated by type
        // and the log captures the real event.properties verbatim.
        const event = {
          type: record.event_type,
          properties: record.data,
        } as unknown as Parameters<typeof processor.processEvent>[0]
        await processor.processEvent(event)
        break
      }

      case "chat.message.input": {
        const input = record.data as RawChatMessageInput
        const sessionID = input.sessionID ?? record.session_id
        if (!sessionID) break
        pendingChatInput = { sessionID, model: parseModel(input.model) }
        break
      }

      case "chat.message.output": {
        if (!pendingChatInput) break
        const { sessionID, model } = pendingChatInput
        pendingChatInput = null
        const output = record.data as RawChatMessageOutput
        await processor.processChatMessageRaw(sessionID, output, model)
        break
      }

      case "chat.system.input": {
        const input = record.data as RawChatSystemInput
        pendingChatSystemSessionID = input.sessionID ?? record.session_id ?? null
        break
      }

      case "chat.system.output": {
        const sessionID = pendingChatSystemSessionID
        pendingChatSystemSessionID = null
        if (!sessionID) break
        const output = record.data as RawChatSystemOutput
        if (Array.isArray(output.system) && output.system.length > 0) {
          await processor.processSystemTransform(sessionID, output.system)
        }
        break
      }

      case "tool.before.input": {
        // Buffer: args may arrive in the paired tool.before.output record
        pendingToolBeforeInput = record.data as RawToolInput
        break
      }

      case "tool.before.output": {
        // Flush buffered tool.before.input, merging args from this output record
        if (!pendingToolBeforeInput) break
        const toolInput = pendingToolBeforeInput
        pendingToolBeforeInput = null
        const sessionID = toolInput.sessionID ?? record.session_id
        const callID = toolInput.callID
        if (!sessionID || !callID) break
        // Args may be on the input record (built-in tools) or output record (MCP tools)
        const outputData = record.data as { args?: unknown }
        const args = toolInput.args ?? outputData.args
        await processor.processToolExecuteBefore(sessionID, callID, args)
        break
      }

      case "tool.after.input": {
        pendingToolAfterInput = record.data as RawToolInput
        break
      }

      case "tool.after.result": {
        if (!pendingToolAfterInput) break
        const toolInput = pendingToolAfterInput
        pendingToolAfterInput = null
        const sessionID = toolInput.sessionID ?? record.session_id
        const { callID, tool } = toolInput
        if (!sessionID || !callID || !tool) break
        const result = record.data as RawToolResult
        // Pass the whole result object as output so extractToolOutput can handle
        // MCP { content: [...] } responses as well as plain { output: "..." } ones.
        const outputValue = result.output !== undefined ? result.output : record.data
        await processor.processToolExecuteAfter(
          sessionID,
          callID,
          tool,
          result.title ?? tool,
          outputValue,
          result.metadata,
        )
        break
      }

      // tool.before.output, config, error — nothing to replay
      default:
        break
    }
  }

  return collector.getSpans()
}

/**
 * Replay a log file and return the span tree.
 * Convenience wrapper around replayLogFile + spansToTree.
 */
export async function replayLogFileToTree(
  filePath: string,
  config: EventProcessorConfig,
): Promise<SpanTree | null> {
  const spans = await replayLogFile(filePath, config)
  return spansToTree(spans)
}
