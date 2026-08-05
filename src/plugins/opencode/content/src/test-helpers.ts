/**
 * Test helpers for span tracing tests
 *
 * Provides a DSL for constructing test events and comparing expected span trees
 */

import type {
  AssistantMessage,
  Event,
  EventMessagePartUpdated,
  EventMessageUpdated,
  EventSessionCreated,
  EventSessionDeleted,
  EventSessionError,
  EventSessionIdle,
  ReasoningPart,
  Session,
  TextPart,
  ToolPart,
  ToolStateCompleted,
  ToolStateError,
  ToolStateRunning,
} from "@opencode-ai/sdk"
import { TestClock } from "./clock"
import { EventProcessor } from "./event-processor"
import { type SpanTree, spansToTree, TestSpanCollector } from "./span-sink"

// ============================================================================
// Types
// ============================================================================

export interface TestSession {
  sessionID: string
  items: Array<TestItem>
}

/**
 * A test item is either an OpenCode Event or a hook call
 */
export type TestItem =
  | Event
  | {
      _hook: "chat.message"
      userMessage: string
      model?: { providerID: string; modelID: string }
      sessionID?: string // Optional: target session for subagent scenarios
    }
  | {
      _hook: "tool.execute"
      callID: string
      tool: string
      title: string
      input: Record<string, unknown>
      output: string | undefined | unknown
    }
  | {
      _hook: "experimental.chat.system.transform"
      system: string[]
      sessionID?: string // Optional: target session (defaults to main session)
    }

export interface TestToolCall {
  id: string
  tool: string
  args: Record<string, unknown>
}

// ============================================================================
// Event Builders - Create real OpenCode SDK events
// ============================================================================

/**
 * Create a test session with items (events and hook calls)
 */
export function session(sessionID: string, ...items: TestItem[]): TestSession {
  return { sessionID, items }
}

/**
 * Create a session.created event
 */
export function sessionCreated(sessionID: string): EventSessionCreated {
  const sessionInfo: Session = {
    id: sessionID,
    projectID: "test-project",
    directory: "/test",
    version: "1.0.0",
    title: "Test",
    time: { created: Date.now(), updated: Date.now() },
  }
  return {
    type: "session.created",
    properties: { info: sessionInfo },
  }
}

/**
 * Create a session.idle event
 */
export function sessionIdle(sessionID: string): EventSessionIdle {
  return {
    type: "session.idle",
    properties: { sessionID },
  }
}

/**
 * Create a session.deleted event
 */
export function sessionDeleted(sessionID: string): EventSessionDeleted {
  const sessionInfo: Session = {
    id: sessionID,
    projectID: "test-project",
    directory: "/test",
    version: "1.0.0",
    title: "Test",
    time: { created: Date.now(), updated: Date.now() },
  }
  return {
    type: "session.deleted",
    properties: { info: sessionInfo },
  }
}

/**
 * Create a session.error event
 */
export function sessionError(
  sessionID: string,
  errorName: string,
  errorMessage?: string,
): EventSessionError {
  return {
    type: "session.error",
    properties: {
      sessionID,
      error: {
        name: errorName,
        data: { message: errorMessage || errorName },
      } as EventSessionError["properties"]["error"],
    },
  }
}

/**
 * Create a session.created event for a child session (subagent)
 */
export function childSessionCreated(
  childSessionID: string,
  parentSessionID: string,
  title?: string,
): EventSessionCreated {
  const sessionInfo: Session & { parentID?: string } = {
    id: childSessionID,
    parentID: parentSessionID,
    projectID: "test-project",
    directory: "/test",
    version: "1.0.0",
    title: title || `Task (@explore subagent)`,
    time: { created: Date.now(), updated: Date.now() },
  }
  return {
    type: "session.created",
    properties: { info: sessionInfo },
  }
}

/**
 * User sends a chat message (hook call, not an event)
 * @param userMessage - The user's message
 * @param options - Optional model and sessionID (for subagent scenarios)
 */
export function chatMessage(
  userMessage: string,
  options?: { providerID: string; modelID: string } | { sessionID: string },
): TestItem {
  if (options && "sessionID" in options) {
    return { _hook: "chat.message", userMessage, sessionID: options.sessionID }
  }
  return {
    _hook: "chat.message",
    userMessage,
    model: options as { providerID: string; modelID: string } | undefined,
  }
}

/**
 * Create message.part.updated event for text
 */
export function textPart(
  sessionID: string,
  messageID: string,
  text: string,
): EventMessagePartUpdated {
  const part: TextPart = {
    id: `prt_text_${messageID}`,
    sessionID,
    messageID,
    type: "text",
    text,
  }
  return {
    type: "message.part.updated",
    properties: { part },
  }
}

/**
 * Create message.part.updated event for reasoning/thinking content
 */
export function reasoningPart(
  sessionID: string,
  messageID: string,
  text: string,
): EventMessagePartUpdated {
  const part: ReasoningPart = {
    id: `prt_reasoning_${messageID}`,
    sessionID,
    messageID,
    type: "reasoning",
    text,
    time: { start: Date.now() },
  }
  return {
    type: "message.part.updated",
    properties: { part },
  }
}

/**
 * Create message.part.updated event for a tool call
 */
export function toolCallPart(
  sessionID: string,
  messageID: string,
  callID: string,
  tool: string,
  args: Record<string, unknown>,
): EventMessagePartUpdated {
  const toolState: ToolStateRunning = {
    status: "running",
    input: args,
    time: { start: Date.now() },
  }
  const part: ToolPart = {
    id: `prt_tool_${callID}`,
    sessionID,
    messageID,
    type: "tool",
    callID,
    tool,
    state: toolState,
  }
  return {
    type: "message.part.updated",
    properties: { part },
  }
}

/**
 * Create message.part.updated event for a completed tool call (output captured from state)
 */
export function toolCallCompletedPart(
  sessionID: string,
  messageID: string,
  callID: string,
  tool: string,
  args: Record<string, unknown>,
  output: string,
): EventMessagePartUpdated {
  const toolState: ToolStateCompleted = {
    status: "completed",
    input: args,
    output,
    title: tool,
    metadata: {},
    time: { start: Date.now() - 100, end: Date.now() },
  }
  const part: ToolPart = {
    id: `prt_tool_${callID}`,
    sessionID,
    messageID,
    type: "tool",
    callID,
    tool,
    state: toolState,
  }
  return {
    type: "message.part.updated",
    properties: { part },
  }
}

/**
 * Create message.part.updated event for a failed tool call
 */
export function toolCallErrorPart(
  sessionID: string,
  messageID: string,
  callID: string,
  tool: string,
  args: Record<string, unknown>,
  error: string,
): EventMessagePartUpdated {
  const toolState: ToolStateError = {
    status: "error",
    input: args,
    error,
    time: { start: Date.now() - 100, end: Date.now() },
  }
  const part: ToolPart = {
    id: `prt_tool_${callID}`,
    sessionID,
    messageID,
    type: "tool",
    callID,
    tool,
    state: toolState,
  }
  return {
    type: "message.part.updated",
    properties: { part },
  }
}

/**
 * Create message.updated event for assistant message completion
 */
export function messageCompleted(
  sessionID: string,
  messageID: string,
  options?: {
    tokens?: {
      input: number
      output: number
      reasoning?: number
      cache?: { read?: number; write?: number }
    }
    model?: { providerID: string; modelID: string }
    time?: { created: number; completed: number }
  },
): EventMessageUpdated {
  const modelInfo = options?.model || { providerID: "anthropic", modelID: "claude-3-haiku" }
  const time = options?.time || { created: Date.now(), completed: Date.now() + 500 }

  const messageInfo: AssistantMessage = {
    id: messageID,
    sessionID,
    role: "assistant",
    time,
    parentID: "parent",
    modelID: modelInfo.modelID,
    providerID: modelInfo.providerID,
    mode: "build",
    path: { cwd: "/test", root: "/test" },
    cost: 0.001,
    tokens: {
      input: options?.tokens?.input ?? 10,
      output: options?.tokens?.output ?? 5,
      reasoning: options?.tokens?.reasoning ?? 0,
      cache: {
        read: options?.tokens?.cache?.read ?? 0,
        write: options?.tokens?.cache?.write ?? 0,
      },
    },
  }
  return {
    type: "message.updated",
    properties: { info: messageInfo },
  }
}

/**
 * Tool execution (hook call, not an event)
 */
export function toolExecute(
  callID: string,
  tool: string,
  title: string,
  input: Record<string, unknown>,
  output: string | undefined | unknown,
): TestItem {
  return { _hook: "tool.execute", callID, tool, title, input, output }
}

/**
 * experimental.chat.system.transform hook call (not an event).
 * Carries the resolved system prompt parts that OpenCode passes to the LLM.
 */
export function systemTransform(system: string[], options?: { sessionID?: string }): TestItem {
  return { _hook: "experimental.chat.system.transform", system, sessionID: options?.sessionID }
}

/**
 * Helper to build a tool call object for use with toolCallPart
 */
export function toolCall(id: string, tool: string, args: Record<string, unknown>): TestToolCall {
  return { id, tool, args }
}

// ============================================================================
// Expected Span Type - matches SpanData structure from Braintrust
// ============================================================================

/**
 * Expected span - matches the structure reported to Braintrust
 * All fields are optional for flexible matching
 */
export interface ExpectedSpan {
  span_attributes?: {
    name?: string | RegExp
    type?: "llm" | "task" | "tool" | "function" | "eval" | "score"
  }
  input?: unknown
  output?: unknown
  error?: string | RegExp
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
  children?: ExpectedSpan[]
}

// ============================================================================
// Main Test Function - Process events and compare to expected structure
// ============================================================================

type HookItem =
  | {
      _hook: "chat.message"
      userMessage: string
      model?: { providerID: string; modelID: string }
      sessionID?: string
    }
  | {
      _hook: "tool.execute"
      callID: string
      tool: string
      title: string
      input: Record<string, unknown>
      output: string | undefined | unknown
    }
  | {
      _hook: "experimental.chat.system.transform"
      system: string[]
      sessionID?: string
    }

function isHook(item: TestItem): item is HookItem {
  return typeof item === "object" && "_hook" in item
}

/**
 * Process test session items and return the span tree
 */
export async function eventsToTree(
  testSession: TestSession,
  projectName = "test-project",
): Promise<SpanTree | null> {
  const clock = new TestClock()
  const collector = new TestSpanCollector()
  const processor = new EventProcessor(collector, { projectName }, { clock })

  const { sessionID } = testSession

  for (const item of testSession.items) {
    // Advance the clock for each item to ensure unique, ordered timestamps
    clock.tick()

    if (isHook(item)) {
      if (item._hook === "chat.message") {
        const hook = item as {
          _hook: "chat.message"
          userMessage: string
          model?: { providerID: string; modelID: string }
          sessionID?: string
        }
        // Use hook's sessionID if specified (for subagent scenarios), otherwise default to main session
        const targetSessionID = hook.sessionID || sessionID
        await processor.processChatMessage(
          targetSessionID,
          hook.userMessage,
          hook.model || { providerID: "anthropic", modelID: "claude-3-haiku" },
        )
      } else if (item._hook === "tool.execute") {
        const hook = item as {
          _hook: "tool.execute"
          callID: string
          tool: string
          title: string
          input: Record<string, unknown>
          output: string | undefined | unknown
        }
        await processor.processToolExecuteBefore(sessionID, hook.callID, hook.input)
        clock.tick() // Advance time between before and after
        await processor.processToolExecuteAfter(
          sessionID,
          hook.callID,
          hook.tool,
          hook.title,
          hook.output,
          hook.input,
        )
      } else if (item._hook === "experimental.chat.system.transform") {
        const hook = item as {
          _hook: "experimental.chat.system.transform"
          system: string[]
          sessionID?: string
        }
        const targetSessionID = hook.sessionID || sessionID
        await processor.processSystemTransform(targetSessionID, hook.system)
      }
    } else {
      // It's a real Event - patch timestamps to use clock time for deterministic ordering
      const event = patchEventTimestamps(item as Event, clock)
      await processor.processEvent(event)
    }
  }

  return spansToTree(collector.getSpans())
}

/**
 * Patch event timestamps to use clock time for deterministic test ordering
 */
function patchEventTimestamps(event: Event, clock: TestClock): Event {
  if (event.type === "message.updated") {
    const props = event.properties as Record<string, unknown>
    const info = props.info as Record<string, unknown> | undefined
    if (info?.time) {
      const time = info.time as Record<string, unknown>
      // Use clock time for start/end to ensure proper ordering with tool spans
      const startTime = clock.now()
      clock.tick()
      const endTime = clock.now()
      // Create patched event with clock timestamps
      const patchedInfo = {
        ...info,
        time: {
          ...time,
          created: startTime,
          completed: endTime,
        },
      }
      return {
        type: event.type,
        properties: { info: patchedInfo },
      } as Event
    }
  }
  return event
}

/**
 * Check if a span name matches (string or regex)
 */
function nameMatches(actual: string | undefined, expected: string | RegExp | undefined): boolean {
  if (expected === undefined) return true
  if (actual === undefined) return false
  if (expected instanceof RegExp) return expected.test(actual)
  return actual === expected
}

/**
 * Check if a single span matches expected (without checking children)
 */
function spanMatchesSingle(actual: SpanTree, expected: ExpectedSpan): boolean {
  if (expected.span_attributes?.name !== undefined) {
    if (!nameMatches(actual.name, expected.span_attributes.name)) return false
  }

  if (expected.span_attributes?.type !== undefined) {
    if (actual.type !== expected.span_attributes.type) return false
  }

  if (expected.metrics) {
    if (
      expected.metrics.prompt_tokens !== undefined &&
      actual.metrics?.prompt_tokens !== expected.metrics.prompt_tokens
    )
      return false
    if (
      expected.metrics.completion_tokens !== undefined &&
      actual.metrics?.completion_tokens !== expected.metrics.completion_tokens
    )
      return false
    if (expected.metrics.tokens !== undefined && actual.metrics?.tokens !== expected.metrics.tokens)
      return false
    if (
      expected.metrics.prompt_cached_tokens !== undefined &&
      actual.metrics?.prompt_cached_tokens !== expected.metrics.prompt_cached_tokens
    )
      return false
    if (
      expected.metrics.prompt_cache_creation_tokens !== undefined &&
      actual.metrics?.prompt_cache_creation_tokens !== expected.metrics.prompt_cache_creation_tokens
    )
      return false
    if (
      expected.metrics.reasoning_tokens !== undefined &&
      actual.metrics?.reasoning_tokens !== expected.metrics.reasoning_tokens
    )
      return false
  }

  if (expected.input !== undefined) {
    if (JSON.stringify(actual.input) !== JSON.stringify(expected.input)) return false
  }

  if (expected.output !== undefined) {
    if (JSON.stringify(actual.output) !== JSON.stringify(expected.output)) return false
  }

  if (expected.error !== undefined) {
    if (!nameMatches(actual.error, expected.error)) return false
  }

  return true
}

/**
 * Check if actual span tree matches expected structure
 */
export function matchesExpected(actual: SpanTree | null, expected: ExpectedSpan): boolean {
  if (!actual) return false

  if (!spanMatchesSingle(actual, expected)) return false

  if (expected.children !== undefined) {
    if (actual.children.length !== expected.children.length) return false

    for (let i = 0; i < expected.children.length; i++) {
      if (!matchesExpected(actual.children[i], expected.children[i])) return false
    }
  }

  return true
}

/**
 * Get a diff description of what doesn't match
 */
export function getDiff(actual: SpanTree | null, expected: ExpectedSpan, path = "root"): string[] {
  const diffs: string[] = []

  if (!actual) {
    diffs.push(`${path}: expected span but got null`)
    return diffs
  }

  if (expected.span_attributes?.name !== undefined) {
    if (!nameMatches(actual.name, expected.span_attributes.name)) {
      diffs.push(
        `${path}.span_attributes.name: expected "${expected.span_attributes.name}", got "${actual.name}"`,
      )
    }
  }

  if (
    expected.span_attributes?.type !== undefined &&
    actual.type !== expected.span_attributes.type
  ) {
    diffs.push(
      `${path}.span_attributes.type: expected "${expected.span_attributes.type}", got "${actual.type}"`,
    )
  }

  if (expected.metrics) {
    if (
      expected.metrics.prompt_tokens !== undefined &&
      actual.metrics?.prompt_tokens !== expected.metrics.prompt_tokens
    ) {
      diffs.push(
        `${path}.metrics.prompt_tokens: expected ${expected.metrics.prompt_tokens}, got ${actual.metrics?.prompt_tokens}`,
      )
    }
    if (
      expected.metrics.completion_tokens !== undefined &&
      actual.metrics?.completion_tokens !== expected.metrics.completion_tokens
    ) {
      diffs.push(
        `${path}.metrics.completion_tokens: expected ${expected.metrics.completion_tokens}, got ${actual.metrics?.completion_tokens}`,
      )
    }
    if (
      expected.metrics.tokens !== undefined &&
      actual.metrics?.tokens !== expected.metrics.tokens
    ) {
      diffs.push(
        `${path}.metrics.tokens: expected ${expected.metrics.tokens}, got ${actual.metrics?.tokens}`,
      )
    }
    if (
      expected.metrics.prompt_cached_tokens !== undefined &&
      actual.metrics?.prompt_cached_tokens !== expected.metrics.prompt_cached_tokens
    ) {
      diffs.push(
        `${path}.metrics.prompt_cached_tokens: expected ${expected.metrics.prompt_cached_tokens}, got ${actual.metrics?.prompt_cached_tokens}`,
      )
    }
    if (
      expected.metrics.prompt_cache_creation_tokens !== undefined &&
      actual.metrics?.prompt_cache_creation_tokens !== expected.metrics.prompt_cache_creation_tokens
    ) {
      diffs.push(
        `${path}.metrics.prompt_cache_creation_tokens: expected ${expected.metrics.prompt_cache_creation_tokens}, got ${actual.metrics?.prompt_cache_creation_tokens}`,
      )
    }
    if (
      expected.metrics.reasoning_tokens !== undefined &&
      actual.metrics?.reasoning_tokens !== expected.metrics.reasoning_tokens
    ) {
      diffs.push(
        `${path}.metrics.reasoning_tokens: expected ${expected.metrics.reasoning_tokens}, got ${actual.metrics?.reasoning_tokens}`,
      )
    }
  }

  if (expected.error !== undefined && !nameMatches(actual.error, expected.error)) {
    diffs.push(`${path}.error: expected "${expected.error}", got "${actual.error}"`)
  }

  if (expected.children !== undefined) {
    if (actual.children.length !== expected.children.length) {
      diffs.push(
        `${path}.children.length: expected ${expected.children.length}, got ${actual.children.length}`,
      )
      diffs.push(
        `  expected: ${expected.children.map((c) => `${c.span_attributes?.type}:${c.span_attributes?.name}`).join(", ")}`,
      )
      diffs.push(`  actual: ${actual.children.map((c) => `${c.type}:${c.name}`).join(", ")}`)
    } else {
      for (let i = 0; i < expected.children.length; i++) {
        diffs.push(...getDiff(actual.children[i], expected.children[i], `${path}.children[${i}]`))
      }
    }
  }

  return diffs
}

/**
 * Assert that actual tree matches expected structure
 */
export function assertTreeMatches(actual: SpanTree | null, expected: ExpectedSpan): void {
  if (!matchesExpected(actual, expected)) {
    const diffs = getDiff(actual, expected)
    throw new Error(`Span tree does not match expected:\n${diffs.join("\n")}`)
  }
}

/**
 * Main entry point: process items and assert tree matches expected
 */
export async function assertEventsProduceTree(
  testSession: TestSession,
  expected: ExpectedSpan,
  projectName = "test-project",
): Promise<void> {
  const actual = await eventsToTree(testSession, projectName)
  assertTreeMatches(actual, expected)
}
