/**
 * Event processor for testing - processes OpenCode events into spans
 *
 * This module extracts the core event processing logic so it can be tested
 * independently of the OpenCode plugin infrastructure.
 */

import type { Event } from "@opencode-ai/sdk"
import type { SpanData } from "./client"
import type { Clock } from "./clock"
import { msToSeconds, wallClock } from "./clock"
import type { SpanSink } from "./span-sink"

// Generate a UUID
function generateUUID(): string {
  return crypto.randomUUID()
}

interface ExplicitSkillRequestMetadata extends Record<string, unknown> {
  loaded_skill_names: string[]
  loaded_skills: Array<{ name: string }>
}

function skillLoadMetadata(tool: string, args: unknown): Record<string, unknown> | undefined {
  if (tool !== "skill") return undefined
  const name =
    args && typeof args === "object" && "name" in args && typeof args.name === "string"
      ? args.name
      : undefined
  return {
    tool_kind: "skill",
    skill_name: name,
  }
}

function skillNameFromArgs(args: unknown): string | undefined {
  return args && typeof args === "object" && "name" in args && typeof args.name === "string"
    ? args.name
    : undefined
}

function normalizeExplicitSkillName(name: string): string | undefined {
  const normalized = name.trim().replace(/[),.;]+$/, "")
  return normalized ? normalized : undefined
}

function explicitSkillRequestMetadata(
  names: readonly string[],
): ExplicitSkillRequestMetadata | undefined {
  const seen = new Set<string>()
  const loaded_skill_names: string[] = []
  for (const name of names) {
    const normalized = normalizeExplicitSkillName(name)
    if (!normalized || seen.has(normalized)) continue
    seen.add(normalized)
    loaded_skill_names.push(normalized)
  }
  if (loaded_skill_names.length === 0) return undefined
  return {
    loaded_skill_names,
    loaded_skills: loaded_skill_names.map((name) => ({ name })),
  }
}

function explicitSkillNamesFromUserMessage(message: string): string[] {
  const names: string[] = []
  for (const match of message.matchAll(/(?:^|\s)\/skills(?::|\s+)([A-Za-z0-9_.:-]+)/g)) {
    names.push(match[1] ?? "")
  }
  return explicitSkillRequestMetadata(names)?.loaded_skill_names ?? []
}

function isExplicitSkillsCommand(message: string): boolean {
  return /(?:^|\s)\/skills(?=$|\s|:)/.test(message)
}

function skillLoadTrigger(
  explicitSkillsCommand: boolean | undefined,
  explicitSkillNames: readonly string[] | undefined,
  tool: string,
  args: unknown,
): "explicit" | undefined {
  if (tool !== "skill") return undefined
  const name = skillNameFromArgs(args)
  if (name === undefined) return undefined
  if (explicitSkillNames?.includes(name)) return "explicit"
  return explicitSkillsCommand ? "explicit" : undefined
}

function skillToolSpanName(tool: string, title: string, args: unknown, fallback: string): string {
  if (tool !== "skill") return fallback
  const name =
    args && typeof args === "object" && "name" in args && typeof args.name === "string"
      ? args.name
      : undefined
  return name ? `skill: ${name}` : title || "skill"
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
}

function conciseErrorText(value: unknown, fallback = "Tool execution failed"): string {
  if (typeof value === "string") return value.split("\n")[0] || fallback
  if (value instanceof Error) return value.message || fallback
  if (isRecord(value)) {
    const candidate = value.error ?? value.message ?? value.stderr ?? value.output ?? value.result
    if (typeof candidate === "string") return candidate.split("\n")[0] || fallback
  }
  return fallback
}

// State management for tracing
interface SessionState {
  rootSpanId: string
  effectiveRootSpanId: string // For child sessions, this is the parent's root span ID; otherwise same as rootSpanId
  currentTurnSpanId?: string
  turnNumber: number
  toolCallCount: number
  startTime: number
  currentTurnStartTime?: number
  currentInput?: string
  currentOutput?: string
  currentMessageId?: string
  currentTurnExplicitSkillsCommand?: boolean
  currentTurnExplicitSkillNames?: string[]
  // Joined system prompt captured from experimental.chat.system.transform
  systemPrompt?: string
  // Parent-child session tracking (for subagents)
  parentSessionId?: string // If this is a child session, the parent's session ID
  parentRootSpanId?: string // The parent's root span ID (child spans link to this as root)
  parentTurnSpanId?: string // The parent's turn span ID (child's root span is a child of this)
  subagentTitle?: string // Title for subagent spans (e.g., "{subagent_type}: {description}")
  // LLM span tracking
  currentAssistantMessageId?: string
  llmOutputParts: Map<string, string>
  llmToolCalls: Map<
    string,
    Array<{ id: string; type: string; function: { name: string; arguments: string } }>
  >
  llmReasoningParts: Map<string, string> // messageId -> reasoning/thinking text
  processedLlmMessages: Set<string>
  // Tool span tracking
  toolStartTimes: Map<string, number>
  toolCallMessageIds: Map<string, string> // callID -> messageId (to look up reasoning)
  toolCallArgs: Map<string, unknown> // callID -> tool arguments
  toolCallOutputs: Map<string, unknown> // callID -> tool output (captured from message.part.updated completed state)
  toolCallErrors: Map<string, string> // callID -> tool error captured from message.part.updated error state
  deniedToolCallIds: Set<string> // callIDs already emitted as denied spans
}

interface PermissionRequestRecord {
  id?: string
  sessionID?: string
  callID?: string
  tool?: string
  input?: unknown
  title?: string
  type?: string
}

export interface EventProcessorConfig {
  projectName: string
  worktree?: string
  directory?: string
  additionalMetadata?: Record<string, unknown>
}

/**
 * Processes OpenCode events and generates spans
 */
export class EventProcessor {
  private sessionStates = new Map<string, SessionState>()
  private permissionRequests = new Map<string, PermissionRequestRecord>()
  private spanSink: SpanSink
  private config: EventProcessorConfig
  private log: (msg: string, data?: unknown) => void
  private clock: Clock

  constructor(
    spanSink: SpanSink,
    config: EventProcessorConfig,
    options?: {
      log?: (msg: string, data?: unknown) => void
      clock?: Clock
    },
  ) {
    this.spanSink = spanSink
    this.config = config
    this.log = options?.log || (() => {})
    this.clock = options?.clock || wallClock
  }

  /**
   * Process a single event
   */
  async processEvent(event: Event): Promise<void> {
    const eventType = event.type as string
    const props = event.properties as Record<string, unknown>
    const info = props.info as Record<string, unknown> | undefined
    const sessionID = (props.sessionID as string) || (info?.id as string) || (props.id as string)

    if (eventType === "session.created") {
      await this.handleSessionCreated(info)
    } else if (eventType === "message.part.updated") {
      await this.handleMessagePartUpdated(props)
    } else if (eventType === "message.updated") {
      await this.handleMessageUpdated(props)
    } else if (eventType === "session.idle") {
      await this.handleSessionIdle(sessionID)
    } else if (eventType === "session.deleted") {
      await this.handleSessionDeleted(sessionID)
    } else if (eventType === "session.error") {
      await this.handleSessionError(props)
    } else if (eventType === "permission.updated" || eventType === "permission.asked") {
      this.handlePermissionRequest(props)
    } else if (eventType === "permission.replied") {
      await this.handlePermissionReplied(props)
    }
  }

  private permissionRecordFromProps(props: Record<string, unknown>): PermissionRequestRecord {
    const info = isRecord(props.info) ? props.info : {}
    const permission = isRecord(props.permission) ? props.permission : {}
    const source =
      Object.keys(permission).length > 0 ? permission : Object.keys(info).length > 0 ? info : props
    const toolRecord = isRecord(source.tool) ? source.tool : {}
    const id =
      typeof source.id === "string"
        ? source.id
        : typeof props.id === "string"
          ? props.id
          : typeof props.permissionID === "string"
            ? props.permissionID
            : undefined
    return {
      id,
      sessionID:
        typeof source.sessionID === "string"
          ? source.sessionID
          : typeof props.sessionID === "string"
            ? props.sessionID
            : typeof toolRecord.sessionID === "string"
              ? toolRecord.sessionID
              : undefined,
      callID:
        typeof source.callID === "string"
          ? source.callID
          : typeof toolRecord.callID === "string"
            ? toolRecord.callID
            : undefined,
      tool:
        typeof source.tool === "string"
          ? source.tool
          : typeof source.toolName === "string"
            ? source.toolName
            : typeof toolRecord.name === "string"
              ? toolRecord.name
              : typeof toolRecord.tool === "string"
                ? toolRecord.tool
                : undefined,
      input: source.input ?? source.args ?? toolRecord.input,
      title: typeof source.title === "string" ? source.title : undefined,
      type: typeof source.type === "string" ? source.type : undefined,
    }
  }

  private handlePermissionRequest(props: Record<string, unknown>): void {
    const record = this.permissionRecordFromProps(props)
    if (record.id !== undefined) {
      this.permissionRequests.set(record.id, record)
    }
  }

  private async handlePermissionReplied(props: Record<string, unknown>): Promise<void> {
    const reply = props.reply ?? props.response
    if (reply !== "reject" && reply !== "deny" && reply !== "denied") return

    const id =
      typeof props.id === "string"
        ? props.id
        : typeof props.permissionID === "string"
          ? props.permissionID
          : typeof props.permission_id === "string"
            ? props.permission_id
            : undefined
    const stored = id !== undefined ? this.permissionRequests.get(id) : undefined
    const direct = this.permissionRecordFromProps(props)
    const record: PermissionRequestRecord = {
      ...stored,
      ...Object.fromEntries(Object.entries(direct).filter(([, value]) => value !== undefined)),
    }
    if (id !== undefined) this.permissionRequests.delete(id)
    if (record.callID === undefined || record.tool === undefined) return

    await this.emitDeniedToolSpan(record, {
      permission_id: id ?? record.id,
      permission_type: record.type,
      title: record.title,
    })
  }

  private stateForPermission(record: PermissionRequestRecord): SessionState | undefined {
    if (record.sessionID !== undefined) {
      const direct = this.sessionStates.get(record.sessionID)
      if (direct?.currentTurnSpanId) return direct
    }
    if (record.callID !== undefined) {
      for (const state of this.sessionStates.values()) {
        if (
          state.currentTurnSpanId &&
          (state.toolCallArgs.has(record.callID) ||
            state.toolStartTimes.has(record.callID) ||
            state.toolCallMessageIds.has(record.callID))
        ) {
          return state
        }
      }
    }
    return undefined
  }

  private async emitDeniedToolSpan(
    record: PermissionRequestRecord,
    permissionMetadata: Record<string, unknown>,
  ): Promise<void> {
    if (record.callID === undefined || record.tool === undefined) return
    const state = this.stateForPermission(record)
    if (!state?.currentTurnSpanId) return

    state.deniedToolCallIds.add(record.callID)
    const toolArgs = state.toolCallArgs.get(record.callID) ?? record.input ?? permissionMetadata
    const title = record.title ?? record.tool
    const toolSpanId = generateUUID()
    const now = this.clock.nowSeconds()
    await this.spanSink.insertSpan({
      id: generateUUID(),
      span_id: toolSpanId,
      root_span_id: state.effectiveRootSpanId,
      span_parents: [state.currentTurnSpanId],
      input: toolArgs,
      metadata: {
        tool_name: record.tool,
        call_id: record.callID,
        tool_approval: "denied",
        ...permissionMetadata,
      },
      metrics: {
        start: state.toolStartTimes.get(record.callID) ?? now,
        end: now,
      },
      span_attributes: {
        name: skillToolSpanName(
          record.tool,
          title,
          toolArgs,
          this.formatToolName(record.tool, title),
        ),
        type: "tool",
      },
    })
  }

  /**
   * Process a chat.message hook call
   */
  async processChatMessage(
    sessionID: string,
    userMessage: string,
    model?: { providerID?: string; modelID?: string },
  ): Promise<void> {
    let state = this.sessionStates.get(sessionID)
    if (!state) {
      // session.created is not delivered to plugins for API-created sessions.
      // Initialize state lazily so API-created sessions are traced correctly.
      this.log("No state found for session, initializing lazily (API-created session)", {
        sessionID,
      })
      state = await this.initSessionStateLazily(sessionID)
    }

    // Finalize previous turn if exists
    if (state.currentTurnSpanId) {
      const prevTurnSpan: SpanData = {
        id: state.currentTurnSpanId,
        span_id: state.currentTurnSpanId,
        root_span_id: state.effectiveRootSpanId,
        output: state.currentOutput || undefined,
        metrics: {
          end: this.clock.nowSeconds(),
        },
        _is_merge: true,
      }
      await this.spanSink.insertSpan(prevTurnSpan)
    }

    // Create new turn span
    state.turnNumber++
    state.currentTurnSpanId = generateUUID()
    state.currentOutput = undefined
    state.currentInput = userMessage
    state.currentTurnExplicitSkillsCommand = isExplicitSkillsCommand(userMessage)
    state.currentTurnExplicitSkillNames = explicitSkillNamesFromUserMessage(userMessage)

    const now = this.clock.now()
    const nowSeconds = msToSeconds(now)
    state.currentTurnStartTime = now

    const turnSpan: SpanData = {
      id: state.currentTurnSpanId,
      span_id: state.currentTurnSpanId,
      root_span_id: state.effectiveRootSpanId,
      span_parents: [state.rootSpanId],
      created: new Date(now).toISOString(),
      input: userMessage || undefined,
      metadata: {
        turn_number: state.turnNumber,
        model: model ? model.modelID : undefined,
        ...explicitSkillRequestMetadata(state.currentTurnExplicitSkillNames),
      },
      metrics: {
        start: nowSeconds,
      },
      span_attributes: {
        name: `Turn ${state.turnNumber}`,
        type: "task",
      },
    }

    await this.spanSink.insertSpan(turnSpan)
    this.log("Created turn span", { turnNumber: state.turnNumber })
  }

  /**
   * Process a chat.message hook call with raw output (mirrors production hook signature).
   * Extracts user message from output.parts, handling undefined/missing parts gracefully.
   */
  async processChatMessageRaw(
    sessionID: string,
    output: { parts?: Array<{ type: string; text?: string }> } | undefined,
    model?: { providerID?: string; modelID?: string },
  ): Promise<void> {
    const userMessage =
      output?.parts
        ?.filter((p) => p.type === "text")
        .map((p) => p.text)
        .join("\n") || ""

    return this.processChatMessage(sessionID, userMessage, model)
  }

  /**
   * Capture the system prompt from experimental.chat.system.transform.
   * Joins the array with \n\n so the LLM span shows one system message.
   */
  async processSystemTransform(sessionID: string, system: string[]): Promise<void> {
    const state = this.sessionStates.get(sessionID)
    if (state && Array.isArray(system) && system.length > 0) {
      state.systemPrompt = system.join("\n\n")
      this.log("Captured system prompt", {
        sessionID,
        parts: system.length,
        length: state.systemPrompt.length,
      })
    }
  }

  /**
   * Lazily initialize session state and emit a root span for API-created sessions.
   * Called from processChatMessage when no state exists for a session — this happens
   * because OpenCode does not deliver session.created to plugins for sessions created
   * via the HTTP API (POST /sessions).
   */
  private async initSessionStateLazily(sessionID: string): Promise<SessionState> {
    const rootSpanId = generateUUID()
    const now = this.clock.now()
    const state: SessionState = {
      rootSpanId,
      effectiveRootSpanId: rootSpanId,
      turnNumber: 0,
      toolCallCount: 0,
      startTime: now,
      llmOutputParts: new Map(),
      llmToolCalls: new Map(),
      llmReasoningParts: new Map(),
      processedLlmMessages: new Set(),
      toolStartTimes: new Map(),
      toolCallMessageIds: new Map(),
      toolCallArgs: new Map(),
      toolCallOutputs: new Map(),
      toolCallErrors: new Map(),
      deniedToolCallIds: new Set(),
    }
    this.sessionStates.set(sessionID, state)

    const root_span: SpanData = {
      id: rootSpanId,
      span_id: rootSpanId,
      root_span_id: rootSpanId,
      created: new Date(now).toISOString(),
      metadata: {
        ...this.config.additionalMetadata,
        session_id: sessionID,
        workspace: this.config.worktree,
        directory: this.config.directory,
      },
      metrics: {
        start: now,
      },
      span_attributes: {
        name: `OpenCode: ${this.config.projectName}`,
        type: "task",
      },
    }

    await this.spanSink.insertSpan(root_span)
    this.log("Created root span via lazy init", { rootSpanId, sessionID })
    return state
  }

  /**
   * Process a tool.execute.before hook call
   */
  async processToolExecuteBefore(sessionID: string, callID: string, args?: unknown): Promise<void> {
    const state = this.sessionStates.get(sessionID)
    if (state) {
      state.toolStartTimes.set(callID, this.clock.nowSeconds())
      if (args !== undefined) {
        state.toolCallArgs.set(callID, args)
      }
    }
  }

  /**
   * Process a tool.execute.after hook call
   */
  async processToolExecuteAfter(
    sessionID: string,
    callID: string,
    tool: string,
    title: string,
    output: string | undefined | unknown,
    metadata: unknown,
  ): Promise<void> {
    const state = this.sessionStates.get(sessionID)
    if (!state?.currentTurnSpanId) {
      this.log("No state or turn for tool", { sessionID })
      return
    }

    state.toolCallCount++

    if (state.deniedToolCallIds.has(callID)) {
      state.deniedToolCallIds.delete(callID)
      state.toolStartTimes.delete(callID)
      state.toolCallArgs.delete(callID)
      state.toolCallOutputs.delete(callID)
      state.toolCallErrors.delete(callID)
      state.toolCallMessageIds.delete(callID)
      this.log("Skipping tool.execute.after for denied tool call", { callID })
      return
    }

    const startTime = state.toolStartTimes.get(callID)
    state.toolStartTimes.delete(callID)

    // Get tool args captured in processToolExecuteBefore
    const toolArgs = state.toolCallArgs.get(callID)
    state.toolCallArgs.delete(callID)

    // Get tool output captured from message.part.updated completed state
    const capturedOutput = state.toolCallOutputs.get(callID)
    state.toolCallOutputs.delete(callID)
    const capturedError = state.toolCallErrors.get(callID)
    state.toolCallErrors.delete(callID)

    // Look up reasoning for this tool call via messageId
    const messageId = state.toolCallMessageIds.get(callID)
    const reasoning = messageId ? state.llmReasoningParts.get(messageId) : undefined
    state.toolCallMessageIds.delete(callID)

    const toolSpanId = generateUUID()
    const endTime = this.clock.nowSeconds()
    // Prefer output captured from message.part.updated, fall back to hook output param.
    // MCP tools return { content: [{ type: "text", text: "..." }] } instead of a plain
    // output value, so extract the text from content[] as a last resort.
    const rawOutput =
      capturedOutput !== undefined
        ? capturedOutput
        : capturedError !== undefined
          ? capturedError
          : extractToolOutput(output)
    const formattedName = this.formatToolName(tool, title)
    const trigger = skillLoadTrigger(
      state.currentTurnExplicitSkillsCommand,
      state.currentTurnExplicitSkillNames,
      tool,
      toolArgs,
    )
    const skillName = skillNameFromArgs(toolArgs)
    if (
      trigger === "explicit" &&
      skillName !== undefined &&
      !state.currentTurnExplicitSkillNames?.includes(skillName)
    ) {
      state.currentTurnExplicitSkillNames = [
        ...(state.currentTurnExplicitSkillNames ?? []),
        skillName,
      ]
      const explicitSkillMetadata = explicitSkillRequestMetadata(
        state.currentTurnExplicitSkillNames,
      )
      if (explicitSkillMetadata !== undefined) {
        await this.spanSink.insertSpan({
          id: state.currentTurnSpanId,
          span_id: state.currentTurnSpanId,
          root_span_id: state.effectiveRootSpanId,
          metadata: explicitSkillMetadata,
          _is_merge: true,
        })
      }
    }
    const toolSpan: SpanData = {
      id: generateUUID(),
      span_id: toolSpanId,
      root_span_id: state.effectiveRootSpanId,
      span_parents: [state.currentTurnSpanId],
      input: toolArgs !== undefined ? toolArgs : metadata,
      output: typeof rawOutput === "string" ? rawOutput.substring(0, 10000) : rawOutput,
      metadata: {
        tool_name: tool,
        call_id: callID,
        tool_approval: "approved",
        title,
        reasoning: reasoning || undefined,
        ...skillLoadMetadata(tool, toolArgs),
        ...(trigger !== undefined ? { skill_load_trigger: trigger } : {}),
      },
      ...(capturedError !== undefined ? { error: capturedError } : {}),
      metrics: {
        start: startTime,
        end: endTime,
      },
      span_attributes: {
        name: skillToolSpanName(tool, title, toolArgs, formattedName),
        type: "tool",
      },
    }

    await this.spanSink.insertSpan(toolSpan)
    this.log("Created tool span", { tool, callID })
  }

  private async handleSessionCreated(
    sessionInfo: Record<string, unknown> | undefined,
  ): Promise<void> {
    const sessionID = sessionInfo?.id as string
    const parentSessionID = sessionInfo?.parentID as string

    // Handle child session (subagent) - link to parent trace
    if (sessionID && parentSessionID) {
      const parentState = this.sessionStates.get(parentSessionID)
      if (parentState) {
        // Extract subagent title from session title
        // OpenCode format: "{description} (@{agent.name} subagent)"
        // We want: "{agent.name}: {description}"
        const sessionTitle = sessionInfo?.title as string | undefined
        let subagentTitle = sessionTitle || "Subagent"
        if (sessionTitle) {
          const match = sessionTitle.match(/^(.+?)\s+\(@(\w+)\s+subagent\)$/)
          if (match) {
            const [, description, agentType] = match
            subagentTitle = `${agentType}: ${description}`
          }
        }

        this.log("Child session created, linking to parent", {
          sessionID,
          parentSessionID,
          parentRootSpanId: parentState.rootSpanId,
          parentTurnSpanId: parentState.currentTurnSpanId,
          subagentTitle,
        })

        // Create child session state with parent linking info
        const rootSpanId = generateUUID()
        const childState: SessionState = {
          rootSpanId,
          effectiveRootSpanId: parentState.effectiveRootSpanId, // Use parent's effective root for trace linking
          turnNumber: 0,
          toolCallCount: 0,
          startTime: this.clock.now(),
          parentSessionId: parentSessionID,
          parentRootSpanId: parentState.effectiveRootSpanId,
          parentTurnSpanId: parentState.currentTurnSpanId,
          subagentTitle,
          llmOutputParts: new Map(),
          llmToolCalls: new Map(),
          llmReasoningParts: new Map(),
          processedLlmMessages: new Set(),
          toolStartTimes: new Map(),
          toolCallMessageIds: new Map(),
          toolCallArgs: new Map(),
          toolCallOutputs: new Map(),
          toolCallErrors: new Map(),
          deniedToolCallIds: new Set(),
        }
        this.sessionStates.set(sessionID, childState)

        // Create root span for child session, linked to parent's trace
        const root_span: SpanData = {
          id: rootSpanId,
          span_id: rootSpanId,
          root_span_id: parentState.effectiveRootSpanId, // Link to parent's trace
          span_parents: parentState.currentTurnSpanId ? [parentState.currentTurnSpanId] : undefined, // Child of parent's turn
          created: new Date(childState.startTime).toISOString(),
          metadata: {
            session_id: sessionID,
            parent_session_id: parentSessionID,
            is_subagent: true,
          },
          metrics: {
            start: msToSeconds(childState.startTime),
          },
          span_attributes: {
            name: subagentTitle,
            type: "task",
          },
        }

        await this.spanSink.insertSpan(root_span)
        this.log("Created child session root span", { rootSpanId })
        return
      }
    }

    // Handle regular (parent) session creation
    if (!sessionID) {
      this.log("No session ID found, skipping trace creation")
      return
    }

    const sessionKey = String(sessionID)

    // Guard: skip if state already exists (lazy-initialized when chat.message arrived
    // before session.created for API-created sessions).
    if (this.sessionStates.has(sessionKey)) {
      this.log("Session state already exists (lazy-initialized), skipping session.created init", {
        sessionKey,
      })
      return
    }

    const rootSpanId = generateUUID()
    const state: SessionState = {
      rootSpanId,
      effectiveRootSpanId: rootSpanId, // For root sessions, effective root is self
      turnNumber: 0,
      toolCallCount: 0,
      startTime: this.clock.now(),
      llmOutputParts: new Map(),
      llmToolCalls: new Map(),
      llmReasoningParts: new Map(),
      processedLlmMessages: new Set(),
      toolStartTimes: new Map(),
      toolCallMessageIds: new Map(),
      toolCallArgs: new Map(),
      toolCallOutputs: new Map(),
      toolCallErrors: new Map(),
      deniedToolCallIds: new Set(),
    }
    this.sessionStates.set(sessionKey, state)

    const root_span: SpanData = {
      id: rootSpanId,
      span_id: rootSpanId,
      root_span_id: rootSpanId,
      created: new Date(state.startTime).toISOString(),
      metadata: {
        ...this.config.additionalMetadata,
        session_id: sessionKey,
        workspace: this.config.worktree,
        directory: this.config.directory,
      },
      metrics: {
        start: msToSeconds(state.startTime),
      },
      span_attributes: {
        name: `OpenCode: ${this.config.projectName}`,
        type: "task",
      },
    }

    await this.spanSink.insertSpan(root_span)
    this.log("Created root span", { rootSpanId })
  }

  private async handleMessagePartUpdated(props: Record<string, unknown>): Promise<void> {
    const part = props.part as Record<string, unknown> | undefined
    const partSessionID = part?.sessionID as string
    const messageId = part?.messageID as string

    if (!partSessionID || !part) {
      this.log("message.part.updated: no sessionID or part")
      return
    }

    const state = this.sessionStates.get(partSessionID)
    if (!state) {
      this.log("message.part.updated: no state for session", { partSessionID })
      return
    }

    // Track text content
    if (part.type === "text" && part.text) {
      const text = part.text as string
      const time = part.time as Record<string, unknown> | undefined

      if (messageId) {
        state.llmOutputParts.set(messageId, text)
        this.log("Tracking LLM output part", { messageId, textLength: text.length })
      }

      if (time?.end && state.currentTurnSpanId) {
        state.currentOutput = text
      }
    }
    // Track tool calls for LLM span output
    else if (part.type === "tool" && messageId) {
      const callID = part.callID as string
      const tool = part.tool as string
      const partState = part.state as Record<string, unknown> | undefined
      const input = partState?.input as Record<string, unknown> | undefined

      if (callID && tool && input) {
        let toolCalls = state.llmToolCalls.get(messageId)
        if (!toolCalls) {
          toolCalls = []
          state.llmToolCalls.set(messageId, toolCalls)
        }

        const existingIndex = toolCalls.findIndex((tc) => tc.id === callID)
        const toolCall = {
          id: callID,
          type: "function" as const,
          function: {
            name: tool,
            arguments: JSON.stringify(input),
          },
        }

        if (existingIndex >= 0) {
          toolCalls[existingIndex] = toolCall
        } else {
          toolCalls.push(toolCall)
        }

        // Store messageId for this callID so we can look up reasoning later
        state.toolCallMessageIds.set(callID, messageId)

        // Capture tool output when state is completed
        if (partState?.status === "completed" && partState?.output !== undefined) {
          state.toolCallOutputs.set(callID, partState.output)
          this.log("Captured tool output from completed state", { callID })
        }
        if (partState?.status === "error" && partState.error !== undefined) {
          state.toolCallErrors.set(callID, conciseErrorText(partState.error))
          this.log("Captured tool error from error state", { callID })
        }

        this.log("Tracking LLM tool call", { messageId, callID, tool })
      }
    }
    // Track reasoning/thinking content for LLM spans
    else if (part.type === "reasoning" && messageId) {
      const text = part.text as string
      if (text) {
        state.llmReasoningParts.set(messageId, text)
        this.log("Tracking LLM reasoning part", { messageId, textLength: text.length })
      }
    }
  }

  private async handleMessageUpdated(props: Record<string, unknown>): Promise<void> {
    const messageInfo = props.info as Record<string, unknown> | undefined
    if (!messageInfo) {
      this.log("message.updated: no info in props")
      return
    }

    const role = messageInfo.role as string
    if (role !== "assistant") {
      this.log("message.updated: skipping non-assistant message", { role })
      return
    }

    const msgSessionID = messageInfo.sessionID as string
    const messageId = messageInfo.id as string
    const time = messageInfo.time as Record<string, unknown> | undefined

    if (!msgSessionID || !messageId) {
      this.log("message.updated: missing sessionID or messageId", { msgSessionID, messageId })
      return
    }

    const state = this.sessionStates.get(msgSessionID)
    if (!state) {
      this.log("message.updated: no state for session", { msgSessionID })
      return
    }

    // Only create LLM span when message is completed
    if (!time?.completed) {
      this.log("message.updated: message not completed yet", { messageId, time })
      return
    }

    // Skip if we already processed this message
    if (state.processedLlmMessages.has(messageId)) {
      this.log("message.updated: already processed", { messageId })
      return
    }

    // Need a current turn to attach the LLM span to
    if (!state.currentTurnSpanId) {
      this.log("message.updated: no current turn span", { messageId })
      return
    }

    // Mark as processed
    state.processedLlmMessages.add(messageId)

    // Extract token info. OpenCode reports input tokens exclusive of cache
    // buckets, so normalize prompt_tokens to Braintrust's inclusive convention
    // by adding cache read/write tokens back in.
    const tokens = messageInfo.tokens as Record<string, unknown> | undefined
    const cacheTokens = tokens?.cache as Record<string, unknown> | undefined
    const inputTokens = (tokens?.input as number) || 0
    const outputTokens = (tokens?.output as number) || 0
    const reasoningTokens = (tokens?.reasoning as number) || 0
    const cacheReadTokens = (cacheTokens?.read as number) || 0
    const cacheWriteTokens = (cacheTokens?.write as number) || 0
    const promptTokens = inputTokens + cacheReadTokens + cacheWriteTokens
    const totalTokens = promptTokens + outputTokens + reasoningTokens

    // Extract model info
    const providerID = (messageInfo.providerID as string) || "unknown"
    const modelID = (messageInfo.modelID as string) || "unknown"
    const modelName = `${providerID}/${modelID}`

    // Get output text, tool calls, and reasoning from tracked parts
    const outputText = state.llmOutputParts.get(messageId) || ""
    const toolCalls = state.llmToolCalls.get(messageId)
    const reasoningText = state.llmReasoningParts.get(messageId)

    // Build assistant message object - include tool_calls and reasoning if present
    const assistantMessage: Record<string, unknown> = {
      role: "assistant",
      content: outputText,
    }
    if (toolCalls && toolCalls.length > 0) {
      assistantMessage.tool_calls = toolCalls
    }
    if (reasoningText) {
      // Braintrust expects reasoning as an array of objects with id and content
      assistantMessage.reasoning = [{ id: "reasoning", content: reasoningText }]
    }

    // Build input/output in Braintrust's expected format
    const llmInput: Array<Record<string, unknown>> = []
    if (state.systemPrompt) {
      llmInput.push({ role: "system", content: state.systemPrompt })
    }
    if (state.currentInput) {
      llmInput.push({ role: "user", content: state.currentInput })
    }
    const llmOutput = [assistantMessage]

    // Create LLM span
    const llmSpanId = generateUUID()
    const llmSpan: SpanData = {
      id: llmSpanId,
      span_id: llmSpanId,
      root_span_id: state.effectiveRootSpanId,
      span_parents: [state.currentTurnSpanId],
      created: new Date(time.created as number).toISOString(),
      input: llmInput.length > 0 ? llmInput : undefined,
      output: llmOutput,
      metrics: {
        start: msToSeconds(time.created as number),
        end: msToSeconds(time.completed as number),
        prompt_tokens: promptTokens,
        completion_tokens: outputTokens,
        tokens: totalTokens,
        prompt_cached_tokens: cacheReadTokens || undefined,
        prompt_cache_creation_tokens: cacheWriteTokens || undefined,
        reasoning_tokens: reasoningTokens || undefined,
      },
      metadata: {
        model: modelID,
        provider: providerID,
        message_id: messageId,
      },
      span_attributes: {
        name: modelName,
        type: "llm",
      },
    }

    await this.spanSink.insertSpan(llmSpan)
    this.log("Created LLM span", { messageId, modelName, tokens: totalTokens })
  }

  private async handleSessionIdle(sessionID: string | undefined): Promise<void> {
    if (!sessionID) {
      this.log("session.idle but no session ID found")
      return
    }

    const sessionKey = String(sessionID)
    const state = this.sessionStates.get(sessionKey)

    if (state) {
      const now = this.clock.nowSeconds()
      const isChildSession = !!state.parentSessionId

      // Close current turn span if exists
      if (state.currentTurnSpanId) {
        const turnSpan: SpanData = {
          id: state.currentTurnSpanId,
          span_id: state.currentTurnSpanId,
          root_span_id: state.effectiveRootSpanId,
          output: state.currentOutput || undefined,
          metrics: {
            end: now,
          },
          _is_merge: true,
        }
        await this.spanSink.insertSpan(turnSpan)
        state.currentTurnSpanId = undefined
        state.currentInput = undefined
        state.currentOutput = undefined
        state.currentTurnStartTime = undefined
        this.log("Turn span closed", { sessionKey, turnNumber: state.turnNumber })
      }

      // For child sessions (subagents), also close the root span since they don't get session.deleted
      if (isChildSession && state.rootSpanId) {
        this.log("Closing child session root span on idle", {
          sessionKey,
          parentSessionId: state.parentSessionId,
          rootSpanId: state.rootSpanId,
        })

        const rootSpan: SpanData = {
          id: state.rootSpanId,
          span_id: state.rootSpanId,
          root_span_id: state.effectiveRootSpanId,
          metrics: {
            end: now,
          },
          metadata: {
            total_turns: state.turnNumber,
            total_tool_calls: state.toolCallCount,
          },
          _is_merge: true,
        }
        await this.spanSink.insertSpan(rootSpan)

        // Clean up child session state
        this.sessionStates.delete(sessionKey)
        this.log("Child session closed", { sessionKey })
      }
    }
  }

  private async handleSessionDeleted(sessionID: string | undefined): Promise<void> {
    if (!sessionID) {
      this.log("session.deleted but no session ID found")
      return
    }

    const sessionKey = String(sessionID)
    const state = this.sessionStates.get(sessionKey)

    if (state) {
      const now = this.clock.nowSeconds()

      // Close current turn span if exists
      if (state.currentTurnSpanId) {
        const turnSpan: SpanData = {
          id: state.currentTurnSpanId,
          span_id: state.currentTurnSpanId,
          root_span_id: state.effectiveRootSpanId,
          output: state.currentOutput || undefined,
          metrics: {
            end: now,
          },
          _is_merge: true,
        }
        await this.spanSink.insertSpan(turnSpan)
      }

      // Close root span
      const span: SpanData = {
        id: state.rootSpanId,
        span_id: state.rootSpanId,
        root_span_id: state.effectiveRootSpanId,
        metrics: {
          end: now,
        },
        metadata: {
          total_turns: state.turnNumber,
          total_tool_calls: state.toolCallCount,
        },
        _is_merge: true,
      }
      await this.spanSink.insertSpan(span)
      this.sessionStates.delete(sessionKey)
      this.log("Session span closed", { sessionKey })
    }
  }

  private async handleSessionError(props: Record<string, unknown>): Promise<void> {
    const sessionID = props.sessionID as string
    if (!sessionID) {
      this.log("session.error but no session ID found")
      return
    }

    const sessionKey = String(sessionID)
    const state = this.sessionStates.get(sessionKey)

    if (state) {
      const now = this.clock.nowSeconds()

      // Extract error info from event.properties
      // Error structure: { name: "ErrorType", data: { message?: string, ... } }
      const errorObj = props.error as { name?: string; data?: { message?: string } } | undefined
      const errorName = errorObj?.name || "UnknownError"
      const errorMessage = errorObj?.data?.message || errorName

      // Format error string similar to Braintrust SDK pattern: "message\n\ntype: ErrorType"
      const errorString = `${errorMessage}\n\ntype: ${errorName}`

      this.log("Handling session error", { sessionKey, errorName, errorMessage })

      // Close current turn span with error if exists
      if (state.currentTurnSpanId) {
        const turnSpan: SpanData = {
          id: state.currentTurnSpanId,
          span_id: state.currentTurnSpanId,
          root_span_id: state.effectiveRootSpanId,
          output: state.currentOutput || undefined,
          error: errorString,
          metrics: { end: now },
          _is_merge: true,
        }
        await this.spanSink.insertSpan(turnSpan)
      }

      // Close root span with error and metadata
      const rootSpan: SpanData = {
        id: state.rootSpanId,
        span_id: state.rootSpanId,
        root_span_id: state.effectiveRootSpanId,
        error: errorString,
        metrics: { end: now },
        metadata: {
          total_turns: state.turnNumber,
          total_tool_calls: state.toolCallCount,
          error_type: errorName,
        },
        _is_merge: true,
      }
      await this.spanSink.insertSpan(rootSpan)

      // Clean up session state
      this.sessionStates.delete(sessionKey)
      this.log("Session error handled", { sessionKey, errorName, errorMessage })
    }
  }

  private formatToolName(tool: string, title?: string): string {
    if (title) {
      let displayTitle = title

      // For file operations, show just the filename instead of full path
      if ((tool === "read" || tool === "edit") && title.includes("/")) {
        const parts = title.split("/")
        displayTitle = parts[parts.length - 1] || title
      }

      // Truncate long titles
      const shortTitle =
        displayTitle.length > 50 ? `${displayTitle.substring(0, 47)}...` : displayTitle
      return `${tool}: ${shortTitle}`
    }
    return tool
  }
}

/**
 * Normalize the output value from a tool.execute.after hook result.
 *
 * Built-in tools return a plain string/object in `result.output`.
 * MCP tools return `{ content: [{ type: "text", text: "..." }, ...] }` with no
 * `output` field.  This function extracts a usable output value from either form.
 */
export function extractToolOutput(output: unknown): unknown {
  if (output === undefined || output === null) return undefined

  // Already a primitive — use as-is
  if (typeof output !== "object") return output

  const obj = output as Record<string, unknown>

  // MCP content array: { content: [{ type: "text", text: "..." }] }
  if (Array.isArray(obj.content)) {
    const textParts = (obj.content as Array<{ type?: string; text?: string }>)
      .filter((c) => c.type === "text" && typeof c.text === "string")
      .map((c) => c.text as string)
    if (textParts.length > 0) return textParts.join("\n")
    // Non-text content (images etc.) — return the array as-is
    return obj.content
  }

  // Plain object with an output field — unwrap it
  if (obj.output !== undefined) return obj.output

  // Anything else — pass through
  return output
}

/**
 * Helper function to process a list of events and return the resulting span tree
 */
export async function processEventsToSpans(
  events: Event[],
  config: EventProcessorConfig,
  chatMessages?: Array<{
    sessionID: string
    userMessage: string
    model?: { providerID?: string; modelID?: string }
  }>,
  _toolCalls?: Array<{
    sessionID: string
    callID: string
    tool: string
    title: string
    output: string
    metadata: unknown
  }>,
): Promise<SpanData[]> {
  const { TestSpanCollector } = await import("./span-sink")
  const collector = new TestSpanCollector()
  const processor = new EventProcessor(collector, config)

  // Process events in order, interleaving chat messages and tool calls as needed
  for (const event of events) {
    await processor.processEvent(event)

    // Check if we need to process a chat message after session.created
    if (event.type === "session.created" && chatMessages) {
      const props = event.properties as Record<string, unknown>
      const info = props.info as Record<string, unknown> | undefined
      const sessionID = (props.sessionID as string) || (info?.id as string)

      const chatMsg = chatMessages.find((cm) => cm.sessionID === sessionID)
      if (chatMsg) {
        await processor.processChatMessage(chatMsg.sessionID, chatMsg.userMessage, chatMsg.model)
      }
    }
  }

  return collector.getSpans()
}
