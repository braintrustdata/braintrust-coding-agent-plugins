import { hostname, userInfo } from "node:os";
import { basename, dirname, resolve } from "node:path";
import {
  VERSION as PI_VERSION,
  type AgentEndEvent,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { BraintrustClient, type BraintrustSpanHandle } from "./client.ts";
import { createLogger, loadConfig } from "./config.ts";
import { createStateStore } from "./state.ts";
import type {
  AgentMessageLike,
  AssistantMessageLike,
  ConfigIssue,
  ImageLike,
  NormalizedAgentMessage,
  NormalizedAssistantMessage,
  TraceConfig,
} from "./types.ts";
import { EXTENSION_VERSION } from "./version.ts";
import {
  buildTurnInput,
  extractErrorText,
  formatToolSpanName,
  generateUuid,
  gitMetadataForCwd,
  isPlainObject,
  normalizeAssistantMessage,
  normalizeContextMessages,
  normalizeToolResult,
  repoSlugForCwd,
  rootSpanName,
  sessionKeyFor,
  shortHash,
  toUnixSeconds,
  truncateValue,
} from "./utils.ts";

const TRACING_STATUS_KEY = "braintrust-tracing";
const TRACING_WIDGET_KEY = "braintrust-trace-link";

interface SessionDescriptor {
  sessionFile: string | undefined;
  sessionId: string | undefined;
  sessionKey: string;
}

interface ProviderResponseMetadata {
  status?: number;
  headers?: Record<string, string>;
}

interface PendingLlmCall {
  startedAt: number;
  input: NormalizedAgentMessage[];
  activeToolNames?: string[];
  activatedToolNames: string[];
  modelMetadata: Record<string, unknown>;
  providerRequest?: Record<string, unknown>;
  providerResponse?: ProviderResponseMetadata;
  firstTokenAt?: number;
  firstThinkingAt?: number;
  lastThinkingAt?: number;
  firstTextAt?: number;
}

interface TrackedToolStart {
  startedAt: number;
  args: unknown;
  toolName: string;
}

interface SkillLoadMetadata {
  skill_name: string;
  skill_path?: string;
}

interface ExplicitSkillRequestMetadata {
  loaded_skill_names: string[];
  loaded_skills: Array<{ name: string }>;
}

interface ActiveCompaction {
  spanId: string;
  span?: BraintrustSpanHandle;
  startedAt: number;
  input: unknown;
}

interface ActiveBranchSummary {
  spanId: string;
  span?: BraintrustSpanHandle;
  startedAt: number;
  input: unknown;
}

interface PendingInputEvent {
  text?: string;
  source?: string;
  streamingBehavior?: string;
  imageCount?: number;
}

interface ActiveTurn {
  spanId: string;
  span?: BraintrustSpanHandle;
  prompt: string;
  explicitSkillNames: string[];
  llmCalls: PendingLlmCall[];
  llmCallCount: number;
  toolCallCount: number;
  toolStarts: Map<string, TrackedToolStart>;
  toolParentSpanIds: Map<string, string>;
  activatedToolNames: Set<string>;
  lastAssistantMessage?: AssistantMessageLike;
  lastOutput?: NormalizedAssistantMessage;
  error?: string;
  thinkingLevel?: string;
}

interface ActiveSession {
  sessionKey: string;
  sessionFile: string | undefined;
  sessionId: string | undefined;
  openedVia?: string;
  parentSessionFile?: string;
  rootSpanId?: string;
  rootSpan?: BraintrustSpanHandle;
  rootSpanRecordId?: string;
  traceRootSpanId?: string;
  parentSpanId?: string;
  traceUrl?: string;
  traceUrlPromise?: Promise<void>;
  startedAt?: number;
  totalTurns: number;
  totalToolCalls: number;
  thinkingLevel?: string;
  currentTurn?: ActiveTurn;
  currentCompaction?: ActiveCompaction;
  currentBranchSummary?: ActiveBranchSummary;
}

let pendingInputEvent: PendingInputEvent | undefined;

function stringArg(args: unknown, keys: readonly string[]): string | undefined {
  if (!isPlainObject(args)) return undefined;
  for (const key of keys) {
    const value = args[key];
    if (typeof value === "string" && value.trim() !== "") return value;
  }
  return undefined;
}

function skillLoadFromRead(toolName: string, args: unknown): SkillLoadMetadata | undefined {
  if (toolName !== "read") return undefined;
  const path = stringArg(args, ["path", "filePath", "file_path", "file"]);
  if (!path || basename(path).toLowerCase() !== "skill.md") return undefined;

  const skillName = basename(dirname(path));
  return {
    skill_name: skillName,
    skill_path: path,
  };
}

function normalizeExplicitSkillName(name: string): string | undefined {
  const normalized = name
    .trim()
    .replace(/^[/$]+/, "")
    .replace(/[),.;:]+$/, "");
  return normalized ? normalized : undefined;
}

function explicitSkillRequestMetadata(
  names: readonly string[],
): ExplicitSkillRequestMetadata | undefined {
  const seen = new Set<string>();
  const loaded_skill_names: string[] = [];
  for (const name of names) {
    const normalized = normalizeExplicitSkillName(name);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    loaded_skill_names.push(normalized);
  }
  if (loaded_skill_names.length === 0) return undefined;
  return {
    loaded_skill_names,
    loaded_skills: loaded_skill_names.map((name) => ({ name })),
  };
}

function explicitSkillNamesFromPiInput(input: PendingInputEvent | undefined): string[] {
  if (!input?.text) return [];
  const names: string[] = [];
  for (const match of input.text.matchAll(/(?:^|\s)\/skill:([^\s]+)/g)) {
    const name = normalizeExplicitSkillName(match[1] ?? "");
    if (name) names.push(name);
  }
  return explicitSkillRequestMetadata(names)?.loaded_skill_names ?? [];
}

function skillLoadTriggerForTurn(
  turn: ActiveTurn,
  skillLoad: SkillLoadMetadata | undefined,
): "explicit" | undefined {
  if (!skillLoad) return undefined;
  return turn.explicitSkillNames.includes(skillLoad.skill_name) ? "explicit" : undefined;
}

function hasSessionRoot(session: ActiveSession | undefined): session is ActiveSession & {
  rootSpanId: string;
  traceRootSpanId: string;
  startedAt: number;
} {
  return Boolean(
    session &&
    typeof session.rootSpanId === "string" &&
    typeof session.traceRootSpanId === "string" &&
    typeof session.startedAt === "number",
  );
}

function getUsername(): string {
  try {
    return userInfo().username;
  } catch {
    return process.env.USER || process.env.USERNAME || "unknown";
  }
}

function getSessionDescriptor(ctx: ExtensionContext): SessionDescriptor {
  const sessionFile = ctx.sessionManager.getSessionFile();
  const sessionId = ctx.sessionManager.getSessionId();
  const sessionKey = sessionKeyFor(
    sessionFile ? resolve(sessionFile) : undefined,
    sessionId,
    ctx.cwd,
  );

  return {
    sessionFile,
    sessionId,
    sessionKey,
  };
}

function safeModelName(model: unknown): string | undefined {
  if (!model) return undefined;
  if (typeof model === "string") return model;
  if (!isPlainObject(model)) return undefined;
  const provider = model.provider ?? model.providerId ?? model.providerID;
  const id = model.id ?? model.modelId ?? model.modelID;
  if (typeof provider === "string" && typeof id === "string") return `${provider}/${id}`;
  if (typeof id === "string") return id;
  return undefined;
}

function modelTraceMetadata(model: unknown): Record<string, unknown> {
  if (!isPlainObject(model)) return {};

  const metadata: Record<string, unknown> = {};
  if (typeof model.api === "string") metadata["pi_coding_agent.api"] = model.api;
  if (typeof model.name === "string") metadata["pi_coding_agent.model_name"] = model.name;
  if (typeof model.reasoning === "boolean") metadata.model_supports_reasoning = model.reasoning;
  if (typeof model.contextWindow === "number") metadata.model_context_window = model.contextWindow;
  if (typeof model.maxTokens === "number") metadata.model_max_tokens = model.maxTokens;

  if (isPlainObject(model.thinkingLevelMap)) {
    metadata.supported_thinking_levels = Object.entries(model.thinkingLevelMap)
      .filter(([, value]) => value !== null)
      .map(([level]) => level);
  }

  if (isPlainObject(model.compat) && typeof model.compat.deferredToolsMode === "string") {
    metadata.deferred_tools_mode = model.compat.deferredToolsMode;
  }

  return metadata;
}

function activeToolNames(pi: ExtensionAPI): string[] | undefined {
  try {
    if (typeof pi.getActiveTools !== "function") return undefined;
    return pi.getActiveTools();
  } catch {
    return undefined;
  }
}

function stringProperty(
  value: Record<string, unknown>,
  keys: readonly string[],
): string | undefined {
  for (const key of keys) {
    const item = value[key];
    if (typeof item === "string" && item.trim()) return item;
  }
  return undefined;
}

function metadataForPendingInput(input: PendingInputEvent | undefined): Record<string, unknown> {
  if (!input) return {};
  return {
    input_source: input.source,
    input_streaming_behavior: input.streamingBehavior ?? "idle",
    input_image_count: input.imageCount,
    raw_input: input.text === undefined ? undefined : truncateValue(input.text),
  };
}

function responseModelName(message: AssistantMessageLike): string | undefined {
  return stringProperty(message as unknown as Record<string, unknown>, [
    "responseModel",
    "routedModel",
    "resolvedModel",
    "actualModel",
    "concreteModel",
    "outputModel",
  ]);
}

function providerResponseMetadata(event: unknown): ProviderResponseMetadata | undefined {
  if (!isPlainObject(event)) return undefined;
  const metadata: ProviderResponseMetadata = {};
  if (typeof event.status === "number") metadata.status = event.status;

  const headers = event.headers;
  if (isPlainObject(headers)) {
    const allowedHeaders: Record<string, string> = {};
    const safeHeaders = new Set(["retry-after", "x-request-id", "request-id", "cf-ray"]);
    for (const [key, value] of Object.entries(headers)) {
      const normalizedKey = key.toLowerCase();
      if (!normalizedKey.startsWith("x-ratelimit-") && !safeHeaders.has(normalizedKey)) {
        continue;
      }
      if (typeof value === "string") allowedHeaders[normalizedKey] = value;
      else if (typeof value === "number" || typeof value === "boolean") {
        allowedHeaders[normalizedKey] = String(value);
      }
    }
    if (Object.keys(allowedHeaders).length > 0) metadata.headers = allowedHeaders;
  }

  return metadata.status !== undefined || metadata.headers ? metadata : undefined;
}

function providerRequestMetadata(event: unknown): Record<string, unknown> | undefined {
  if (!isPlainObject(event) || !isPlainObject(event.payload)) return undefined;
  const payload = event.payload;
  const metadata: Record<string, unknown> = {};

  if (typeof payload.model === "string") metadata.provider_request_model = payload.model;
  const maxTokens = numberProperty(payload, ["max_tokens", "max_completion_tokens"]);
  if (maxTokens !== undefined) metadata.provider_request_max_tokens = maxTokens;
  if (typeof payload.temperature === "number") {
    metadata.provider_request_temperature = payload.temperature;
  }

  const thinking = isPlainObject(payload.thinking) ? payload.thinking : undefined;
  const reasoning = isPlainObject(payload.reasoning) ? payload.reasoning : undefined;
  const outputConfig = isPlainObject(payload.output_config) ? payload.output_config : undefined;
  const chatTemplate = isPlainObject(payload.chat_template_kwargs)
    ? payload.chat_template_kwargs
    : undefined;

  const thinkingType = stringProperty(thinking ?? {}, ["type"]);
  const thinkingEffort =
    stringProperty(outputConfig ?? {}, ["effort"]) ??
    stringProperty(reasoning ?? {}, ["effort"]) ??
    stringProperty(payload, ["reasoning_effort"]);
  const thinkingBudget = numberProperty(thinking, ["budget_tokens", "budgetTokens"]);

  if (thinkingType) metadata.effective_thinking_type = thinkingType;
  if (thinkingEffort) metadata.effective_thinking_effort = thinkingEffort;
  if (thinkingBudget !== undefined) {
    metadata.effective_thinking_budget_tokens = thinkingBudget;
    metadata.effective_thinking_uses_token_budget = true;
  } else if (thinkingType === "adaptive") {
    metadata.effective_thinking_uses_token_budget = false;
  }

  const thinkingEnabled =
    typeof payload.enable_thinking === "boolean"
      ? payload.enable_thinking
      : typeof reasoning?.enabled === "boolean"
        ? reasoning.enabled
        : typeof chatTemplate?.enable_thinking === "boolean"
          ? chatTemplate.enable_thinking
          : thinkingType === "adaptive" || thinkingType === "enabled"
            ? true
            : thinkingType === "disabled"
              ? false
              : undefined;
  if (thinkingEnabled !== undefined) metadata.effective_thinking_enabled = thinkingEnabled;

  const immediateTools = Array.isArray(payload.tools) ? payload.tools : [];
  const deferredAnthropicTools = immediateTools.filter(
    (tool) => isPlainObject(tool) && tool.defer_loading === true,
  ).length;
  let deferredKimiTools = 0;
  if (Array.isArray(payload.messages)) {
    for (const message of payload.messages) {
      if (isPlainObject(message) && Array.isArray(message.tools)) {
        deferredKimiTools += message.tools.length;
      }
    }
  }
  const deferredToolCount = deferredAnthropicTools + deferredKimiTools;
  const toolCount = immediateTools.length + deferredKimiTools;
  if (toolCount > 0) metadata.provider_request_tool_count = toolCount;
  if (deferredToolCount > 0) {
    metadata.provider_request_deferred_tool_count = deferredToolCount;
  }

  return Object.keys(metadata).length > 0 ? metadata : undefined;
}

function addedToolNames(result: unknown): string[] {
  if (!isPlainObject(result) || !Array.isArray(result.addedToolNames)) return [];
  return [
    ...new Set(result.addedToolNames.filter((name): name is string => typeof name === "string")),
  ];
}

function tokenMetric(value: unknown): number | undefined {
  return typeof value === "number" &&
    Number.isFinite(value) &&
    value >= 0 &&
    Number.isInteger(value)
    ? value
    : undefined;
}

function nonNegativeMetric(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function compactMetrics(metrics: Record<string, number | undefined>): Record<string, number> {
  return Object.fromEntries(
    Object.entries(metrics).filter((entry): entry is [string, number] => entry[1] !== undefined),
  );
}

function usageMetrics(usage: AssistantMessageLike["usage"]): Record<string, number> {
  if (!usage) return {};

  const inputTokens = tokenMetric(usage.input);
  const completionTokens = tokenMetric(usage.output);
  const cachedTokens = tokenMetric(usage.cacheRead);
  const cacheCreationTokens = tokenMetric(usage.cacheWrite);
  const cacheCreation1hTokens = tokenMetric(usage.cacheWrite1h);
  const promptTokens =
    inputTokens === undefined
      ? undefined
      : inputTokens + (cachedTokens ?? 0) + (cacheCreationTokens ?? 0);
  const totalTokens =
    promptTokens !== undefined && completionTokens !== undefined
      ? promptTokens + completionTokens
      : tokenMetric(usage.totalTokens);

  const metrics: Record<string, number | undefined> = {
    prompt_tokens: promptTokens,
    completion_tokens: completionTokens,
    tokens: totalTokens,
    prompt_cached_tokens: cachedTokens,
    completion_reasoning_tokens: tokenMetric(usage.reasoning),
    estimated_cost: nonNegativeMetric(usage.cost?.total),
  };

  if (cacheCreation1hTokens === undefined) {
    metrics.prompt_cache_creation_tokens = cacheCreationTokens;
  } else {
    metrics.prompt_cache_creation_1h_tokens = cacheCreation1hTokens;
    if (cacheCreationTokens !== undefined) {
      metrics.prompt_cache_creation_5m_tokens = Math.max(
        0,
        cacheCreationTokens - cacheCreation1hTokens,
      );
    }
  }

  return compactMetrics(metrics);
}

function pendingLlmCall(turn: ActiveTurn): PendingLlmCall | undefined {
  return turn.llmCalls.at(-1);
}

function recordStreamingTiming(turn: ActiveTurn, event: unknown, observedAt: number): void {
  const pending = pendingLlmCall(turn);
  if (!pending || !isPlainObject(event) || typeof event.type !== "string") return;
  if (event.type === "start") return;

  pending.firstTokenAt ??= observedAt;
  if (event.type.startsWith("thinking_")) {
    pending.firstThinkingAt ??= observedAt;
    pending.lastThinkingAt = observedAt;
  }
  if (event.type.startsWith("text_")) pending.firstTextAt ??= observedAt;
}

function secondsBetween(startedAt: number, endedAt: number | undefined): number | undefined {
  if (endedAt === undefined) return undefined;
  return Math.max(0, endedAt - startedAt) / 1000;
}

function streamingTimingMetadata(
  pending: PendingLlmCall,
  endedAt: number,
): Record<string, unknown> {
  const thinkingEndedAt = pending.firstTextAt ?? pending.lastThinkingAt ?? endedAt;
  return {
    "pi_coding_agent.time_to_first_thinking": secondsBetween(
      pending.startedAt,
      pending.firstThinkingAt,
    ),
    "pi_coding_agent.time_to_first_text": secondsBetween(pending.startedAt, pending.firstTextAt),
    "pi_coding_agent.thinking_duration": pending.firstThinkingAt
      ? secondsBetween(pending.firstThinkingAt, thinkingEndedAt)
      : undefined,
  };
}

function thinkingBlockMetadata(message: AssistantMessageLike): Record<string, unknown> {
  const thinkingBlocks = (message.content ?? []).filter(
    (part) => isPlainObject(part) && part.type === "thinking",
  );
  if (thinkingBlocks.length === 0) return {};

  return {
    thinking_block_count: thinkingBlocks.length,
    empty_thinking_block_count: thinkingBlocks.filter(
      (part) =>
        isPlainObject(part) && (typeof part.thinking !== "string" || part.thinking.length === 0),
    ).length,
  };
}

function getPreviousSessionFile(event: unknown): string | undefined {
  if (!isPlainObject(event)) return undefined;
  return typeof event.previousSessionFile === "string" ? event.previousSessionFile : undefined;
}

function getEventReason(event: unknown): string | undefined {
  if (!isPlainObject(event)) return undefined;
  return typeof event.reason === "string" ? event.reason : undefined;
}

function booleanProperty(value: unknown, key: string): boolean | undefined {
  if (!isPlainObject(value)) return undefined;
  return typeof value[key] === "boolean" ? value[key] : undefined;
}

function numberProperty(value: unknown, keys: readonly string[]): number | undefined {
  if (!isPlainObject(value)) return undefined;
  for (const key of keys) {
    const item = value[key];
    if (typeof item === "number" && Number.isFinite(item)) return item;
  }
  return undefined;
}

function compactionMetadata(event: unknown, eventType: string): Record<string, unknown> {
  const metadata: Record<string, unknown> = { event_type: eventType };
  const eventObject = isPlainObject(event) ? event : undefined;
  const preparation = isPlainObject(eventObject?.preparation) ? eventObject.preparation : undefined;
  const compactionEntry = isPlainObject(eventObject?.compactionEntry)
    ? eventObject.compactionEntry
    : undefined;

  const reason = getEventReason(event);
  if (reason) metadata.compaction_reason = reason;

  const willRetry = booleanProperty(event, "willRetry");
  if (willRetry !== undefined) metadata.will_retry = willRetry;

  const tokensBefore =
    numberProperty(compactionEntry, ["tokensBefore", "tokens_before"]) ??
    numberProperty(preparation, ["tokensBefore", "tokens_before"]);
  if (tokensBefore !== undefined) metadata.tokens_before = tokensBefore;

  const estimatedTokensAfter =
    numberProperty(compactionEntry, [
      "estimatedTokensAfter",
      "estimated_tokens_after",
      "tokensAfter",
      "tokens_after",
      "postCompactionTokens",
      "post_compaction_tokens",
    ]) ??
    numberProperty(preparation, [
      "estimatedTokensAfter",
      "estimated_tokens_after",
      "tokensAfter",
      "tokens_after",
      "postCompactionTokens",
      "post_compaction_tokens",
    ]);
  if (estimatedTokensAfter !== undefined) {
    metadata.estimated_tokens_after = estimatedTokensAfter;
  }

  return metadata;
}

function treePreparationInput(event: unknown): unknown {
  const preparation =
    isPlainObject(event) && isPlainObject(event.preparation) ? event.preparation : undefined;
  if (!preparation) return undefined;

  return truncateValue({
    target_id: typeof preparation.targetId === "string" ? preparation.targetId : undefined,
    old_leaf_id: typeof preparation.oldLeafId === "string" ? preparation.oldLeafId : undefined,
    common_ancestor_id:
      typeof preparation.commonAncestorId === "string" ? preparation.commonAncestorId : undefined,
    entries_to_summarize: Array.isArray(preparation.entriesToSummarize)
      ? preparation.entriesToSummarize.length
      : undefined,
    user_wants_summary:
      typeof preparation.userWantsSummary === "boolean" ? preparation.userWantsSummary : undefined,
    custom_instructions:
      typeof preparation.customInstructions === "string"
        ? preparation.customInstructions
        : undefined,
    replace_instructions:
      typeof preparation.replaceInstructions === "boolean"
        ? preparation.replaceInstructions
        : undefined,
    label: typeof preparation.label === "string" ? preparation.label : undefined,
  });
}

function isAssistantMessage(message: unknown): message is AssistantMessageLike {
  return isPlainObject(message) && message.role === "assistant";
}

function findLastAssistant(
  messages: AgentEndEvent["messages"] = [],
): AssistantMessageLike | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (isAssistantMessage(message)) return message;
  }
  return undefined;
}

function getSessionName(ctx: ExtensionContext): string | undefined {
  try {
    const name = ctx.sessionManager.getSessionName?.();
    return typeof name === "string" && name.trim() ? name : undefined;
  } catch {
    return undefined;
  }
}

function standardRootMetadata(ctx: ExtensionContext, config: TraceConfig): Record<string, unknown> {
  const descriptor = getSessionDescriptor(ctx);
  const sessionName = getSessionName(ctx);
  const gitMetadata = gitMetadataForCwd(ctx.cwd);
  return {
    ...config.additionalMetadata,
    source: "pi",
    extension_version: EXTENSION_VERSION,
    pi_version: PI_VERSION,
    pi_mode: ctx.mode,
    session_name: sessionName,
    session_id: descriptor.sessionId,
    session_key: descriptor.sessionKey,
    session_file: descriptor.sessionFile,
    workspace: basename(ctx.cwd),
    directory: ctx.cwd,
    repo: repoSlugForCwd(ctx.cwd),
    ...gitMetadata,
    hostname: hostname(),
    username: getUsername(),
    os: process.platform,
  };
}

function projectTraceUrl(config: TraceConfig, traceId: string | undefined): string | undefined {
  if (!traceId || !config.orgName) return undefined;
  return `${config.appUrl}/app/${encodeURIComponent(config.orgName)}/p/${encodeURIComponent(config.projectName)}/logs?oid=${encodeURIComponent(traceId)}`;
}

function primaryConfigIssue(config: TraceConfig): ConfigIssue | undefined {
  return config.configIssues.find((issue) => issue.severity === "error") ?? config.configIssues[0];
}

function configIssueStatusLabel(issue: ConfigIssue | undefined): string | undefined {
  if (!issue) return undefined;
  return issue.severity === "warning" ? "config warning" : "config error";
}

function setTracingStatus(
  ctx: ExtensionContext,
  config: TraceConfig,
  options: {
    active: boolean;
    initError?: string;
    missingApiKey?: boolean;
    configIssue?: ConfigIssue;
  },
): void {
  if (!ctx.hasUI || !config.showUi) {
    if (ctx.hasUI) ctx.ui.setStatus(TRACING_STATUS_KEY, undefined);
    return;
  }

  const theme = ctx.ui.theme;

  if (options.initError) {
    ctx.ui.setStatus(
      TRACING_STATUS_KEY,
      theme.fg("warning", "Braintrust") + theme.fg("dim", " setup failed"),
    );
    return;
  }

  if (options.active) {
    ctx.ui.setStatus(
      TRACING_STATUS_KEY,
      theme.fg("accent", "Braintrust") +
        theme.fg(
          "dim",
          ` tracing ${config.projectName}${options.configIssue ? " (config warning)" : ""}`,
        ),
    );
    return;
  }

  if (options.missingApiKey) {
    ctx.ui.setStatus(
      TRACING_STATUS_KEY,
      theme.fg("warning", "Braintrust") + theme.fg("dim", " missing API key"),
    );
    return;
  }

  const configIssueLabel = configIssueStatusLabel(options.configIssue);
  if (configIssueLabel) {
    ctx.ui.setStatus(
      TRACING_STATUS_KEY,
      theme.fg("warning", "Braintrust") + theme.fg("dim", ` ${configIssueLabel}`),
    );
    return;
  }

  ctx.ui.setStatus(TRACING_STATUS_KEY, undefined);
}

function makeHyperlink(url: string, text: string): string {
  return `\u001B]8;;${url}\u0007${text}\u001B]8;;\u0007`;
}

function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  const left = Math.max(1, Math.floor((maxLength - 1) / 2));
  const right = Math.max(1, maxLength - left - 1);
  return `${value.slice(0, left)}…${value.slice(-right)}`;
}

function shortenTraceUrl(traceUrl: string): string {
  try {
    const parsed = new URL(traceUrl);
    const host = parsed.host.replace(/^www\./, "");
    const oid = parsed.searchParams.get("oid") ?? parsed.searchParams.get("id");
    const shortOid = oid ? truncateMiddle(oid, 16) : undefined;
    const suffix = shortOid ? `?oid=${shortOid}` : "";
    return truncateMiddle(`${host}${parsed.pathname}${suffix}`, 72);
  } catch {
    return truncateMiddle(traceUrl, 72);
  }
}

function displayPath(path: string): string {
  const home = process.env.HOME;
  if (home && path.startsWith(home)) return `~${path.slice(home.length)}`;
  return path;
}

function setTraceWidget(
  ctx: ExtensionContext,
  config: TraceConfig,
  traceUrl: string | undefined,
  configIssue: ConfigIssue | undefined,
): void {
  if (!ctx.hasUI || !config.showUi) {
    if (ctx.hasUI) ctx.ui.setWidget(TRACING_WIDGET_KEY, undefined);
    return;
  }

  const theme = ctx.ui.theme;
  const lines: string[] = [];

  if (traceUrl && config.showTraceLink) {
    const label = makeHyperlink(
      traceUrl,
      theme.fg("accent", theme.underline("Braintrust trace ↗")),
    );
    lines.push(label, theme.fg("dim", shortenTraceUrl(traceUrl)));
  }

  if (configIssue) {
    const issueLabel =
      traceUrl || configIssue.severity === "warning"
        ? "Braintrust config warning"
        : "Braintrust config error";
    lines.push(
      theme.fg("warning", issueLabel),
      theme.fg(
        "dim",
        truncateMiddle(`${displayPath(configIssue.path)}: ${configIssue.message}`, 120),
      ),
    );
  }

  ctx.ui.setWidget(TRACING_WIDGET_KEY, lines.length > 0 ? lines : undefined, {
    placement: "belowEditor",
  });
}

export default function braintrustPiExtension(pi: ExtensionAPI): void {
  const config = loadConfig(process.cwd());
  const logger = createLogger(config);
  const store = createStateStore(config.stateDir, logger);

  let client: BraintrustClient | undefined;
  let clientInitializationError: string | undefined;

  if (config.enabled && config.apiKey) {
    client = new BraintrustClient(config, logger);
    client.initialize().catch((error: unknown) => {
      clientInitializationError = String(error);
      logger.error("failed to initialize Braintrust client", { error: clientInitializationError });
    });
  } else if (config.enabled && !config.apiKey) {
    logger.warn("TRACE_TO_BRAINTRUST is enabled but BRAINTRUST_API_KEY is missing");
  }

  function tracingEnabled(): boolean {
    return Boolean(config.enabled && client && !clientInitializationError);
  }

  function refreshTracingUi(ctx: ExtensionContext): void {
    const configIssue = primaryConfigIssue(config);
    setTracingStatus(ctx, config, {
      active: tracingEnabled(),
      initError: clientInitializationError,
      missingApiKey: Boolean(config.enabled && !config.apiKey),
      configIssue,
    });
    setTraceWidget(ctx, config, activeSession?.traceUrl, configIssue);
  }

  function persistTraceUrl(session: ActiveSession, traceUrl: string): void {
    session.traceUrl = traceUrl;
    store.patch(session.sessionKey, {
      traceUrl,
      lastSeenAt: Date.now(),
    });
  }

  function refreshTraceUrl(ctx: ExtensionContext, session: ActiveSession): void {
    if (!client || session.traceUrlPromise || !hasSessionRoot(session)) return;

    const quickUrl =
      session.traceUrl ??
      client.getSpanLink(session.rootSpan) ??
      projectTraceUrl(config, session.rootSpanRecordId);

    if (quickUrl && quickUrl !== session.traceUrl) {
      persistTraceUrl(session, quickUrl);
      if (activeSession?.sessionKey === session.sessionKey) refreshTracingUi(ctx);
    }

    if (!session.rootSpan || session.traceUrl) return;

    session.traceUrlPromise = client
      .getSpanPermalink(session.rootSpan)
      .then((traceUrl) => {
        if (!traceUrl || traceUrl === session.traceUrl) return;
        persistTraceUrl(session, traceUrl);
        if (activeSession?.sessionKey === session.sessionKey) refreshTracingUi(ctx);
      })
      .finally(() => {
        session.traceUrlPromise = undefined;
      });
  }

  let activeSession: ActiveSession | undefined;

  function materializeSessionRoot(
    ctx: ExtensionContext,
    session: ActiveSession,
  ): ActiveSession | undefined {
    if (!client) return undefined;
    if (hasSessionRoot(session)) return session;

    const startedAt = Date.now();
    const rootSpanId = generateUuid();
    const traceRootSpanId = config.rootSpanId ?? rootSpanId;
    const parentSpanId = config.parentSpanId;
    const rootSpan = client.startSpan({
      spanId: rootSpanId,
      rootSpanId: traceRootSpanId,
      parentSpanId,
      startedAt,
      name: getSessionName(ctx) ?? rootSpanName(ctx.cwd),
      type: "task",
      metadata: {
        ...standardRootMetadata(ctx, config),
        opened_via: session.openedVia,
        parent_session_file: session.parentSessionFile,
      },
    });

    session.rootSpanId = rootSpanId;
    session.rootSpan = rootSpan;
    session.rootSpanRecordId = rootSpan?.id;
    session.traceRootSpanId = traceRootSpanId;
    session.parentSpanId = parentSpanId;
    session.traceUrl = client.getSpanLink(rootSpan) ?? projectTraceUrl(config, rootSpan?.id);
    session.startedAt = startedAt;

    store.set(session.sessionKey, {
      rootSpanId,
      rootSpanRecordId: rootSpan?.id,
      traceRootSpanId,
      parentSpanId,
      traceUrl: session.traceUrl,
      startedAt,
      totalTurns: session.totalTurns,
      totalToolCalls: session.totalToolCalls,
      lastSeenAt: startedAt,
      sessionFile: session.sessionFile,
    });
    store.schedulePersist(0);

    refreshTracingUi(ctx);
    refreshTraceUrl(ctx, session);
    return session;
  }

  async function ensureSession(
    ctx: ExtensionContext,
    options: {
      reason?: string;
      parentSessionFile?: string | undefined;
      createIfMissingRoot?: boolean;
    } = {},
  ): Promise<ActiveSession | undefined> {
    if (!tracingEnabled() || !client) return undefined;

    const descriptor = getSessionDescriptor(ctx);
    if (activeSession?.sessionKey === descriptor.sessionKey) {
      activeSession.sessionFile = descriptor.sessionFile;
      activeSession.sessionId = descriptor.sessionId;
      if (!hasSessionRoot(activeSession)) {
        activeSession.openedVia ??= options.reason;
        activeSession.parentSessionFile ??= options.parentSessionFile;
      }

      if (options.createIfMissingRoot === false) {
        refreshTracingUi(ctx);
        return activeSession;
      }

      return materializeSessionRoot(ctx, activeSession);
    }

    const persisted = store.get(descriptor.sessionKey);
    if (persisted) {
      activeSession = {
        sessionKey: descriptor.sessionKey,
        sessionFile: descriptor.sessionFile,
        sessionId: descriptor.sessionId,
        openedVia: options.reason,
        parentSessionFile: options.parentSessionFile,
        rootSpanId: persisted.rootSpanId,
        rootSpan: undefined,
        rootSpanRecordId: persisted.rootSpanRecordId,
        traceRootSpanId: persisted.traceRootSpanId ?? persisted.rootSpanId,
        parentSpanId: persisted.parentSpanId,
        traceUrl: persisted.traceUrl ?? projectTraceUrl(config, persisted.rootSpanRecordId),
        startedAt: persisted.startedAt,
        totalTurns: persisted.totalTurns ?? 0,
        totalToolCalls: persisted.totalToolCalls ?? 0,
        currentTurn: undefined,
      };
      store.patch(descriptor.sessionKey, {
        traceUrl: activeSession.traceUrl,
        lastSeenAt: Date.now(),
        sessionFile: descriptor.sessionFile,
      });
      refreshTracingUi(ctx);
      return activeSession;
    }

    activeSession = {
      sessionKey: descriptor.sessionKey,
      sessionFile: descriptor.sessionFile,
      sessionId: descriptor.sessionId,
      openedVia: options.reason,
      parentSessionFile: options.parentSessionFile,
      totalTurns: 0,
      totalToolCalls: 0,
      currentTurn: undefined,
    };

    if (options.createIfMissingRoot === false) {
      refreshTracingUi(ctx);
      return activeSession;
    }

    return materializeSessionRoot(ctx, activeSession);
  }

  async function finishTurn(
    reason: string,
    endedAt = Date.now(),
    finalAssistantMessage?: AssistantMessageLike,
  ): Promise<void> {
    if (!activeSession?.currentTurn || !client) return;

    const turn = activeSession.currentTurn;
    const finalAssistant = finalAssistantMessage ?? turn.lastAssistantMessage;
    const finalOutput = finalAssistant
      ? normalizeAssistantMessage(finalAssistant)
      : turn.lastOutput;
    const error =
      turn.error ||
      (finalAssistant?.stopReason === "error" || finalAssistant?.stopReason === "aborted"
        ? extractErrorText(finalAssistant, finalAssistant.errorMessage)
        : undefined);

    client.logSpan(turn.span, {
      output: finalOutput,
      error,
      metadata: {
        llm_calls: turn.llmCallCount,
        tool_calls: turn.toolCallCount,
        finish_reason: reason,
      },
    });
    client.endSpan(turn.span, endedAt);

    activeSession.currentTurn = undefined;
    store.patch(activeSession.sessionKey, {
      totalTurns: activeSession.totalTurns,
      totalToolCalls: activeSession.totalToolCalls,
      lastSeenAt: endedAt,
    });
    await store.flush();
  }

  async function finalizeSession(reason: string, endedAt = Date.now()): Promise<void> {
    if (!activeSession || !client) return;

    await finishTurn(reason, endedAt);
    if (!hasSessionRoot(activeSession)) return;

    const summaryMetadata = {
      total_turns: activeSession.totalTurns,
      total_tool_calls: activeSession.totalToolCalls,
      last_close_reason: reason,
    };

    if (activeSession.rootSpan) {
      client.logSpan(activeSession.rootSpan, {
        metadata: summaryMetadata,
      });
      client.endSpan(activeSession.rootSpan, endedAt);
    } else if (activeSession.rootSpanRecordId) {
      client.updateSpan({
        id: activeSession.rootSpanRecordId,
        spanId: activeSession.rootSpanId,
        rootSpanId: activeSession.traceRootSpanId,
        metadata: summaryMetadata,
        metrics: {
          end: toUnixSeconds(endedAt),
        },
      });
    }

    store.patch(activeSession.sessionKey, {
      totalTurns: activeSession.totalTurns,
      totalToolCalls: activeSession.totalToolCalls,
      lastSeenAt: endedAt,
    });
    await store.flush();
  }

  async function rolloverSession(
    ctx: ExtensionContext,
    reason: string,
    previousSessionFile: string | undefined,
  ): Promise<void> {
    if (!tracingEnabled()) return;

    const previousKey = activeSession?.sessionKey;
    const nextKey = getSessionDescriptor(ctx).sessionKey;
    if (previousKey && previousKey !== nextKey) {
      await finalizeSession(reason);
      activeSession = undefined;
    }

    await ensureSession(ctx, {
      reason,
      parentSessionFile: previousSessionFile,
      createIfMissingRoot: false,
    });
  }

  pi.on("session_start", async (event, ctx) => {
    refreshTracingUi(ctx);

    const reason = getEventReason(event);
    if (reason === "new" || reason === "resume" || reason === "fork") {
      await rolloverSession(
        ctx,
        reason === "fork" ? "session_fork" : "session_switch",
        getPreviousSessionFile(event),
      );
      return;
    }

    await ensureSession(ctx, {
      reason: "session_start",
      createIfMissingRoot: false,
    });
  });

  pi.on("input", (event) => {
    if (!isPlainObject(event)) return;
    pendingInputEvent = {
      text: typeof event.text === "string" ? event.text : undefined,
      source: typeof event.source === "string" ? event.source : undefined,
      streamingBehavior:
        typeof event.streamingBehavior === "string" ? event.streamingBehavior : undefined,
      imageCount: Array.isArray(event.images) ? event.images.length : undefined,
    };
  });

  pi.on("before_agent_start", async (event, ctx) => {
    refreshTracingUi(ctx);
    const session = await ensureSession(ctx, { reason: "agent_start" });
    if (!session || !client || !hasSessionRoot(session)) return;

    if (session.currentTurn) {
      await finishTurn("replaced_by_new_prompt", Date.now());
    }

    const startedAt = Date.now();
    session.totalTurns += 1;
    const turnSpanId = generateUuid();
    const turnInput = buildTurnInput(
      event.prompt,
      event.images as readonly ImageLike[] | undefined,
    );
    const explicitSkillNames = explicitSkillNamesFromPiInput(pendingInputEvent);
    const explicitSkillMetadata = explicitSkillRequestMetadata(explicitSkillNames);
    const turnSpan = client.startSpan({
      spanId: turnSpanId,
      rootSpanId: session.traceRootSpanId,
      parentSpanId: session.rootSpanId,
      startedAt,
      input: turnInput,
      metadata: {
        turn_number: session.totalTurns,
        active_model: safeModelName(ctx.model),
        thinking_level: session.thinkingLevel,
        ...metadataForPendingInput(pendingInputEvent),
        ...explicitSkillMetadata,
      },
      name: `Turn ${session.totalTurns}`,
      type: "task",
    });

    pendingInputEvent = undefined;

    session.currentTurn = {
      spanId: turnSpanId,
      span: turnSpan,
      prompt: turnInput,
      explicitSkillNames,
      llmCalls: [],
      llmCallCount: 0,
      toolCallCount: 0,
      toolStarts: new Map(),
      toolParentSpanIds: new Map(),
      activatedToolNames: new Set(),
      lastAssistantMessage: undefined,
      lastOutput: undefined,
      error: undefined,
      thinkingLevel: session.thinkingLevel,
    };

    store.patch(session.sessionKey, {
      totalTurns: session.totalTurns,
      lastSeenAt: startedAt,
    });
  });

  pi.on("context", async (event, ctx) => {
    if (!activeSession?.currentTurn) return;
    const tools = activeToolNames(pi);
    activeSession.currentTurn.llmCalls.push({
      startedAt: Date.now(),
      input: normalizeContextMessages(event.messages as unknown as readonly AgentMessageLike[]),
      activeToolNames: tools,
      activatedToolNames: [...activeSession.currentTurn.activatedToolNames],
      modelMetadata: modelTraceMetadata(ctx.model),
    });
  });

  pi.on("before_provider_request", async (event) => {
    if (!activeSession?.currentTurn) return;
    const metadata = providerRequestMetadata(event);
    if (!metadata) return;
    const pending = pendingLlmCall(activeSession.currentTurn);
    if (pending) pending.providerRequest = metadata;
  });

  pi.on("after_provider_response", async (event) => {
    if (!activeSession?.currentTurn) return;
    const metadata = providerResponseMetadata(event);
    if (!metadata) return;
    const pending = [...activeSession.currentTurn.llmCalls]
      .reverse()
      .find((call) => !call.providerResponse);
    if (pending) pending.providerResponse = metadata;
  });

  pi.on("message_update", async (event) => {
    if (!activeSession?.currentTurn || !isPlainObject(event)) return;
    recordStreamingTiming(activeSession.currentTurn, event.assistantMessageEvent, Date.now());
  });

  pi.on("thinking_level_select", async (event) => {
    if (!isPlainObject(event) || typeof event.level !== "string") return;
    if (activeSession) activeSession.thinkingLevel = event.level;
    if (activeSession?.currentTurn) activeSession.currentTurn.thinkingLevel = event.level;
  });

  pi.on("session_before_compact", async (event, ctx) => {
    const session = await ensureSession(ctx, { reason: "compact" });
    if (!session || !client || !hasSessionRoot(session)) return;

    const startedAt = Date.now();
    const compactionSpanId = generateUuid();
    const input = truncateValue({
      custom_instructions: isPlainObject(event) ? event.customInstructions : undefined,
      branch_entry_count: Array.isArray(event.branchEntries)
        ? event.branchEntries.length
        : undefined,
      preparation: isPlainObject(event) ? event.preparation : undefined,
    });
    const compactionSpan = client.startSpan({
      spanId: compactionSpanId,
      rootSpanId: session.traceRootSpanId,
      parentSpanId: session.rootSpanId,
      startedAt,
      input,
      metadata: compactionMetadata(event, "session_before_compact"),
      name: "Compaction",
      type: "task",
    });

    session.currentCompaction = {
      spanId: compactionSpanId,
      span: compactionSpan,
      startedAt,
      input,
    };
  });

  pi.on("session_compact", async (event, ctx) => {
    const session = await ensureSession(ctx, { reason: "compact" });
    if (!session || !client || !hasSessionRoot(session)) return;

    const compaction = session.currentCompaction ?? {
      spanId: generateUuid(),
      span: undefined,
      startedAt: Date.now(),
      input: undefined,
    };
    if (!compaction.span) {
      compaction.span = client.startSpan({
        spanId: compaction.spanId,
        rootSpanId: session.traceRootSpanId,
        parentSpanId: session.rootSpanId,
        startedAt: compaction.startedAt,
        input: compaction.input,
        metadata: {
          event_type: "session_compact",
        },
        name: "Compaction",
        type: "task",
      });
    }

    client.logSpan(compaction.span, {
      output: truncateValue(isPlainObject(event) ? event.compactionEntry : undefined),
      metadata: {
        ...compactionMetadata(event, "session_compact"),
        from_extension: isPlainObject(event) ? event.fromExtension : undefined,
      },
    });
    client.endSpan(compaction.span, Date.now());
    session.currentCompaction = undefined;

    store.patch(session.sessionKey, {
      lastSeenAt: Date.now(),
    });
  });

  pi.on("session_before_tree", async (event, ctx) => {
    const userWantsSummary = isPlainObject(event)
      ? booleanProperty(event.preparation, "userWantsSummary")
      : undefined;
    if (userWantsSummary !== true) return;

    const session = await ensureSession(ctx, { reason: "tree" });
    if (!session || !client || !hasSessionRoot(session)) return;

    const startedAt = Date.now();
    const branchSummarySpanId = generateUuid();
    const input = treePreparationInput(event);
    const branchSummarySpan = client.startSpan({
      spanId: branchSummarySpanId,
      rootSpanId: session.traceRootSpanId,
      parentSpanId: session.rootSpanId,
      startedAt,
      input,
      metadata: {
        event_type: "session_before_tree",
        user_wants_summary: userWantsSummary,
      },
      name: "Branch Summary",
      type: "task",
    });

    session.currentBranchSummary = {
      spanId: branchSummarySpanId,
      span: branchSummarySpan,
      startedAt,
      input,
    };
  });

  pi.on("session_tree", async (event, ctx) => {
    const hasSummaryEntry = isPlainObject(event) && event.summaryEntry !== undefined;
    if (!activeSession?.currentBranchSummary && !hasSummaryEntry) return;

    const session = await ensureSession(ctx, { reason: "tree" });
    if (!session || !client || !hasSessionRoot(session)) return;

    const branchSummary = session.currentBranchSummary ?? {
      spanId: generateUuid(),
      span: undefined,
      startedAt: Date.now(),
      input: undefined,
    };
    if (!branchSummary.span) {
      branchSummary.span = client.startSpan({
        spanId: branchSummary.spanId,
        rootSpanId: session.traceRootSpanId,
        parentSpanId: session.rootSpanId,
        startedAt: branchSummary.startedAt,
        input: branchSummary.input,
        metadata: {
          event_type: "session_tree",
        },
        name: "Branch Summary",
        type: "task",
      });
    }

    client.logSpan(branchSummary.span, {
      output: truncateValue(isPlainObject(event) ? event.summaryEntry : undefined),
      metadata: {
        event_type: "session_tree",
        from_extension: isPlainObject(event) ? event.fromExtension : undefined,
        new_leaf_id:
          isPlainObject(event) && typeof event.newLeafId === "string" ? event.newLeafId : undefined,
        old_leaf_id:
          isPlainObject(event) && typeof event.oldLeafId === "string" ? event.oldLeafId : undefined,
      },
    });
    client.endSpan(branchSummary.span, Date.now());
    session.currentBranchSummary = undefined;

    store.patch(session.sessionKey, {
      lastSeenAt: Date.now(),
    });
  });

  pi.on("message_end", async (event) => {
    const session = activeSession;
    if (
      !session?.currentTurn ||
      !isAssistantMessage(event.message) ||
      !client ||
      !hasSessionRoot(session)
    ) {
      return;
    }
    const message = event.message;

    const pending: PendingLlmCall = session.currentTurn.llmCalls.shift() ?? {
      startedAt: Date.now(),
      input: [{ role: "user", content: session.currentTurn.prompt }],
      activatedToolNames: [...session.currentTurn.activatedToolNames],
      modelMetadata: {},
    };

    const requestedModelName = safeModelName(message) ?? message.model;
    const responseModel = responseModelName(message);
    const modelName = responseModel ?? requestedModelName;
    const endedAt = Date.now();
    const normalizedOutput = normalizeAssistantMessage(message);
    const error =
      message.stopReason === "error" || message.stopReason === "aborted"
        ? extractErrorText(message, message.errorMessage)
        : undefined;

    session.currentTurn.llmCallCount += 1;
    session.currentTurn.lastAssistantMessage = message;
    session.currentTurn.lastOutput = normalizedOutput;
    if (error) session.currentTurn.error = error;

    const llmSpanId = generateUuid();
    const llmSpan = client.startSpan({
      spanId: llmSpanId,
      rootSpanId: session.traceRootSpanId,
      parentSpanId: session.currentTurn.spanId,
      startedAt: pending.startedAt,
      input: pending.input,
      metadata: {
        ...pending.modelMetadata,
        ...pending.providerRequest,
        ...streamingTimingMetadata(pending, endedAt),
        ...thinkingBlockMetadata(message),
        api: message.api,
        provider: message.provider,
        model: modelName,
        requested_model: requestedModelName,
        response_model: responseModel,
        "pi_coding_agent.response_id": message.responseId,
        stop_reason: message.stopReason,
        thinking_level: session.currentTurn.thinkingLevel ?? session.thinkingLevel,
        "pi_coding_agent.active_tools": pending.activeToolNames,
        active_tool_count: pending.activeToolNames?.length,
        activated_tools:
          pending.activatedToolNames.length > 0 ? pending.activatedToolNames : undefined,
        provider_response_status: pending.providerResponse?.status,
        provider_response_headers: pending.providerResponse?.headers,
        cache_read_tokens: message.usage?.cacheRead,
        cache_write_tokens: message.usage?.cacheWrite,
      },
      name: modelName || "llm",
      type: "llm",
    });

    for (const part of message.content ?? []) {
      if (!isPlainObject(part) || part.type !== "toolCall" || typeof part.id !== "string") {
        continue;
      }
      session.currentTurn.toolParentSpanIds.set(part.id, llmSpanId);
    }

    client.logSpan(llmSpan, {
      output: [normalizedOutput],
      error,
      metrics: compactMetrics({
        ...usageMetrics(message.usage),
        time_to_first_token: secondsBetween(pending.startedAt, pending.firstTokenAt),
      }),
    });
    client.endSpan(llmSpan, endedAt);
  });

  pi.on("tool_execution_start", async (event) => {
    if (!activeSession?.currentTurn) return;
    activeSession.currentTurn.toolStarts.set(event.toolCallId, {
      startedAt: Date.now(),
      args: event.args,
      toolName: event.toolName,
    });
  });

  pi.on("tool_execution_end", async (event, ctx) => {
    const session = activeSession;
    if (!session?.currentTurn || !client || !hasSessionRoot(session)) return;

    const tracked = session.currentTurn.toolStarts.get(event.toolCallId) ?? {
      startedAt: Date.now(),
      args: undefined,
      toolName: event.toolName,
    };
    session.currentTurn.toolStarts.delete(event.toolCallId);
    const parentLlmSpanId = session.currentTurn.toolParentSpanIds.get(event.toolCallId);
    session.currentTurn.toolParentSpanIds.delete(event.toolCallId);

    const endedAt = Date.now();
    session.totalToolCalls += 1;
    session.currentTurn.toolCallCount += 1;

    const output = normalizeToolResult(event.result);
    const error = event.isError
      ? extractErrorText(event.result, `${event.toolName} failed`)
      : undefined;

    if (error && !session.currentTurn.error) {
      session.currentTurn.error = error;
    }

    const activatedTools = addedToolNames(event.result);
    for (const toolName of activatedTools) session.currentTurn.activatedToolNames.add(toolName);
    const currentActiveTools = activatedTools.length > 0 ? activeToolNames(pi) : undefined;
    const currentModelMetadata = activatedTools.length > 0 ? modelTraceMetadata(ctx.model) : {};

    const skillLoad = skillLoadFromRead(event.toolName, tracked.args);
    const skillLoadTrigger = skillLoadTriggerForTurn(session.currentTurn, skillLoad);
    const metadataToolName = skillLoad ? "skill" : event.toolName;
    const spanName = skillLoad
      ? `skill: ${skillLoad.skill_name}`
      : formatToolSpanName(event.toolName, tracked.args);

    const toolSpan = client.startSpan({
      spanId: generateUuid(),
      rootSpanId: session.traceRootSpanId,
      parentSpanId: parentLlmSpanId ?? session.currentTurn.spanId,
      startedAt: tracked.startedAt,
      input: tracked.args,
      metadata: {
        tool_name: metadataToolName,
        original_tool_name: skillLoad ? event.toolName : undefined,
        tool_call_id: event.toolCallId,
        is_error: event.isError,
        tool_approval: "approved",
        parent_llm_span_id: parentLlmSpanId,
        activated_tools: activatedTools.length > 0 ? activatedTools : undefined,
        activated_tool_count: activatedTools.length > 0 ? activatedTools.length : undefined,
        "pi_coding_agent.active_tools": currentActiveTools,
        active_tool_count: currentActiveTools?.length,
        deferred_tools_mode: currentModelMetadata.deferred_tools_mode,
        ...skillLoad,
        skill_load_trigger: skillLoadTrigger,
      },
      name: spanName,
      type: "tool",
    });

    client.logSpan(toolSpan, {
      output,
      error,
    });
    client.endSpan(toolSpan, endedAt);

    store.patch(session.sessionKey, {
      totalTurns: session.totalTurns,
      totalToolCalls: session.totalToolCalls,
      lastSeenAt: endedAt,
    });
  });

  pi.on("agent_end", async (event) => {
    if (!activeSession?.currentTurn) return;
    if (isPlainObject(event) && event.willRetry === true) return;
    const finalAssistant = findLastAssistant(event.messages);
    await finishTurn("agent_end", Date.now(), finalAssistant);
  });

  pi.on("session_shutdown", async (event, ctx) => {
    if (ctx.hasUI) {
      ctx.ui.setStatus(TRACING_STATUS_KEY, undefined);
      ctx.ui.setWidget(TRACING_WIDGET_KEY, undefined);
    }

    // pi 0.68.0+ exposes a structured reason ("quit" | "reload" | "new" | "resume"
    // | "fork"). Older pi hosts pass no payload, so we fall back to the generic
    // label to stay backwards-compatible and keep the existing metadata shape.
    const reason = getEventReason(event) ?? "session_shutdown";
    logger.debug("session_shutdown", { reason });

    if (client && !clientInitializationError) {
      // On reload the same pi session is about to resume in a freshly imported
      // extension instance, which restores its state from the persisted store and
      // keeps writing to the existing root span. Finalizing here would close that
      // root span out from under the reloaded instance, so we just flush pending
      // writes and let the new instance continue the trace.
      if (reason !== "reload") {
        await finalizeSession(reason);
      }
      await client.flush();
    }
    activeSession = undefined;
    pendingInputEvent = undefined;
    await store.flush();
    await logger.flush();
  });

  for (const configIssue of config.configIssues) {
    if (configIssue.severity === "error") {
      logger.error("Braintrust config issue", configIssue);
    } else {
      logger.warn("Braintrust config issue", configIssue);
    }
  }

  logger.debug("Braintrust pi tracing extension loaded", {
    enabled: config.enabled,
    project: config.projectName,
    hasApiKey: Boolean(config.apiKey),
    logFile: logger.filePath,
    configIssues: config.configIssues,
    configHash: shortHash(
      JSON.stringify({
        enabled: config.enabled,
        project: config.projectName,
        debug: config.debug,
        stateDir: config.stateDir,
      }),
    ),
  });
}
