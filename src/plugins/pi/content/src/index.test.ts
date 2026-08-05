import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";

const packageVersion = (JSON.parse(readFileSync("package.json", "utf8")) as { version: string })
  .version;

const mockState = vi.hoisted(() => ({
  startSpans: [] as Array<Record<string, unknown>>,
  logSpans: [] as Array<Record<string, unknown>>,
  endSpans: [] as Array<Record<string, unknown>>,
  updateSpans: [] as Array<Record<string, unknown>>,
  statuses: [] as Array<{ key: string; text: string | undefined }>,
  widgets: [] as Array<{ key: string; content: string[] | undefined }>,
  initializeCalls: 0,
  flushCalls: 0,
  config: {
    enabled: true,
    apiKey: "test-key",
    apiUrl: undefined,
    appUrl: "https://www.braintrust.dev",
    orgName: undefined,
    projectName: "pi",
    debug: false,
    logFile: undefined,
    stateDir: "/tmp/braintrust-pi-extension-test",
    additionalMetadata: {},
    parentSpanId: undefined,
    rootSpanId: undefined,
    showUi: true,
    showTraceLink: true,
    configIssues: [] as Array<{ path: string; message: string; severity: "error" | "warning" }>,
  },
}));

vi.mock("./client.ts", () => {
  class MockBraintrustClient {
    async initialize(): Promise<void> {
      mockState.initializeCalls += 1;
    }

    startSpan(args: Record<string, unknown>): Record<string, unknown> {
      mockState.startSpans.push(args);
      return {
        id: `record-${String(args.spanId)}`,
        spanId: args.spanId,
        rootSpanId: args.rootSpanId,
      };
    }

    logSpan(span: Record<string, unknown> | undefined, event: Record<string, unknown>): void {
      mockState.logSpans.push({ span, event });
    }

    endSpan(span: Record<string, unknown> | undefined, endedAt?: number): void {
      mockState.endSpans.push({ span, endedAt });
    }

    getSpanLink(span: Record<string, unknown> | undefined): string | undefined {
      if (!span) return undefined;
      return "https://www.braintrust.dev/app/test-org/p/pi/logs?oid=trace-row-1";
    }

    async getSpanPermalink(span: Record<string, unknown> | undefined): Promise<string | undefined> {
      return this.getSpanLink(span);
    }

    updateSpan(args: Record<string, unknown>): void {
      mockState.updateSpans.push(args);
    }

    async flush(): Promise<void> {
      mockState.flushCalls += 1;
    }
  }

  return {
    BraintrustClient: MockBraintrustClient,
  };
});

vi.mock("./config.ts", () => ({
  loadConfig: () => ({ ...mockState.config }),
  createLogger: () => ({
    filePath: "/tmp/braintrust-pi-extension-test.log",
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    flush: async () => {},
  }),
}));

vi.mock("./state.ts", () => ({
  createStateStore: () => ({
    get: () => undefined,
    set: () => undefined,
    patch: () => undefined,
    delete: () => undefined,
    schedulePersist: () => undefined,
    flush: async () => {},
  }),
}));

beforeEach(() => {
  mockState.startSpans.length = 0;
  mockState.logSpans.length = 0;
  mockState.endSpans.length = 0;
  mockState.updateSpans.length = 0;
  mockState.statuses.length = 0;
  mockState.widgets.length = 0;
  mockState.initializeCalls = 0;
  mockState.flushCalls = 0;
  mockState.config = {
    enabled: true,
    apiKey: "test-key",
    apiUrl: undefined,
    appUrl: "https://www.braintrust.dev",
    orgName: undefined,
    projectName: "pi",
    debug: false,
    logFile: undefined,
    stateDir: "/tmp/braintrust-pi-extension-test",
    additionalMetadata: {},
    parentSpanId: undefined,
    rootSpanId: undefined,
    showUi: true,
    showTraceLink: true,
    configIssues: [],
  };
  vi.resetModules();
});

async function createHarness() {
  const handlers = new Map<string, (...args: unknown[]) => unknown>();
  const { default: braintrustPiExtension } = await import("./legacy-processor.ts");

  braintrustPiExtension({
    on(eventName: string, handler: (...args: unknown[]) => unknown) {
      handlers.set(eventName, handler);
    },
    getActiveTools() {
      return ["read", "bash", "tool_search", "Calculator"];
    },
  } as never);

  const ctx = {
    cwd: "/tmp/workspace",
    hasUI: true,
    model: "anthropic/claude-sonnet-4",
    ui: {
      theme: {
        fg: (_color: string, text: string) => text,
        underline: (text: string) => text,
      },
      setStatus: (key: string, text: string | undefined) => {
        mockState.statuses.push({ key, text });
      },
      setWidget: (key: string, content: string[] | undefined, _options?: unknown) => {
        mockState.widgets.push({ key, content });
      },
    },
    sessionManager: {
      getSessionFile: () => "/tmp/session.json",
      getSessionId: () => "session-1",
      getSessionName: () => "Checkout fix",
    },
    mode: "json",
  };

  async function emit(eventName: string, event: Record<string, unknown> = {}): Promise<void> {
    const handler = handlers.get(eventName);
    if (!handler) throw new Error(`No handler registered for ${eventName}`);
    await handler(event, ctx);
  }

  return { emit, handlers, ctx };
}

describe("braintrustPiExtension", () => {
  it("uses session_start reasons instead of legacy session transition events", async () => {
    const { handlers } = await createHarness();

    expect(handlers.has("session_switch")).toBe(false);
    expect(handlers.has("session_fork")).toBe(false);
  });

  it("shows a trace url only after the session produces a turn", async () => {
    const { emit } = await createHarness();

    await emit("session_start");

    expect(mockState.statuses[0]).toEqual({
      key: "braintrust-tracing",
      text: "Braintrust tracing pi",
    });
    expect(mockState.startSpans).toEqual([]);
    expect(mockState.widgets.at(-1)).toEqual({
      key: "braintrust-trace-link",
      content: undefined,
    });

    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    expect(mockState.widgets.at(-1)?.key).toBe("braintrust-trace-link");
    expect(mockState.widgets.at(-1)?.content?.[0]).toContain("Braintrust trace ↗");
    expect(mockState.widgets.at(-1)?.content?.[1]).toBe(
      "braintrust.dev/app/test-org/p/pi/logs?oid=trace-row-1",
    );
    expect(mockState.startSpans[0]?.metadata).toMatchObject({ extension_version: packageVersion });

    await emit("session_shutdown");

    expect(mockState.statuses.at(-1)).toEqual({
      key: "braintrust-tracing",
      text: undefined,
    });
    expect(mockState.widgets.at(-1)).toEqual({
      key: "braintrust-trace-link",
      content: undefined,
    });
  });

  it("surfaces malformed Braintrust config in the UI", async () => {
    mockState.config.configIssues = [
      {
        path: "/Users/test/.pi/agent/braintrust.json",
        message: "Expected double-quoted property name in JSON at position 42",
        severity: "error",
      },
    ];

    const { emit } = await createHarness();

    await emit("session_start");

    expect(mockState.statuses[0]).toEqual({
      key: "braintrust-tracing",
      text: "Braintrust tracing pi (config warning)",
    });
    expect(mockState.widgets.at(-1)?.key).toBe("braintrust-trace-link");
    expect(mockState.widgets.at(-1)?.content).toContain("Braintrust config error");
    expect(mockState.widgets.at(-1)?.content?.[1]).toContain(".pi/agent/braintrust.json");
  });

  it("does not create a root span for an idle session", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("session_shutdown");

    expect(mockState.startSpans).toEqual([]);
    expect(mockState.endSpans).toEqual([]);
    expect(mockState.updateSpans).toEqual([]);
  });

  it("annotates root spans with pi version, mode, and session name metadata", async () => {
    const { emit } = await createHarness();

    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    expect(mockState.startSpans[0]).toMatchObject({
      name: "Checkout fix",
      type: "task",
      metadata: {
        source: "pi",
        extension_version: packageVersion,
        pi_version: expect.any(String),
        pi_mode: "json",
        session_name: "Checkout fix",
      },
    });
  });

  it("annotates turn spans with idle input event metadata", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("input", {
      text: "/skill:review inspect this",
      images: [{ type: "image", source: "screen.png" }],
      source: "interactive",
    });
    await emit("before_agent_start", {
      prompt: "Expanded skill content",
      images: [],
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );

    expect(turnSpan?.metadata).toMatchObject({
      input_source: "interactive",
      input_streaming_behavior: "idle",
      input_image_count: 1,
      raw_input: "/skill:review inspect this",
    });
  });

  it("annotates turn spans with follow-up input event metadata", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("input", {
      text: "queue this next",
      images: [],
      source: "rpc",
      streamingBehavior: "followUp",
    });
    await emit("before_agent_start", {
      prompt: "queue this next",
      images: [],
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );

    expect(turnSpan?.metadata).toMatchObject({
      input_source: "rpc",
      input_streaming_behavior: "followUp",
      input_image_count: 0,
      raw_input: "queue this next",
    });
  });

  it("records resolved model, thinking level, and provider response metadata on llm spans", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("thinking_level_select", { level: "high", previousLevel: "off" });
    await emit("before_agent_start", {
      prompt: "Use a routed model",
      images: [],
    });
    await emit("context", { messages: [{ role: "user", content: "Use a routed model" }] });
    await emit("after_provider_response", {
      status: 200,
      headers: {
        "x-ratelimit-remaining-requests": "42",
        "retry-after": "5",
        "x-request-id": "request-123",
        authorization: "secret",
      },
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "openrouter",
        model: "auto",
        responseModel: "anthropic/claude-sonnet-4-5",
        timestamp: 1_700_000_000_000,
        content: [{ type: "text", text: "Done." }],
      },
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );
    const llmSpan = mockState.startSpans.find((span) => span.type === "llm");

    expect(turnSpan?.metadata).toMatchObject({ thinking_level: "high" });
    expect(llmSpan).toMatchObject({ name: "anthropic/claude-sonnet-4-5" });
    expect(llmSpan?.metadata).toMatchObject({
      model: "anthropic/claude-sonnet-4-5",
      requested_model: "auto",
      response_model: "anthropic/claude-sonnet-4-5",
      thinking_level: "high",
      provider_response_status: 200,
      provider_response_headers: {
        "x-ratelimit-remaining-requests": "42",
        "retry-after": "5",
        "x-request-id": "request-123",
      },
    });
    const llmMetadata = llmSpan?.metadata as
      | { provider_response_headers?: Record<string, unknown> }
      | undefined;
    expect(llmMetadata?.provider_response_headers?.authorization).toBeUndefined();
  });

  it("records canonical usage, cost, effective thinking, and streaming metrics", async () => {
    vi.useFakeTimers();
    try {
      const { emit, ctx } = await createHarness();
      (ctx as { model: unknown }).model = {
        provider: "kimi-coding",
        id: "kimi-k3",
        name: "Kimi K3",
        api: "anthropic-messages",
        reasoning: true,
        contextWindow: 262_144,
        maxTokens: 65_536,
        thinkingLevelMap: { max: "max", high: null },
        compat: { deferredToolsMode: "kimi" },
      };

      vi.setSystemTime(1_000);
      await emit("before_agent_start", { prompt: "Think carefully", images: [] });
      vi.setSystemTime(2_000);
      await emit("context", { messages: [{ role: "user", content: "Think carefully" }] });
      await emit("before_provider_request", {
        payload: {
          model: "kimi-k3",
          max_tokens: 65_536,
          thinking: { type: "adaptive" },
          output_config: { effort: "max" },
          tools: [{ name: "bash", defer_loading: true }],
          messages: [{ role: "assistant", signature: "provider-secret" }],
        },
      });
      vi.setSystemTime(2_250);
      await emit("message_update", { assistantMessageEvent: { type: "thinking_delta" } });
      vi.setSystemTime(2_750);
      await emit("message_update", { assistantMessageEvent: { type: "text_delta" } });
      vi.setSystemTime(3_000);
      await emit("message_end", {
        message: {
          role: "assistant",
          api: "anthropic-messages",
          provider: "kimi-coding",
          model: "kimi-k3",
          responseId: "response-1",
          content: [
            { type: "thinking", thinking: "A short plan", thinkingSignature: "opaque" },
            { type: "text", text: "Done." },
          ],
          usage: {
            input: 100,
            output: 30,
            cacheRead: 20,
            cacheWrite: 15,
            cacheWrite1h: 10,
            reasoning: 12,
            totalTokens: 165,
            cost: { total: 0.0042 },
          },
          stopReason: "stop",
        },
      });

      const llmSpan = mockState.startSpans.find((span) => span.type === "llm");
      expect(llmSpan?.metadata).toMatchObject({
        provider: "kimi-coding",
        model: "kimi-k3",
        "pi_coding_agent.api": "anthropic-messages",
        "pi_coding_agent.model_name": "Kimi K3",
        "pi_coding_agent.response_id": "response-1",
        model_supports_reasoning: true,
        model_context_window: 262_144,
        model_max_tokens: 65_536,
        supported_thinking_levels: ["max"],
        deferred_tools_mode: "kimi",
        provider_request_model: "kimi-k3",
        provider_request_max_tokens: 65_536,
        effective_thinking_type: "adaptive",
        effective_thinking_effort: "max",
        effective_thinking_uses_token_budget: false,
        effective_thinking_enabled: true,
        provider_request_tool_count: 1,
        provider_request_deferred_tool_count: 1,
        "pi_coding_agent.time_to_first_thinking": 0.25,
        "pi_coding_agent.time_to_first_text": 0.75,
        "pi_coding_agent.thinking_duration": 0.5,
        thinking_block_count: 1,
        empty_thinking_block_count: 0,
        active_tool_count: 4,
      });
      expect(JSON.stringify(llmSpan?.metadata)).not.toContain("provider-secret");
      expect(JSON.stringify(llmSpan?.metadata)).not.toContain("opaque");

      const llmLog = mockState.logSpans.find(
        (entry) => (entry.span as { spanId?: unknown } | undefined)?.spanId === llmSpan?.spanId,
      );
      const llmEvent = llmLog?.event as Record<string, unknown> | undefined;
      const llmMetrics = llmEvent?.metrics as Record<string, unknown> | undefined;
      expect(JSON.stringify(llmEvent)).not.toContain("opaque");
      expect(llmMetrics).toEqual({
        prompt_tokens: 135,
        completion_tokens: 30,
        tokens: 165,
        prompt_cached_tokens: 20,
        completion_reasoning_tokens: 12,
        estimated_cost: 0.0042,
        prompt_cache_creation_1h_tokens: 10,
        prompt_cache_creation_5m_tokens: 5,
        time_to_first_token: 0.25,
      });
      expect(llmMetrics?.prompt_cache_creation_tokens).toBe(undefined);
    } finally {
      vi.useRealTimers();
    }
  });

  it("records tools activated through deferred tool loading", async () => {
    const { emit, ctx } = await createHarness();
    (ctx as { model: unknown }).model = {
      provider: "kimi-coding",
      id: "kimi-k3",
      compat: { deferredToolsMode: "kimi" },
    };

    await emit("before_agent_start", { prompt: "Use a calculator", images: [] });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "kimi-coding",
        model: "kimi-k3",
        content: [
          {
            type: "toolCall",
            id: "tool-search-1",
            name: "tool_search",
            arguments: { query: "calculator" },
          },
        ],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-search-1",
      toolName: "tool_search",
      args: { query: "calculator" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-search-1",
      toolName: "tool_search",
      isError: false,
      result: {
        content: [{ type: "text", text: "Loaded Calculator" }],
        addedToolNames: ["Calculator", "Calculator"],
      },
    });

    const loaderSpan = mockState.startSpans.find((span) => span.name === "tool_search");
    expect(loaderSpan?.metadata).toMatchObject({
      activated_tools: ["Calculator"],
      activated_tool_count: 1,
      active_tool_count: 4,
      deferred_tools_mode: "kimi",
      "pi_coding_agent.active_tools": ["read", "bash", "tool_search", "Calculator"],
    });

    await emit("context", {
      messages: [{ role: "toolResult", toolName: "tool_search", content: "Loaded" }],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "kimi-coding",
        model: "kimi-k3",
        content: [{ type: "text", text: "The tool is ready." }],
      },
    });

    const llmSpans = mockState.startSpans.filter((span) => span.type === "llm");
    expect(llmSpans.at(-1)?.metadata).toMatchObject({
      activated_tools: ["Calculator"],
      active_tool_count: 4,
    });
  });

  it("traces session compaction as a task span", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("session_before_compact", {
      customInstructions: "Keep debugging context",
      branchEntries: [{ id: "entry-1" }, { id: "entry-2" }],
      reason: "overflow",
      willRetry: true,
      preparation: {
        messages: [{ role: "user", content: "long context" }],
        tokensBefore: 120_000,
      },
    });
    await emit("session_compact", {
      fromExtension: true,
      reason: "overflow",
      willRetry: true,
      compactionEntry: {
        id: "compact-1",
        summary: "Short summary",
        tokensBefore: 120_000,
        estimatedTokensAfter: 24_000,
      },
    });

    const rootSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.parentSpanId === undefined,
    );
    const compactionSpan = mockState.startSpans.find((span) => span.name === "Compaction");

    expect(rootSpan).toBeDefined();
    expect(compactionSpan).toMatchObject({
      type: "task",
      parentSpanId: rootSpan?.spanId,
      input: {
        custom_instructions: "Keep debugging context",
        branch_entry_count: 2,
        preparation: {
          messages: [{ role: "user", content: "long context" }],
        },
      },
      metadata: {
        event_type: "session_before_compact",
        compaction_reason: "overflow",
        will_retry: true,
        tokens_before: 120_000,
      },
    });
    const compactionLog = mockState.logSpans.find(
      (entry) =>
        (entry.span as { spanId?: unknown } | undefined)?.spanId === compactionSpan?.spanId,
    );
    expect(compactionLog?.event).toEqual({
      output: {
        id: "compact-1",
        summary: "Short summary",
        tokensBefore: 120_000,
        estimatedTokensAfter: 24_000,
      },
      metadata: {
        event_type: "session_compact",
        from_extension: true,
        compaction_reason: "overflow",
        will_retry: true,
        tokens_before: 120_000,
        estimated_tokens_after: 24_000,
      },
    });
    expect(
      mockState.endSpans.some(
        (entry) =>
          (entry.span as { spanId?: unknown } | undefined)?.spanId === compactionSpan?.spanId,
      ),
    ).toBe(true);
  });

  it("traces branch summary events as a task span", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("session_before_tree", {
      preparation: {
        targetId: "target-entry",
        oldLeafId: "old-leaf",
        commonAncestorId: "ancestor",
        entriesToSummarize: [{ id: "entry-1" }, { id: "entry-2" }],
        userWantsSummary: true,
        customInstructions: "Focus on the debugging branch",
        replaceInstructions: false,
        label: "debug-branch",
      },
    });
    await emit("session_tree", {
      newLeafId: "target-entry",
      oldLeafId: "old-leaf",
      fromExtension: false,
      summaryEntry: {
        id: "summary-1",
        summary: "Debugging branch summary",
        fromId: "old-leaf",
        details: { modifiedFiles: ["src/index.ts"] },
      },
    });

    const rootSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.parentSpanId === undefined,
    );
    const branchSummarySpan = mockState.startSpans.find((span) => span.name === "Branch Summary");

    expect(branchSummarySpan).toMatchObject({
      type: "task",
      parentSpanId: rootSpan?.spanId,
      input: {
        target_id: "target-entry",
        old_leaf_id: "old-leaf",
        common_ancestor_id: "ancestor",
        entries_to_summarize: 2,
        user_wants_summary: true,
        custom_instructions: "Focus on the debugging branch",
        replace_instructions: false,
        label: "debug-branch",
      },
      metadata: {
        event_type: "session_before_tree",
        user_wants_summary: true,
      },
    });

    const branchSummaryLog = mockState.logSpans.find(
      (entry) =>
        (entry.span as { spanId?: unknown } | undefined)?.spanId === branchSummarySpan?.spanId,
    );
    expect(branchSummaryLog?.event).toEqual({
      output: {
        id: "summary-1",
        summary: "Debugging branch summary",
        fromId: "old-leaf",
        details: { modifiedFiles: ["src/index.ts"] },
      },
      metadata: {
        event_type: "session_tree",
        from_extension: false,
        new_leaf_id: "target-entry",
        old_leaf_id: "old-leaf",
      },
    });
    expect(
      mockState.endSpans.some(
        (entry) =>
          (entry.span as { spanId?: unknown } | undefined)?.spanId === branchSummarySpan?.spanId,
      ),
    ).toBe(true);
  });

  it("parents tool spans under the llm span that emitted the matching tool call", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_000,
        content: [
          { type: "text", text: "I'll inspect package.json." },
          {
            type: "toolCall",
            id: "tool-1",
            name: "read",
            arguments: { path: "package.json" },
          },
        ],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-1",
      toolName: "read",
      args: { path: "package.json" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-1",
      toolName: "read",
      isError: false,
      result: {
        content: [{ type: "text", text: '{"name":"@braintrust/pi-extension"}' }],
      },
    });

    const llmSpan = mockState.startSpans.find((span) => span.type === "llm");
    const toolSpan = mockState.startSpans.find((span) => span.type === "tool");

    expect(llmSpan).toBeDefined();
    expect(toolSpan).toBeDefined();
    expect(toolSpan?.parentSpanId).toBe(llmSpan?.spanId);
    expect(toolSpan?.metadata).toMatchObject({
      tool_name: "read",
      tool_call_id: "tool-1",
      tool_approval: "approved",
      parent_llm_span_id: llmSpan?.spanId,
    });
  });

  it("marks failed tool spans with approval metadata and an error", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Run a command",
      images: [],
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-error",
      toolName: "bash",
      args: { command: "exit 1" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-error",
      toolName: "bash",
      isError: true,
      result: {
        content: [{ type: "text", text: "Exit code 1" }],
      },
    });

    const toolSpan = mockState.startSpans.find((span) => span.type === "tool");
    const toolLog = mockState.logSpans.find(
      (entry) => (entry.span as { spanId?: unknown } | undefined)?.spanId === toolSpan?.spanId,
    );

    expect(toolSpan?.metadata).toMatchObject({
      tool_name: "bash",
      tool_call_id: "tool-error",
      is_error: true,
      tool_approval: "approved",
    });
    expect((toolLog?.event as { error?: unknown } | undefined)?.error).toBe("Exit code 1");
  });

  it("normalizes SKILL.md reads as skill tool spans", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Use the review skill",
      images: [],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_000,
        content: [
          {
            type: "toolCall",
            id: "tool-skill",
            name: "read",
            arguments: { path: "/home/user/.agents/skills/review/SKILL.md" },
          },
        ],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-skill",
      toolName: "read",
      args: { path: "/home/user/.agents/skills/review/SKILL.md" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-skill",
      toolName: "read",
      isError: false,
      result: { content: [{ type: "text", text: "---\nname: review\n---" }] },
    });

    const skillSpan = mockState.startSpans.find((span) => span.name === "skill: review");
    expect(skillSpan).toMatchObject({
      type: "tool",
      metadata: {
        tool_name: "skill",
        original_tool_name: "read",
        tool_call_id: "tool-skill",
        skill_name: "review",
        skill_path: "/home/user/.agents/skills/review/SKILL.md",
      },
    });
  });

  it("marks matching explicit /skill loads on turn and skill spans", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("input", {
      text: "/skill:review inspect this",
      images: [],
      source: "interactive",
    });
    await emit("before_agent_start", {
      prompt: "Expanded skill content",
      images: [],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_000,
        content: [
          {
            type: "toolCall",
            id: "tool-skill",
            name: "read",
            arguments: { path: "/home/user/.agents/skills/review/SKILL.md" },
          },
        ],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-skill",
      toolName: "read",
      args: { path: "/home/user/.agents/skills/review/SKILL.md" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-skill",
      toolName: "read",
      isError: false,
      result: { content: [{ type: "text", text: "---\nname: review\n---" }] },
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );
    expect(turnSpan?.metadata).toMatchObject({
      raw_input: "/skill:review inspect this",
      loaded_skill_names: ["review"],
      loaded_skills: [{ name: "review" }],
    });

    const skillSpan = mockState.startSpans.find((span) => span.name === "skill: review");
    expect(skillSpan?.metadata).toMatchObject({
      tool_name: "skill",
      skill_name: "review",
      skill_load_trigger: "explicit",
    });
  });

  it("does not mark natural-language skill reads as explicit", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("input", {
      text: "Use the review skill",
      images: [],
      source: "interactive",
    });
    await emit("before_agent_start", {
      prompt: "Use the review skill",
      images: [],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_000,
        content: [
          {
            type: "toolCall",
            id: "tool-skill",
            name: "read",
            arguments: { path: "/home/user/.agents/skills/review/SKILL.md" },
          },
        ],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-skill",
      toolName: "read",
      args: { path: "/home/user/.agents/skills/review/SKILL.md" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-skill",
      toolName: "read",
      isError: false,
      result: { content: [{ type: "text", text: "---\nname: review\n---" }] },
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );
    const turnMetadata = turnSpan?.metadata as Record<string, unknown> | undefined;
    expect(turnMetadata?.loaded_skill_names).toBeUndefined();
    expect(turnMetadata?.loaded_skills).toBeUndefined();

    const skillSpan = mockState.startSpans.find((span) => span.name === "skill: review");
    const skillMetadata = skillSpan?.metadata as Record<string, unknown> | undefined;
    expect(skillMetadata?.skill_load_trigger).toBeUndefined();
  });

  it("preserves fork metadata when the root span is created lazily", async () => {
    const { emit } = await createHarness();

    await emit("session_start", {
      reason: "fork",
      previousSessionFile: "/tmp/parent-session.json",
    });
    await emit("before_agent_start", {
      prompt: "Continue from the fork",
      images: [],
    });

    expect(mockState.startSpans[0]).toMatchObject({
      type: "task",
      metadata: {
        opened_via: "session_fork",
        parent_session_file: "/tmp/parent-session.json",
      },
    });
  });

  it("adds the git repo slug to root span metadata when available", async () => {
    const { default: braintrustPiExtension } = await import("./legacy-processor.ts");
    const handlers = new Map<string, (...args: unknown[]) => unknown>();

    braintrustPiExtension({
      on(eventName: string, handler: (...args: unknown[]) => unknown) {
        handlers.set(eventName, handler);
      },
    } as never);

    const ctx = {
      cwd: process.cwd(),
      hasUI: true,
      model: "anthropic/claude-sonnet-4",
      ui: {
        theme: {
          fg: (_color: string, text: string) => text,
          underline: (text: string) => text,
        },
        setStatus: (key: string, text: string | undefined) => {
          mockState.statuses.push({ key, text });
        },
        setWidget: (key: string, content: string[] | undefined, _options?: unknown) => {
          mockState.widgets.push({ key, content });
        },
      },
      sessionManager: {
        getSessionFile: () => "/tmp/session.json",
        getSessionId: () => "session-1",
        getSessionName: () => undefined,
      },
      mode: "tui",
    };

    const emit = async (eventName: string, event: Record<string, unknown> = {}): Promise<void> => {
      const handler = handlers.get(eventName);
      if (!handler) throw new Error(`No handler registered for ${eventName}`);
      await handler(event, ctx);
    };

    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    expect(mockState.startSpans[0]).toMatchObject({
      type: "task",
      metadata: {
        repo: "braintrustdata/braintrust-coding-agent-plugins",
        git_origin_url: expect.stringMatching(
          /^(git@github\.com:|https:\/\/github\.com\/)braintrustdata\/braintrust-coding-agent-plugins(?:\.git)?$/,
        ),
        git_commit_sha: expect.stringMatching(/^[0-9a-f]{40}$/),
      },
    });
  });

  it("falls back to the turn span when no matching tool call was emitted by the llm", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Run the test suite",
      images: [],
    });
    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_001,
        content: [{ type: "text", text: "Running the test suite." }],
      },
    });
    await emit("tool_execution_start", {
      toolCallId: "tool-missing",
      toolName: "bash",
      args: { command: "npm test" },
    });
    await emit("tool_execution_end", {
      toolCallId: "tool-missing",
      toolName: "bash",
      isError: false,
      result: {
        content: [{ type: "text", text: "tests passed" }],
      },
    });

    const turnSpan = mockState.startSpans.find(
      (span) => span.type === "task" && span.name === "Turn 1",
    );
    const toolSpan = mockState.startSpans.find((span) => span.type === "tool");

    expect(turnSpan).toBeDefined();
    expect(toolSpan).toBeDefined();
    expect(toolSpan?.parentSpanId).toBe(turnSpan?.spanId);
    expect(toolSpan?.metadata).toMatchObject({
      tool_name: "bash",
      tool_call_id: "tool-missing",
    });
    expect((toolSpan?.metadata as Record<string, unknown> | undefined)?.parent_llm_span_id).toBe(
      undefined,
    );
  });

  it("keeps the turn open when agent_end will retry", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Retry after a transient failure",
      images: [],
    });
    await emit("agent_end", { willRetry: true, messages: [] });

    expect(mockState.endSpans).toEqual([]);

    await emit("message_end", {
      message: {
        role: "assistant",
        provider: "anthropic",
        model: "claude-sonnet-4",
        timestamp: 1_700_000_000_000,
        content: [{ type: "text", text: "Recovered after retry." }],
      },
    });
    await emit("agent_end", {
      messages: [
        {
          role: "assistant",
          content: [{ type: "text", text: "Recovered after retry." }],
        },
      ],
    });

    expect(
      mockState.logSpans.some(
        (entry) =>
          ((entry.event as Record<string, unknown>).metadata as Record<string, unknown> | undefined)
            ?.finish_reason === "agent_end",
      ),
    ).toBe(true);
  });

  it("records the structured shutdown reason on the finalized root span", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });
    await emit("session_shutdown", { reason: "quit" });

    const rootFinalizeLog = mockState.logSpans
      .map((entry) => entry.event as Record<string, unknown>)
      .find(
        (event) =>
          (event.metadata as Record<string, unknown> | undefined)?.last_close_reason === "quit",
      );
    expect(rootFinalizeLog).toBeDefined();
    expect(mockState.endSpans.length).toBeGreaterThan(0);
    expect(mockState.flushCalls).toBeGreaterThan(0);
  });

  it("does not finalize the root span on reload shutdowns", async () => {
    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    const startsBefore = mockState.startSpans.length;
    const endsBefore = mockState.endSpans.length;
    const flushesBefore = mockState.flushCalls;

    await emit("session_shutdown", { reason: "reload" });

    // No additional span endings during reload, but pending writes are still flushed.
    expect(mockState.startSpans.length).toBe(startsBefore);
    expect(mockState.endSpans.length).toBe(endsBefore);
    expect(mockState.flushCalls).toBeGreaterThan(flushesBefore);
    const reloadClose = mockState.logSpans
      .map((entry) => entry.event as Record<string, unknown>)
      .some(
        (event) =>
          (event.metadata as Record<string, unknown> | undefined)?.last_close_reason === "reload",
      );
    expect(reloadClose).toBe(false);
  });

  it("hides all UI when showUi is false", async () => {
    mockState.config.showUi = false;

    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    const statusUpdates = mockState.statuses.filter(
      (s) => s.key === "braintrust-tracing" && s.text !== undefined,
    );
    const widgetUpdates = mockState.widgets.filter(
      (w) => w.key === "braintrust-trace-link" && w.content !== undefined,
    );

    expect(statusUpdates).toEqual([]);
    expect(widgetUpdates).toEqual([]);
  });

  it("hides just the trace link when showTraceLink is false", async () => {
    mockState.config.showTraceLink = false;

    const { emit } = await createHarness();

    await emit("session_start");
    await emit("before_agent_start", {
      prompt: "Inspect the package",
      images: [],
    });

    const statusUpdates = mockState.statuses.filter(
      (s) => s.key === "braintrust-tracing" && s.text !== undefined,
    );
    expect(statusUpdates.length).toBeGreaterThan(0);
    expect(statusUpdates[0]?.text).toContain("Braintrust");

    const widgetUpdates = mockState.widgets.filter(
      (w) => w.key === "braintrust-trace-link" && w.content !== undefined,
    );
    expect(widgetUpdates).toEqual([]);
  });
});
