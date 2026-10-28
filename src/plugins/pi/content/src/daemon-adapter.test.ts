import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockState = vi.hoisted(() => ({
  logs: [] as Array<Record<string, unknown>>,
  flushes: [] as string[],
  closed: 0,
  claim: true,
  legacyContinuation: undefined as Record<string, unknown> | undefined,
  legacyContinuationFor: vi.fn(),
  logGate: undefined as Promise<void> | undefined,
  statusGate: undefined as Promise<void> | undefined,
  statusCalls: 0,
  permalink: "https://www.braintrust.dev/trace/1" as string | undefined,
}));

vi.mock("./runtime/daemon-client.ts", () => ({
  claimManagedTracingInstance: () => mockState.claim,
  DaemonClient: class {
    async log(envelope: Record<string, unknown>): Promise<boolean> {
      mockState.logs.push(envelope);
      await mockState.logGate;
      return true;
    }
    async flush(sessionId: string): Promise<boolean> {
      mockState.flushes.push(sessionId);
      return true;
    }
    async status(sessionId: string): Promise<Record<string, unknown>> {
      mockState.statusCalls += 1;
      await mockState.statusGate;
      return {
        daemon_version: "test",
        uptime_ms: 1,
        sessions: [
          {
            session_id: sessionId,
            source: "pi",
            queued: 0,
            spans_emitted: 1,
            permalink: mockState.permalink,
          },
        ],
      };
    }
    async close(): Promise<void> {
      mockState.closed += 1;
    }
  },
}));

vi.mock("./config.ts", () => ({
  loadConfig: () => ({
    enabled: true,
    profile: "work",
    orgName: "acme",
    projectName: "agents",
    additionalMetadata: { team: "platform" },
    showUi: true,
    showTraceLink: true,
    route: {
      auth: { profile: "work", org_name: "acme" },
      destination: { type: "project_logs", project_name: "agents" },
      flush_mode: "flush_on_turn_end",
      additional_metadata: { team: "platform" },
    },
  }),
}));

vi.mock("./legacy-session.ts", () => ({
  legacyContinuationFor: (...args: unknown[]) => {
    mockState.legacyContinuationFor(...args);
    return mockState.legacyContinuation;
  },
}));

vi.mock("./pi-package.ts", () => ({
  runningPiPackage: () => ({
    name: "@oh-my-pi/pi-coding-agent",
    version: "18.12.6",
    configDir: ".pi",
  }),
}));

type Handler = (...args: unknown[]) => Promise<unknown>;

/** Load the extension against a fake Pi and return its registered handlers. */
async function loadExtension(): Promise<Map<string, Handler>> {
  const handlers = new Map<string, Handler>();
  const pi = { on: (name: string, handler: Handler) => handlers.set(name, handler) };
  const { default: extension } = await import("./index.ts");
  extension(pi as never);
  return handlers;
}

function fakeContext(ui: Record<string, unknown> = { setStatus: vi.fn(), setWidget: vi.fn() }) {
  return {
    cwd: "/tmp/project",
    model: { provider: "openai", id: "gpt-5" },
    hasUI: true,
    ui,
    sessionManager: {
      getSessionFile: () => "/tmp/session.jsonl",
      getSessionId: () => "native-session",
    },
  };
}

describe("Pi daemon adapter", () => {
  afterEach(() => vi.restoreAllMocks());

  beforeEach(() => {
    mockState.logs.length = 0;
    mockState.flushes.length = 0;
    mockState.closed = 0;
    mockState.claim = true;
    mockState.legacyContinuation = undefined;
    mockState.legacyContinuationFor.mockClear();
    mockState.logGate = undefined;
    mockState.statusGate = undefined;
    mockState.statusCalls = 0;
    mockState.permalink = "https://www.braintrust.dev/trace/1";
  });

  it("does not register a duplicate managed adapter instance", async () => {
    mockState.claim = false;
    const handlers = await loadExtension();
    expect(handlers.size).toBe(0);
  });

  it("forwards the running host name and version instead of an installed peer", async () => {
    const now = vi.spyOn(Date, "now").mockReturnValue(1_234);
    const handlers = await loadExtension();
    const ctx = fakeContext();
    await handlers.get("session_start")?.({ reason: "new" }, ctx);
    now.mockReturnValue(5_678);
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);
    expect(mockState.logs[0]).toMatchObject({
      source: "pi",
      source_version: "18.12.6",
      ts_ms: 1_234,
      payload: { host_name: "@oh-my-pi/pi-coding-agent" },
    });
  });

  it("does not block session startup on daemon status", async () => {
    const ctx = fakeContext();
    const handlers = await loadExtension();

    let releaseStatus!: () => void;
    mockState.statusGate = new Promise<void>((resolve) => {
      releaseStatus = resolve;
    });
    expect(handlers.get("session_start")?.({ reason: "new" }, ctx)).toBeUndefined();
    await vi.waitFor(() => expect(mockState.statusCalls).toBe(1));
    releaseStatus();
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);
  });

  it("does not restore UI from a pending status refresh after shutdown", async () => {
    const statuses: unknown[] = [];
    const widgets: unknown[] = [];
    const ctx = fakeContext({
      setStatus: (...args: unknown[]) => statuses.push(args),
      setWidget: (...args: unknown[]) => widgets.push(args),
    });
    const handlers = await loadExtension();

    let releaseStatus!: () => void;
    mockState.statusGate = new Promise<void>((resolve) => {
      releaseStatus = resolve;
    });
    await handlers.get("session_start")?.({ reason: "new" }, ctx);
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);

    expect(statuses.at(-1)).toEqual(["braintrust-tracing", undefined]);
    expect(widgets.at(-1)).toEqual(["braintrust-trace-link", undefined]);

    releaseStatus();
    await Promise.resolve();
    await Promise.resolve();

    expect(statuses).toEqual([["braintrust-tracing", undefined]]);
    expect(widgets).toEqual([["braintrust-trace-link", undefined]]);
  });

  it("forwards native events and keeps the trace-link UI", async () => {
    const statuses: unknown[] = [];
    const widgets: unknown[] = [];
    const ctx = fakeContext({
      setStatus: (...args: unknown[]) => statuses.push(args),
      setWidget: (...args: unknown[]) => widgets.push(args),
    });
    const handlers = await loadExtension();

    expect([...handlers.keys()]).toEqual([
      "session_start",
      "input",
      "before_agent_start",
      "context",
      "before_provider_request",
      "after_provider_response",
      "message_update",
      "thinking_level_select",
      "message_end",
      "tool_execution_start",
      "tool_execution_end",
      "session_before_compact",
      "session_compact",
      "session_before_tree",
      "session_tree",
      "agent_end",
      "session_shutdown",
    ]);

    await handlers.get("session_start")?.({ reason: "new" }, ctx);
    await handlers.get("before_agent_start")?.({ prompt: "hello" }, ctx);
    await handlers.get("context")?.({ messages: [{ role: "user", content: "hello" }] }, ctx);
    await handlers.get("message_update")?.({
      type: "message_update",
      assistantMessageEvent: { type: "text_start" },
    });
    await handlers.get("message_update")?.({
      type: "message_update",
      assistantMessageEvent: { type: "text_delta", delta: "first" },
      message: { role: "assistant", content: "first" },
    });
    await handlers.get("message_update")?.({
      type: "message_update",
      assistantMessageEvent: { type: "text_delta", delta: "second" },
      message: { role: "assistant", content: "firstsecond" },
    });
    let acknowledge!: () => void;
    mockState.logGate = new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    expect(handlers.get("agent_end")?.({ messages: [] }, ctx)).toBeUndefined();
    acknowledge();
    mockState.logGate = undefined;
    await handlers.get("input")?.({ text: "next turn" });
    await handlers.get("context")?.({ messages: [{ role: "user", content: "next" }] }, ctx);
    await handlers.get("message_update")?.({
      type: "message_update",
      assistantMessageEvent: { type: "thinking_delta", delta: "first thought" },
    });
    await handlers.get("session_compact")?.({}, ctx);
    await handlers.get("session_tree")?.({}, ctx);
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);

    expect(mockState.logs.map((log) => log.event)).toEqual([
      "session_start",
      "before_agent_start",
      "context",
      "message_update",
      "agent_end",
      "input",
      "context",
      "message_update",
      "session_compact",
      "session_tree",
      "session_shutdown",
    ]);
    expect(
      mockState.logs.every((log) => log.source === "pi" && typeof log.ts_ms === "number"),
    ).toBe(true);
    expect(mockState.logs[0]?.route).toEqual({
      auth: { profile: "work", org_name: "acme" },
      destination: { type: "project_logs", project_name: "agents" },
      flush_mode: "flush_on_turn_end",
      additional_metadata: { team: "platform" },
    });
    expect(mockState.logs[0]?.payload).toMatchObject({
      event: { reason: "new" },
      extension_version: expect.any(String),
      session_file: "/tmp/session.jsonl",
      native_session_id: "native-session",
      cwd: "/tmp/project",
      model: { provider: "openai", id: "gpt-5" },
    });
    const streamingUpdates = mockState.logs.filter((log) => log.event === "message_update");
    expect(streamingUpdates).toHaveLength(2);
    expect(
      streamingUpdates.map(
        (log) =>
          (log.payload as { event: { assistantMessageEvent: { type: string } } }).event
            .assistantMessageEvent.type,
      ),
    ).toEqual(["text_delta", "thinking_delta"]);
    expect(mockState.flushes).toHaveLength(0);
    expect(widgets).toContainEqual([
      "braintrust-trace-link",
      ["Braintrust trace", "https://www.braintrust.dev/trace/1"],
      { placement: "belowEditor" },
    ]);
    expect(statuses.length).toBeGreaterThan(0);
    expect(mockState.closed).toBe(2);
  });

  it("polls for the trace link until the daemon delivers the first turn", async () => {
    vi.useFakeTimers();
    try {
      const widgets: unknown[] = [];
      const ctx = fakeContext({
        setStatus: vi.fn(),
        setWidget: (...args: unknown[]) => widgets.push(args),
      });
      const handlers = await loadExtension();

      // Pi starts and finishes the first turn before the daemon uploads it.
      mockState.permalink = undefined;
      await handlers.get("session_start")?.({ reason: "new" }, ctx);
      await handlers.get("agent_end")?.({ messages: [] }, ctx);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(widgets.at(-1)).toEqual([
        "braintrust-trace-link",
        undefined,
        { placement: "belowEditor" },
      ]);

      mockState.permalink = "https://www.braintrust.dev/trace/1";
      await vi.advanceTimersByTimeAsync(2_000);
      expect(widgets.at(-1)).toEqual([
        "braintrust-trace-link",
        ["Braintrust trace", "https://www.braintrust.dev/trace/1"],
        { placement: "belowEditor" },
      ]);

      // Once the link is shown, neither the timer nor later turns query again.
      const calls = mockState.statusCalls;
      await handlers.get("agent_end")?.({ messages: [] }, ctx);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(mockState.statusCalls).toBe(calls);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops polling for the trace link at its deadline and at shutdown", async () => {
    vi.useFakeTimers();
    try {
      const ctx = fakeContext();
      const handlers = await loadExtension();
      mockState.permalink = undefined;
      await handlers.get("session_start")?.({ reason: "new" }, ctx);

      await handlers.get("agent_end")?.({ messages: [] }, ctx);
      await vi.advanceTimersByTimeAsync(120_000);
      const afterDeadline = mockState.statusCalls;
      await vi.advanceTimersByTimeAsync(120_000);
      expect(mockState.statusCalls).toBe(afterDeadline);

      await handlers.get("agent_end")?.({ messages: [] }, ctx);
      await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);
      const atShutdown = mockState.statusCalls;
      await vi.advanceTimersByTimeAsync(120_000);
      expect(mockState.statusCalls).toBe(atShutdown);
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses throttled tool-call deltas to keep long generations alive", async () => {
    let now = 100_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const handlers = await loadExtension();
    const ctx = fakeContext();
    await handlers.get("context")?.({ messages: [] }, ctx);
    const update = () =>
      handlers.get("message_update")?.({
        type: "message_update",
        assistantMessageEvent: { type: "toolcall_delta", delta: "{" },
      });

    await update();
    now += 9_999;
    await update();
    now += 1;
    await update();

    await handlers.get("session_shutdown")?.({ reason: "quit" }, fakeContext());
    expect(mockState.logs.filter((log) => log.event === "message_update")).toHaveLength(2);
  });

  it("forwards legacy continuation state, read once per Pi session", async () => {
    mockState.legacyContinuation = {
      span: "legacy-root",
      trace: "legacy-trace",
      turns: 3,
      tools: 7,
    };
    const ctx = fakeContext();
    const handlers = await loadExtension();

    await handlers.get("session_start")?.({ reason: "resume" }, ctx);
    await handlers.get("context")?.({}, ctx);
    await handlers.get("tool_execution_end")?.({}, ctx);
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);

    expect(mockState.logs[0]?.payload).toMatchObject({
      legacy_resume: mockState.legacyContinuation,
    });
    expect(mockState.legacyContinuationFor).toHaveBeenCalledTimes(1);
    expect(mockState.legacyContinuationFor).toHaveBeenCalledWith("/tmp/session.jsonl");
  });
});
