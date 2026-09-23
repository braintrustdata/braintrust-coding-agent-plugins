import { beforeEach, describe, expect, it, vi } from "vitest";

const mockState = vi.hoisted(() => ({
  logs: [] as Array<Record<string, unknown>>,
  flushes: [] as string[],
  closed: 0,
  claim: true,
  legacyContinuation: undefined as Record<string, unknown> | undefined,
  logGate: undefined as Promise<void> | undefined,
  statusGate: undefined as Promise<void> | undefined,
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
            permalink: "https://www.braintrust.dev/trace/1",
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
  legacyContinuationFor: () => mockState.legacyContinuation,
}));

describe("Pi daemon adapter", () => {
  beforeEach(() => {
    mockState.logs.length = 0;
    mockState.flushes.length = 0;
    mockState.closed = 0;
    mockState.claim = true;
    mockState.legacyContinuation = undefined;
    mockState.logGate = undefined;
    mockState.statusGate = undefined;
  });

  it("does not register a duplicate managed adapter instance", async () => {
    mockState.claim = false;
    const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
    const pi = {
      on: (name: string, handler: (...args: unknown[]) => Promise<unknown>) =>
        handlers.set(name, handler),
    };
    const { default: extension } = await import("./index.ts");
    extension(pi as never);
    expect(handlers.size).toBe(0);
  });

  it("does not block session startup on daemon status", async () => {
    const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
    const pi = {
      on: (name: string, handler: (...args: unknown[]) => Promise<unknown>) =>
        handlers.set(name, handler),
    };
    const ctx = {
      cwd: "/tmp/project",
      hasUI: true,
      ui: { setStatus: vi.fn(), setWidget: vi.fn() },
      sessionManager: {
        getSessionFile: () => "/tmp/session.jsonl",
        getSessionId: () => "native-session",
      },
    };
    const { default: extension } = await import("./index.ts");
    extension(pi as never);

    let releaseStatus!: () => void;
    mockState.statusGate = new Promise<void>((resolve) => {
      releaseStatus = resolve;
    });
    let started = false;
    const startup = handlers
      .get("session_start")?.({ reason: "new" }, ctx)
      .then(() => {
        started = true;
      });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(started).toBe(true);
    releaseStatus();
    await startup;
  });

  it("does not restore UI from a pending status refresh after shutdown", async () => {
    const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
    const statuses: unknown[] = [];
    const widgets: unknown[] = [];
    const pi = {
      on: (name: string, handler: (...args: unknown[]) => Promise<unknown>) =>
        handlers.set(name, handler),
    };
    const ctx = {
      cwd: "/tmp/project",
      hasUI: true,
      ui: {
        setStatus: (...args: unknown[]) => statuses.push(args),
        setWidget: (...args: unknown[]) => widgets.push(args),
      },
      sessionManager: {
        getSessionFile: () => "/tmp/session.jsonl",
        getSessionId: () => "native-session",
      },
    };
    const { default: extension } = await import("./index.ts");
    extension(pi as never);

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
    const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
    const statuses: unknown[] = [];
    const widgets: unknown[] = [];
    const pi = {
      on: (name: string, handler: (...args: unknown[]) => Promise<unknown>) =>
        handlers.set(name, handler),
    };
    const ctx = {
      cwd: "/tmp/project",
      model: { provider: "openai", id: "gpt-5" },
      hasUI: true,
      ui: {
        setStatus: (...args: unknown[]) => statuses.push(args),
        setWidget: (...args: unknown[]) => widgets.push(args),
      },
      sessionManager: {
        getSessionFile: () => "/tmp/session.jsonl",
        getSessionId: () => "native-session",
      },
    };
    const { default: extension } = await import("./index.ts");
    extension(pi as never);

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
    let turnEnded = false;
    const turnEnd = handlers
      .get("agent_end")?.({ messages: [] })
      .then(() => {
        turnEnded = true;
      });
    await Promise.resolve();
    expect(turnEnded).toBe(false);
    acknowledge();
    await turnEnd;
    mockState.logGate = undefined;
    expect(mockState.flushes).toHaveLength(0);
    await handlers.get("input")?.({ text: "next turn" });
    expect(mockState.logs.at(-1)?.event).toBe("input");
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

  it("forwards legacy continuation state with the first daemon event", async () => {
    mockState.legacyContinuation = {
      span: "legacy-root",
      trace: "legacy-trace",
      turns: 3,
      tools: 7,
    };
    const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
    const pi = {
      on: (name: string, handler: (...args: unknown[]) => Promise<unknown>) =>
        handlers.set(name, handler),
    };
    const ctx = {
      cwd: "/tmp/project",
      hasUI: false,
      ui: { setStatus: vi.fn(), setWidget: vi.fn() },
      sessionManager: {
        getSessionFile: () => "/tmp/session.jsonl",
        getSessionId: () => "native-session",
      },
    };
    const { default: extension } = await import("./index.ts");
    extension(pi as never);

    await handlers.get("session_start")?.({ reason: "resume" }, ctx);

    expect(mockState.logs[0]?.payload).toMatchObject({
      legacy_resume: mockState.legacyContinuation,
    });
  });
});
