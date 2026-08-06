import { beforeEach, describe, expect, it, vi } from "vitest";

const mockState = vi.hoisted(() => ({
  logs: [] as Array<Record<string, unknown>>,
  flushes: [] as string[],
  closed: 0,
}));

vi.mock("./runtime/daemon-client.ts", () => ({
  DaemonClient: class {
    async log(envelope: Record<string, unknown>): Promise<boolean> {
      mockState.logs.push(envelope);
      return true;
    }
    async flush(sessionId: string): Promise<boolean> {
      mockState.flushes.push(sessionId);
      return true;
    }
    async status(sessionId: string): Promise<Record<string, unknown>> {
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
  }),
}));

describe("Pi daemon adapter", () => {
  beforeEach(() => {
    mockState.logs.length = 0;
    mockState.flushes.length = 0;
    mockState.closed = 0;
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
    await handlers.get("agent_end")?.({ messages: [] });
    await handlers.get("session_shutdown")?.({ reason: "quit" }, ctx);

    expect(mockState.logs.map((log) => log.event)).toEqual([
      "session_start",
      "before_agent_start",
      "agent_end",
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
    expect(mockState.flushes).toHaveLength(2);
    expect(
      widgets.some((args) => JSON.stringify(args).includes("https://www.braintrust.dev/trace/1")),
    ).toBe(true);
    expect(statuses.length).toBeGreaterThan(0);
    expect(mockState.closed).toBe(1);
  });
});
