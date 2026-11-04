import { describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  logs: [] as Array<Record<string, unknown>>,
  closed: false,
  gate: undefined as Promise<void> | undefined,
}));
vi.mock("./runtime/daemon-client", () => ({
  claimManagedTracingInstance: () => true,
  resolveDaemonTraceSettings: (settings: unknown) => settings,
  DaemonClient: class {
    async prepare() {
      return true;
    }
    async log(event: Record<string, unknown>) {
      state.logs.push(event);
      return true;
    }
    async logPipelined(event: Record<string, unknown>) {
      state.logs.push(event);
      await state.gate;
      return true;
    }
    async close() {
      state.closed = true;
    }
  },
}));
vi.mock("./tracing/plugin", () => ({
  readPluginConfig: async () => ({ trace_to_braintrust: true, project: "test" }),
}));
vi.mock("./tools/bt-cli", () => ({
  BtCliToolsClient: class {
    async listProjects() {
      return [{ name: "Example", id: "p1" }];
    }
  },
}));
import plugin from "./server";

describe("OpenCode 2 capture", () => {
  it.each([undefined, "managed-run"])(
    "registers executable Braintrust tools (managed: %s)",
    async (managedRun) => {
      vi.stubEnv("BT_TRACE_MANAGED_RUN_ID", managedRun);
      const definitions: Array<{
        name: string;
        input: object;
        execute(input: unknown): Promise<{ content: string }>;
      }> = [];
      const cleanup = await plugin.setup({
        app: { version: "2.2.5" },
        session: { async hook() {} },
        location: { directory: "/test" },
        event: { async *subscribe() {} },
        tool: {
          async transform(callback) {
            callback({
              add(definition) {
                definitions.push(definition);
              },
            });
          },
        },
      });
      const projects = definitions.find(
        (definition) => definition.name === "braintrust_list_projects",
      );
      expect(projects).toBeDefined();
      expect(projects?.input).toMatchObject({ type: "object" });
      expect(await projects?.execute({})).toEqual({ content: "- Example (p1)" });
      await cleanup?.();
      vi.unstubAllEnvs();
    },
  );
  it("forwards native events in order and closes the daemon", async () => {
    state.logs.length = 0;
    state.closed = false;
    const events = [
      { type: "session.inbox.enqueued", created: 1, data: { sessionID: "s", inboxID: "i" } },
      { type: "session.step.started", created: 2, data: { sessionID: "s" } },
    ];
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
      session: { async hook() {} },
      location: { directory: "/test" },
      tool: {
        async transform(callback) {
          callback({ add() {} });
        },
      },
      event: {
        async *subscribe() {
          yield* events;
        },
      },
    });
    await cleanup?.();
    expect(state.logs.map((event) => event.event)).toEqual([
      "session_start",
      ...events.map((e) => e.type),
    ]);
    expect(state.logs[1]).toMatchObject({
      source: "opencode",
      source_version: "2.2.5",
      session_id: "s",
      ts_ms: 1,
      payload: events[0],
    });
    expect(state.closed).toBe(true);
  });

  it("routes an early child model context through its parent session", async () => {
    state.logs.length = 0;
    let capture: (event: {
      sessionID: string;
      system: Array<{ text: string }>;
      messages: unknown[];
    }) => void = () => {};
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
      location: { directory: "/test" },
      session: {
        async hook(_name, callback) {
          capture = callback;
        },
      },
      tool: {
        async transform(callback) {
          callback({ add() {} });
        },
      },
      event: {
        async *subscribe() {
          capture({
            sessionID: "child",
            system: [{ text: "system" }],
            messages: [{ role: "user", content: "hello" }],
          });
          yield { type: "session.created", data: { sessionID: "child", parentID: "parent" } };
          yield { type: "session.step.started", data: { sessionID: "child" } };
        },
      },
    });
    await cleanup?.();
    expect(state.logs.map((entry) => entry.event)).toEqual([
      "session.created",
      "model.context",
      "session.step.started",
    ]);
    expect(state.logs[1]).toMatchObject({
      session_id: "parent",
      payload: { data: { sessionID: "child" } },
    });
  });
  it("sends later events without waiting for acknowledgements and drains them on cleanup", async () => {
    state.logs.length = 0;
    state.closed = false;
    let release = () => {};
    state.gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const cleanup = await plugin.setup({
      app: { version: "2.0.0" },
      location: { directory: "/test" },
      session: { async hook() {} },
      tool: {
        async transform(callback) {
          callback({ add() {} });
        },
      },
      event: {
        async *subscribe() {
          yield { type: "session.created", data: { sessionID: "s" } };
          yield { type: "session.execution.succeeded", data: { sessionID: "s" } };
        },
      },
    });
    let finished = false;
    const closing = cleanup?.().then(() => {
      finished = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(state.logs.map((entry) => entry.event)).toEqual([
      "session.created",
      "session.execution.succeeded",
    ]);
    expect(finished).toBe(false);
    release();
    await closing;
    expect(state.closed).toBe(true);
    state.gate = undefined;
  });
});
