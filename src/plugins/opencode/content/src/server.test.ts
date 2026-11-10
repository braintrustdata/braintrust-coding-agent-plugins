import { describe, expect, it, vi } from "vite-plus/test";

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
      await state.gate;
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
      await state.gate;
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
        session: {
          async hook() {},
          async get() {
            return {};
          },
        },
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
  it("loads and keeps tracing when host registration APIs fail", async () => {
    state.logs = [];
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
      session: {
        async hook() {
          throw new Error("context hook unavailable");
        },
        async get() {
          return {};
        },
      },
      location: { directory: "/test" },
      event: {
        async *subscribe() {
          yield { type: "session.created", data: { sessionID: "root" } };
        },
      },
      tool: {} as never,
    });
    expect(cleanup).toBeTypeOf("function");
    await cleanup?.();
    expect(state.logs.map((log) => log.event)).toEqual(["session.created"]);
  });

  it("forwards native events in order and closes the daemon", async () => {
    state.logs.length = 0;
    state.closed = false;
    const events = [
      { type: "session.inbox.enqueued", created: 1, data: { sessionID: "s", inboxID: "i" } },
      { type: "session.step.started", created: 2, data: { sessionID: "s" } },
    ];
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
      session: {
        async hook() {},
        async get() {
          return {};
        },
      },
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
    await vi.waitFor(() => expect(state.logs.length).toBeGreaterThan(0));
    await cleanup?.();
    await vi.waitFor(() => expect(state.closed).toBe(true));
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
        async get() {
          return {};
        },
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
    await vi.waitFor(() => expect(state.logs.length).toBeGreaterThan(0));
    await cleanup?.();
    await vi.waitFor(() => expect(state.closed).toBe(true));
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
  it("starts and forwards without waiting for daemon acknowledgements", async () => {
    state.logs.length = 0;
    state.closed = false;
    let release = () => {};
    state.gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const cleanup = await plugin.setup({
      app: { version: "2.0.0" },
      location: { directory: "/test" },
      session: {
        async hook() {},
        async get() {
          return {};
        },
      },
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
    await vi.waitFor(() => expect(state.logs).toHaveLength(2));
    const closing = cleanup?.();
    expect(state.logs.map((entry) => entry.event)).toEqual([
      "session.created",
      "session.execution.succeeded",
    ]);
    expect(state.closed).toBe(false);
    release();
    await closing;
    await vi.waitFor(() => expect(state.closed).toBe(true));
    state.gate = undefined;
  });
  it("resolves a resumed nested session to its original root", async () => {
    state.logs.length = 0;
    state.closed = false;
    const parents: Record<string, string | undefined> = { child: "parent", parent: "root" };
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
      location: { directory: "/test" },
      session: {
        async hook() {},
        async get({ sessionID }) {
          return { parentID: parents[sessionID] };
        },
      },
      tool: {
        async transform(callback) {
          callback({ add() {} });
        },
      },
      event: {
        async *subscribe() {
          yield { type: "session.step.started", data: { sessionID: "child" } };
        },
      },
    });
    await vi.waitFor(() => expect(state.logs).toHaveLength(2));
    expect(state.logs.map((entry) => entry.session_id)).toEqual(["root", "root"]);
    await cleanup?.();
    await vi.waitFor(() => expect(state.closed).toBe(true));
  });
});
