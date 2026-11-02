import { describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ logs: [] as Array<Record<string, unknown>>, closed: false }));
vi.mock("./runtime/daemon-client", () => ({
  claimManagedTracingInstance: () => true,
  resolveDaemonTraceSettings: (settings: unknown) => settings,
  DaemonClient: class {
    async log(event: Record<string, unknown>) {
      state.logs.push(event);
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
  it("registers executable Braintrust tools", async () => {
    const definitions: Array<{
      name: string;
      input: object;
      execute(input: unknown): Promise<{ content: string }>;
    }> = [];
    const cleanup = await plugin.setup({
      app: { version: "2.2.5" },
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
});
