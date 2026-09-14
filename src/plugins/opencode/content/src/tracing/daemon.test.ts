import { beforeEach, describe, expect, it, vi } from "vitest";
import type { PluginInput } from "@opencode-ai/plugin";
import type { Event } from "@opencode-ai/sdk";

const mockState = vi.hoisted(() => ({
  logs: [] as Array<Record<string, unknown>>,
  flushes: [] as string[],
  closed: 0,
  logGate: undefined as Promise<void> | undefined,
}));

vi.mock("../runtime/daemon-client", () => ({
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
    async close(): Promise<void> {
      mockState.closed += 1;
    }
  },
}));

import { createDaemonTracingHooks } from "./daemon";

describe("OpenCode daemon adapter", () => {
  beforeEach(() => {
    mockState.logs.length = 0;
    mockState.flushes.length = 0;
    mockState.closed = 0;
    mockState.logGate = undefined;
  });

  it("awaits journal acknowledgement but leaves lifecycle delivery flushing to the daemon", async () => {
    const input = {
      directory: "/tmp/project",
      worktree: "/tmp/project",
    } as PluginInput;
    const hooks = createDaemonTracingHooks(
      input,
      {
        projectName: "agents",
        route: {
          destination: { type: "project_logs", project_name: "agents" },
          flush_mode: "fire_and_forget",
        },
      },
      () => {},
    );
    const handleEvent = hooks.event as (input: { event: Event }) => Promise<void>;

    let acknowledge!: () => void;
    mockState.logGate = new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    let idleForwarded = false;
    const idle = handleEvent({
      event: { type: "session.idle", properties: { sessionID: "native-session" } } as Event,
    }).then(() => {
      idleForwarded = true;
    });
    await Promise.resolve();
    expect(idleForwarded).toBe(false);
    acknowledge();
    await idle;

    mockState.logGate = undefined;
    for (const type of ["session.deleted", "session.error"] as const) {
      await handleEvent({
        event: { type, properties: { sessionID: "native-session" } } as Event,
      });
    }
    await handleEvent({
      event: { type: "server.instance.disposed", properties: {} } as Event,
    });

    expect(mockState.logs.map((log) => log.event)).toEqual([
      "session.idle",
      "session.deleted",
      "session.error",
      "server.instance.disposed",
    ]);
    expect(mockState.logs.every((log) => log.source === "opencode")).toBe(true);
    expect(mockState.flushes).toHaveLength(0);
    expect(mockState.closed).toBe(1);
  });
});
