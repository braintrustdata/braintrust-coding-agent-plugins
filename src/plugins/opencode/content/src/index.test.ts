/**
 * Tests for OpenCode plugin registration behavior
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { PluginInput } from "@opencode-ai/plugin";

vi.mock("@opencode-ai/plugin", () => ({
  tool: Object.assign((definition: unknown) => definition, {
    schema: {
      string: () => ({
        optional() {
          return this;
        },
        describe() {
          return this;
        },
      }),
      number: () => ({
        optional() {
          return this;
        },
        describe() {
          return this;
        },
      }),
    },
  }),
}));

function createInput(directory: string): PluginInput {
  return {
    directory,
    worktree: directory,
    project: "test-project",
    client: {
      app: {
        log: async () => {},
      },
    },
  } as unknown as PluginInput;
}

describe("BraintrustPlugin", () => {
  const originalEnv: Record<string, string | undefined> = {};
  const envVars = [
    "TRACE_TO_BRAINTRUST",
    "BRAINTRUST_PROFILE",
    "BRAINTRUST_OPENCODE_ENABLE_TOOLS",
    "HOME",
  ];
  let directory: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "braintrust-opencode-plugin-"));
    for (const key of envVars) {
      originalEnv[key] = process.env[key];
      delete process.env[key];
    }
    process.env.HOME = directory;
    process.env.TRACE_TO_BRAINTRUST = "false";
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "true";
  });

  afterEach(() => {
    rmSync(directory, { recursive: true, force: true });
    for (const key of envVars) {
      if (originalEnv[key] !== undefined) {
        process.env[key] = originalEnv[key];
      } else {
        delete process.env[key];
      }
    }
  });

  it("registers Braintrust tools when enabled", async () => {
    const { BraintrustPlugin } = await import("./index");
    const hooks = await BraintrustPlugin(createInput(directory));
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(hooks.tool).toBeDefined();
    expect(Object.keys(hooks.tool ?? {}).sort()).toEqual([
      "braintrust_get_experiments",
      "braintrust_list_projects",
      "braintrust_log_data",
      "braintrust_query_logs",
    ]);
  });

  it("does not register Braintrust tools when disabled", async () => {
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "false";

    const { BraintrustPlugin } = await import("./index");
    const hooks = await BraintrustPlugin(createInput(directory));
    await Promise.resolve();

    expect(hooks.tool).toBeUndefined();
  });

  it("registers daemon tracing independently of the tools", async () => {
    process.env.TRACE_TO_BRAINTRUST = "true";
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "false";

    const { BraintrustPlugin } = await import("./index");
    const hooks = await BraintrustPlugin(createInput(directory));

    expect(hooks.event).toBeDefined();
    expect(hooks["chat.message"]).toBeDefined();
    expect(hooks["tool.execute.before"]).toBeDefined();
    expect(hooks["tool.execute.after"]).toBeDefined();
    expect(hooks.tool).toBeUndefined();
  });
});
