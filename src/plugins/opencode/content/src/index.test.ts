/**
 * Tests for OpenCode plugin registration behavior
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
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
    "BT_TRACE_INVOCATION_SETTINGS",
    "BT_TRACE_MANAGED_RUN_ID",
    // os.homedir() reads HOME on POSIX and USERPROFILE on Windows.
    "HOME",
    "USERPROFILE",
    "XDG_CONFIG_HOME",
  ];
  let directory: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "braintrust-opencode-plugin-"));
    for (const key of envVars) {
      originalEnv[key] = process.env[key];
      delete process.env[key];
    }
    process.env.HOME = directory;
    process.env.USERPROFILE = directory;
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
    const configDir = join(directory, ".config", "opencode");
    mkdirSync(configDir, { recursive: true });
    writeFileSync(
      join(configDir, "braintrust.json"),
      JSON.stringify({ trace_to_braintrust: true }),
    );
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "false";

    const { BraintrustPlugin } = await import("./index");
    const hooks = await BraintrustPlugin(createInput(directory));

    expect(hooks.event).toBeDefined();
    expect(hooks["chat.message"]).toBeDefined();
    expect(hooks["tool.execute.before"]).toBeDefined();
    expect(hooks["tool.execute.after"]).toBeDefined();
    expect(hooks.tool).toBeUndefined();
  });

  it("lets project tracing override the user default and managed runs override both", async () => {
    const global = join(directory, ".config", "opencode");
    const project = directory;
    mkdirSync(global, { recursive: true });
    mkdirSync(project, { recursive: true });
    writeFileSync(join(global, "braintrust.json"), JSON.stringify({ trace_to_braintrust: false }));
    writeFileSync(join(project, ".braintrust.json"), JSON.stringify({ trace_to_braintrust: true }));
    const { BraintrustPlugin } = await import("./index");
    expect((await BraintrustPlugin(createInput(directory))).event).toBeDefined();
    writeFileSync(join(global, "braintrust.json"), JSON.stringify({ trace_to_braintrust: true }));
    writeFileSync(
      join(project, ".braintrust.json"),
      JSON.stringify({ trace_to_braintrust: false }),
    );
    const paused = await BraintrustPlugin(createInput(directory));
    expect(paused.event).toBeUndefined();
    expect(paused.tool).toBeDefined();
    process.env.BT_TRACE_INVOCATION_SETTINGS = JSON.stringify({
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "managed" } },
    });
    expect((await BraintrustPlugin(createInput(directory))).event).toBeDefined();
  });

  it("does not bypass a project opt-out when its plugin list is invalid", async () => {
    const global = join(directory, ".config", "opencode");
    mkdirSync(global, { recursive: true });
    writeFileSync(join(global, "braintrust.json"), JSON.stringify({ trace_to_braintrust: true }));
    writeFileSync(
      join(directory, ".braintrust.json"),
      JSON.stringify({ trace_to_braintrust: false, route: { span_plugins: [null] } }),
    );
    const { BraintrustPlugin } = await import("./index");
    expect((await BraintrustPlugin(createInput(directory))).event).toBeUndefined();
    process.env.BT_TRACE_INVOCATION_SETTINGS = JSON.stringify({
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "run" } },
    });
    expect((await BraintrustPlugin(createInput(directory))).event).toBeDefined();
  });

  it("recursively merges project metadata and appends arrays", async () => {
    const global = join(directory, ".config", "opencode");
    const project = directory;
    mkdirSync(global, { recursive: true });
    mkdirSync(project, { recursive: true });
    writeFileSync(
      join(global, "braintrust.json"),
      JSON.stringify({
        trace_to_braintrust: false,
        route: {
          destination: { type: "project_logs", project_name: "global" },
          additional_metadata: { nested: { a: 1, b: 2 }, list: [1, 2] },
        },
      }),
    );
    writeFileSync(
      join(project, ".braintrust.json"),
      JSON.stringify({
        trace_to_braintrust: true,
        route: { additional_metadata: { nested: { b: 3 }, list: [4] } },
      }),
    );
    const { readPluginConfig } = await import("./tracing/plugin");
    const { loadConfig } = await import("./config");
    const config = loadConfig(await readPluginConfig({ directory }));
    expect(config.tracingEnabled).toBe(true);
    expect(config.route.additional_metadata).toEqual({ nested: { a: 1, b: 3 }, list: [1, 2, 4] });
  });

  it("inherits the user route while applying explicit project selections", async () => {
    const global = join(directory, ".config", "opencode");
    const project = directory;
    mkdirSync(global, { recursive: true });
    mkdirSync(project, { recursive: true });
    mkdirSync(join(directory, ".git"));
    const nested = join(directory, "src");
    mkdirSync(nested);
    writeFileSync(
      join(global, "braintrust.json"),
      JSON.stringify({
        trace_to_braintrust: true,
        route: {
          auth: { source: "environment", profile_id: "old", org_name: "org" },
          destination: { type: "project_logs", project_name: "global" },
          span_plugins: ["trusted.js"],
        },
      }),
    );
    writeFileSync(
      join(project, ".braintrust.json"),
      JSON.stringify({
        project: "local",
        profile: "new",
        route: { span_plugins: ["untrusted.js"] },
      }),
    );
    const { readPluginConfig } = await import("./tracing/plugin");
    const { loadConfig } = await import("./config");
    const config = loadConfig(await readPluginConfig({ directory: nested }));
    expect(config.route.destination).toEqual({ type: "project_logs", project_name: "local" });
    expect(config.route.auth).toMatchObject({
      source: "saved_profile",
      profile: "new",
      org_name: "org",
    });
    expect(config.route.auth?.profile_id).toBeUndefined();
    expect(config.route.span_plugins).toEqual(["trusted.js", join(directory, "untrusted.js")]);
    writeFileSync(
      join(project, ".braintrust.json"),
      JSON.stringify({
        inherit_plugins: false,
        route: { span_plugins: ["project.js"] },
      }),
    );
    const replaced = loadConfig(await readPluginConfig({ directory: nested }));
    expect(replaced.route.span_plugins).toEqual([join(directory, "project.js")]);
  });

  it("provides a trace-only managed entrypoint without data tools", async () => {
    process.env.BT_TRACE_MANAGED_RUN_ID = `trace-only-${Date.now()}`;
    process.env.BT_TRACE_INVOCATION_SETTINGS = JSON.stringify({
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "managed" } },
    });
    const { default: tracingPlugin } = await import("./tracing");
    const hooks = await tracingPlugin(createInput(directory));

    expect(hooks.event).toBeDefined();
    expect(hooks["chat.message"]).toBeDefined();
    expect(hooks.tool).toBeUndefined();
  });

  it("registers one tracing adapter when managed and installed copies load", async () => {
    process.env.BT_TRACE_MANAGED_RUN_ID = `dedupe-${Date.now()}`;
    process.env.BT_TRACE_INVOCATION_SETTINGS = JSON.stringify({
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "managed" } },
    });
    const [{ BraintrustPlugin }, { default: tracingPlugin }] = await Promise.all([
      import("./index"),
      import("./tracing"),
    ]);

    const installedHooks = await BraintrustPlugin(createInput(directory));
    const injectedHooks = await tracingPlugin(createInput(directory));
    expect(installedHooks.event).toBeDefined();
    expect(injectedHooks.event).toBeUndefined();
  });
});
