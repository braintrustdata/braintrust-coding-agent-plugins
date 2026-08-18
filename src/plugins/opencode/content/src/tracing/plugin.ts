import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin";
import { loadConfig, type PluginConfig } from "../config";
import { claimManagedTracingInstance } from "../runtime/daemon-client";
import { createDaemonTracingHooks } from "./daemon";

export async function readPluginConfig(input: PluginInput): Promise<PluginConfig | undefined> {
  let pluginConfig: PluginConfig | undefined;
  try {
    const fs = await import("node:fs");
    const path = await import("node:path");
    const os = await import("node:os");
    const configHome = process.env.XDG_CONFIG_HOME || path.join(os.homedir(), ".config");
    const configPaths = [
      path.join(configHome, "opencode", "braintrust.json"),
      path.join(input.directory, ".opencode", "braintrust.json"),
    ];

    for (const configPath of configPaths) {
      try {
        if (fs.existsSync(configPath)) {
          const parsed = JSON.parse(fs.readFileSync(configPath, "utf-8")) as PluginConfig;
          pluginConfig = pluginConfig ? { ...pluginConfig, ...parsed } : parsed;
        }
      } catch {
        // Malformed optional config is fail-open; continue to the next layer.
      }
    }
  } catch {
    // Config loading failed; environment settings can still enable tracing.
  }
  return pluginConfig;
}

export function addTracingHooks(
  input: PluginInput,
  config: ReturnType<typeof loadConfig>,
  hooks: Hooks,
): void {
  if (!config.tracingEnabled) return;
  if (!claimManagedTracingInstance("opencode")) {
    input.client.app
      .log({
        body: {
          service: "braintrust-trace",
          level: "info",
          message: "Managed tracing is already registered by another plugin instance.",
        },
      })
      .catch(() => {});
    return;
  }

  const tracingHooks = createDaemonTracingHooks(input, config, (message, extra) => {
    input.client.app
      .log({ body: { service: "braintrust-trace", level: "warn", message, extra } })
      .catch(() => {});
  });
  Object.assign(hooks, tracingHooks);

  input.client.app
    .log({
      body: {
        service: "braintrust",
        level: "info",
        message: `Tracing hooks registered: ${Object.keys(tracingHooks).join(", ")}`,
      },
    })
    .catch(() => {});
}

/** Trace-only entrypoint used by `bt trace run opencode`. */
export const BraintrustTracingPlugin: Plugin = async (input: PluginInput) => {
  const config = loadConfig(await readPluginConfig(input));
  const hooks: Hooks = {};
  addTracingHooks(input, config, hooks);
  return hooks;
};
