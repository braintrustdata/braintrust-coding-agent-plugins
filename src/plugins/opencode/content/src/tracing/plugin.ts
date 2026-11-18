import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin";
import { loadConfig, type PluginConfig } from "../config";
import { claimManagedTracingInstance } from "../runtime/daemon-client";
import { createDaemonTracingHooks } from "./daemon";

export async function readPluginConfig(input: {
  directory: string;
}): Promise<PluginConfig | undefined> {
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
    let globallyDisabled = false;
    for (const [index, configPath] of configPaths.entries()) {
      try {
        if (fs.existsSync(configPath)) {
          const parsed = JSON.parse(fs.readFileSync(configPath, "utf-8")) as PluginConfig & {
            tracing_disabled?: boolean;
          };
          if (index === 0) globallyDisabled = parsed.tracing_disabled === true;
          pluginConfig = pluginConfig ? { ...pluginConfig, ...parsed } : parsed;
        }
      } catch {
        // Malformed optional config is fail-open; continue to the next layer.
      }
    }
    if (globallyDisabled) pluginConfig = { ...pluginConfig, trace_to_braintrust: false };
  } catch {
    // Config loading failed; invocation settings can still enable tracing.
  }
  return pluginConfig;
}

export function addTracingHooks(
  input: PluginInput,
  config: ReturnType<typeof loadConfig>,
  hooks: Hooks,
): void {
  if (!config.tracingEnabled || !claimManagedTracingInstance("opencode")) return;
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

/** Trace-only entrypoint injected by `bt trace run opencode`. */
export const BraintrustTracingPlugin: Plugin = async (input: PluginInput) => {
  const config = loadConfig(await readPluginConfig(input));
  const hooks: Hooks = {};
  addTracingHooks(input, config, hooks);
  return hooks;
};
