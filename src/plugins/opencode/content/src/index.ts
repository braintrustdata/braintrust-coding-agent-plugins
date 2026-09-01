/**
 * Braintrust plugin for OpenCode
 *
 * Provides two main capabilities:
 * 1. Tracing - Automatically traces OpenCode sessions to Braintrust
 * 2. Data Access - Tools to query and interact with Braintrust data
 */

import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin";
import { loadConfig } from "./config";
import { createBraintrustTools } from "./tools";
import { BtCliToolsClient } from "./tools/bt-cli";
import { addTracingHooks, readPluginConfig } from "./tracing/plugin";

export const BraintrustPlugin: Plugin = async (input: PluginInput) => {
  const { client } = input;

  const pluginConfig = await readPluginConfig(input);

  const config = loadConfig(pluginConfig);

  const toolsClient = config.enableTools ? new BtCliToolsClient(config) : undefined;

  const hooks: Hooks = {};

  addTracingHooks(input, config, hooks);

  if (toolsClient) {
    hooks.tool = createBraintrustTools(toolsClient);
  }

  if (config.tracingEnabled || toolsClient) {
    client.app
      .log({
        body: {
          service: "braintrust",
          level: "info",
          message: `Braintrust plugin enabled for project "${config.projectName}"`,
        },
      })
      .catch(() => {});
  } else {
    client.app
      .log({
        body: {
          service: "braintrust",
          level: "info",
          message: "Braintrust tracing and tools are disabled.",
        },
      })
      .catch(() => {});
  }

  return hooks;
};

// Default export for OpenCode plugin loading
export default BraintrustPlugin;

// Re-export types only (not the class, since OpenCode will try to call all exports as plugins)
export type { BraintrustConfig, PluginConfig } from "./config";
