/**
 * Braintrust plugin for OpenCode
 *
 * Provides two main capabilities:
 * 1. Tracing - Automatically traces OpenCode sessions to Braintrust
 * 2. Data Access - Tools to query and interact with Braintrust data
 */

import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin"
import { loadConfig, type PluginConfig } from "./config"
import { createBraintrustTools } from "./tools"
import { BtCliToolsClient } from "./tools/bt-cli"
import { createDaemonTracingHooks } from "./tracing/daemon"

export const BraintrustPlugin: Plugin = async (input: PluginInput) => {
  const { client } = input

  // Load plugin config from config files
  // Precedence: global config -> project config (project overrides global)
  let pluginConfig: PluginConfig | undefined
  try {
    const fs = await import("node:fs")
    const path = await import("node:path")
    const os = await import("node:os")

    // Load configs in order: global first, then project (so project overrides global)
    const configPaths = [
      path.join(os.homedir(), ".config", "opencode", "braintrust.json"), // global
      path.join(input.directory, ".opencode", "braintrust.json"), // project
    ]

    for (const configPath of configPaths) {
      try {
        if (fs.existsSync(configPath)) {
          const content = fs.readFileSync(configPath, "utf-8")
          const parsed = JSON.parse(content) as PluginConfig
          // Merge: later config overrides earlier
          pluginConfig = pluginConfig ? { ...pluginConfig, ...parsed } : parsed
        }
      } catch {
        // Continue to next path
      }
    }
  } catch {
    // Config loading failed, proceed with env vars only
  }

  const config = loadConfig(pluginConfig)

  const toolsClient = config.enableTools ? new BtCliToolsClient(config) : undefined

  const hooks: Hooks = {}

  // Add tracing hooks if enabled
  if (config.tracingEnabled) {
    const tracingHooks = createDaemonTracingHooks(input, config, (message, extra) => {
      client.app
        .log({ body: { service: "braintrust-trace", level: "warn", message, extra } })
        .catch(() => {})
    })
    Object.assign(hooks, tracingHooks)

    client.app
      .log({
        body: {
          service: "braintrust",
          level: "info",
          message: `Tracing hooks registered: ${Object.keys(tracingHooks).join(", ")}`,
        },
      })
      .catch(() => {})
  }

  if (toolsClient) {
    hooks.tool = createBraintrustTools(toolsClient)
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
      .catch(() => {})
  } else {
    client.app
      .log({
        body: {
          service: "braintrust",
          level: "info",
          message: "Braintrust tracing and tools are disabled.",
        },
      })
      .catch(() => {})
  }

  return hooks
}

// Default export for OpenCode plugin loading
export default BraintrustPlugin

// Re-export types only (not the class, since OpenCode will try to call all exports as plugins)
export type { BraintrustConfig, PluginConfig } from "./config"
