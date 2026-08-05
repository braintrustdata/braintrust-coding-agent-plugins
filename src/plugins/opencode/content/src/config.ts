/** Shared OpenCode plugin configuration. */

export interface BraintrustConfig {
  profile?: string
  apiKey: string
  apiUrl?: string
  appUrl: string
  orgName?: string
  projectName: string
  tracingEnabled: boolean
  enableTools: boolean
  debug: boolean
  additionalMetadata?: Record<string, unknown>
}

/**
 * Plugin config from opencode.json `braintrust` section.
 * Uses snake_case to match environment variable naming.
 */
export interface PluginConfig {
  profile?: string
  api_key?: string
  api_url?: string
  app_url?: string
  org_name?: string
  project?: string
  trace_to_braintrust?: boolean
  enable_tools?: boolean
  debug?: boolean
  additional_metadata?: Record<string, unknown>
}

/**
 * Parse a boolean environment variable.
 * Accepts: "true", "TRUE", "1", "tRuE" (case-insensitive) as truthy.
 * All other values (including undefined, "false", "0", "no") are falsy.
 */
export function parseBooleanEnv(value: string | undefined): boolean {
  if (!value) return false
  const normalized = value.toLowerCase()
  return normalized === "true" || normalized === "1"
}

/**
 * Load Braintrust config with the following precedence (later overrides earlier):
 * 1. Default values
 * 2. opencode.json `braintrust` section (pluginConfig)
 * 3. Environment variables (highest priority)
 */
export function loadConfig(pluginConfig?: PluginConfig): BraintrustConfig {
  // Defaults
  const defaults: BraintrustConfig = {
    profile: undefined,
    apiKey: "",
    apiUrl: "https://api.braintrust.dev",
    appUrl: "https://www.braintrust.dev",
    orgName: undefined,
    projectName: "opencode",
    tracingEnabled: false,
    enableTools: true,
    debug: false,
  }

  // Layer 1: Apply opencode.json config (if provided)
  if (pluginConfig) {
    if (pluginConfig.profile) defaults.profile = pluginConfig.profile
    if (pluginConfig.api_key) defaults.apiKey = pluginConfig.api_key
    if (pluginConfig.api_url) defaults.apiUrl = pluginConfig.api_url
    if (pluginConfig.app_url) defaults.appUrl = pluginConfig.app_url
    if (pluginConfig.org_name) defaults.orgName = pluginConfig.org_name
    if (pluginConfig.project) defaults.projectName = pluginConfig.project
    if (pluginConfig.trace_to_braintrust !== undefined) {
      defaults.tracingEnabled = pluginConfig.trace_to_braintrust
    }
    if (pluginConfig.enable_tools !== undefined) {
      defaults.enableTools = pluginConfig.enable_tools
    }
    if (pluginConfig.debug !== undefined) {
      defaults.debug = pluginConfig.debug
    }
    if (pluginConfig.additional_metadata) {
      defaults.additionalMetadata = pluginConfig.additional_metadata
    }
  }

  // Layer 2: Apply environment variables (override opencode.json)
  let additionalMetadata = defaults.additionalMetadata
  if (process.env.BRAINTRUST_ADDITIONAL_METADATA) {
    try {
      additionalMetadata = JSON.parse(process.env.BRAINTRUST_ADDITIONAL_METADATA)
    } catch {
      // Invalid JSON in env var — ignore and keep config file value (if any)
    }
  }

  return {
    profile: process.env.BRAINTRUST_PROFILE || defaults.profile,
    apiKey: process.env.BRAINTRUST_API_KEY || defaults.apiKey,
    apiUrl: process.env.BRAINTRUST_API_URL || defaults.apiUrl,
    appUrl: process.env.BRAINTRUST_APP_URL || defaults.appUrl,
    orgName: process.env.BRAINTRUST_ORG_NAME || defaults.orgName,
    projectName: process.env.BRAINTRUST_PROJECT || defaults.projectName,
    tracingEnabled: process.env.TRACE_TO_BRAINTRUST
      ? parseBooleanEnv(process.env.TRACE_TO_BRAINTRUST)
      : defaults.tracingEnabled,
    enableTools: process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS
      ? parseBooleanEnv(process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS)
      : defaults.enableTools,
    debug: process.env.BRAINTRUST_DEBUG
      ? parseBooleanEnv(process.env.BRAINTRUST_DEBUG)
      : defaults.debug,
    additionalMetadata,
  }
}
