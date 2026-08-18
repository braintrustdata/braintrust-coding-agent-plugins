import {
  type DaemonSessionRoute,
  jsonRecord,
  parseJsonRecord,
  parseOptionalBoolean,
  resolveDaemonTraceSettings,
} from "./runtime/daemon-client";

/** OpenCode-specific behavior plus its independently selected tracing route. */

export interface BraintrustConfig {
  profile?: string;
  orgName?: string;
  projectName: string;
  tracingEnabled: boolean;
  enableTools: boolean;
  debug: boolean;
  additionalMetadata?: Record<string, unknown>;
  route: DaemonSessionRoute;
}

/**
 * Plugin config from opencode.json `braintrust` section.
 * Uses snake_case to match environment variable naming.
 */
export interface PluginConfig {
  profile?: string;
  org_name?: string;
  project?: string;
  trace_to_braintrust?: boolean;
  enable_tools?: boolean;
  debug?: boolean;
  additional_metadata?: Record<string, unknown>;
  route?: DaemonSessionRoute;
}

/**
 * Parse a boolean environment variable.
 * Accepts: "true", "TRUE", "1", "tRuE" (case-insensitive) as truthy.
 * All other values (including undefined, "false", "0", "no") are falsy.
 */
export function parseBooleanEnv(value: string | undefined): boolean {
  return parseOptionalBoolean(value) ?? false;
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
    orgName: undefined,
    projectName: "opencode",
    tracingEnabled: false,
    enableTools: true,
    debug: false,
    route: {
      destination: { type: "project_logs", project_name: "opencode" },
      flush_mode: "fire_and_forget",
    },
  };

  // Layer 1: Apply opencode.json config (if provided)
  if (pluginConfig) {
    const auth = pluginConfig.route?.auth;
    const destination = pluginConfig.route?.destination as
      | { type?: unknown; project_name?: unknown }
      | undefined;
    if (auth?.profile) defaults.profile = auth.profile;
    if (auth?.org_name) defaults.orgName = auth.org_name;
    if (
      destination?.type === "project_logs" &&
      typeof destination.project_name === "string" &&
      destination.project_name
    ) {
      defaults.projectName = destination.project_name;
    }
    if (pluginConfig.profile) defaults.profile = pluginConfig.profile;
    if (pluginConfig.org_name) defaults.orgName = pluginConfig.org_name;
    if (pluginConfig.project) defaults.projectName = pluginConfig.project;
    if (pluginConfig.trace_to_braintrust !== undefined) {
      defaults.tracingEnabled = pluginConfig.trace_to_braintrust;
    }
    if (pluginConfig.enable_tools !== undefined) {
      defaults.enableTools = pluginConfig.enable_tools;
    }
    if (pluginConfig.debug !== undefined) {
      defaults.debug = pluginConfig.debug;
    }
    defaults.additionalMetadata =
      jsonRecord(pluginConfig.additional_metadata) ??
      jsonRecord(pluginConfig.route?.additional_metadata) ??
      defaults.additionalMetadata;
  }

  // Layer 2: Apply environment variables (override opencode.json)
  const additionalMetadata =
    parseJsonRecord(process.env.BRAINTRUST_ADDITIONAL_METADATA) ?? defaults.additionalMetadata;

  const profile = process.env.BRAINTRUST_PROFILE || defaults.profile;
  const orgName = process.env.BRAINTRUST_ORG_NAME || defaults.orgName;
  const projectName = process.env.BRAINTRUST_PROJECT || defaults.projectName;
  const tracingEnabled =
    parseOptionalBoolean(process.env.TRACE_TO_BRAINTRUST) ?? defaults.tracingEnabled;
  const persistentRoute: DaemonSessionRoute = {
    ...(pluginConfig?.route ?? defaults.route),
    auth: {
      ...pluginConfig?.route?.auth,
      ...(profile ? { profile } : {}),
      ...(orgName ? { org_name: orgName } : {}),
    },
  };
  if (
    !pluginConfig?.route ||
    pluginConfig.project !== undefined ||
    process.env.BRAINTRUST_PROJECT !== undefined
  ) {
    persistentRoute.destination = { type: "project_logs", project_name: projectName };
  }
  if (additionalMetadata) persistentRoute.additional_metadata = additionalMetadata;
  const traceSettings = resolveDaemonTraceSettings({
    trace_to_braintrust: tracingEnabled,
    route: persistentRoute,
  });
  const route = traceSettings.route ?? persistentRoute;
  const routeAuth = route.auth;
  const routeDestination = route.destination as { type?: unknown; project_name?: unknown };

  return {
    profile: routeAuth?.profile ?? profile,
    orgName: routeAuth?.org_name ?? orgName,
    projectName:
      routeDestination?.type === "project_logs" && typeof routeDestination.project_name === "string"
        ? routeDestination.project_name
        : projectName,
    tracingEnabled: traceSettings.trace_to_braintrust === true,
    enableTools:
      parseOptionalBoolean(process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS) ?? defaults.enableTools,
    debug: parseOptionalBoolean(process.env.BRAINTRUST_DEBUG) ?? defaults.debug,
    additionalMetadata: jsonRecord(route.additional_metadata) ?? additionalMetadata,
    route,
  };
}
