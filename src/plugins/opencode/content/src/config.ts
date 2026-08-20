import { type DaemonSessionRoute, resolveDaemonTraceSettings } from "./runtime/daemon-client";

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
  if (!value) return false;
  const normalized = value.toLowerCase();
  return normalized === "true" || normalized === "1";
}

/**
 * Load Braintrust config with the following precedence (later overrides earlier):
 * 1. Default values
 * 2. opencode.json `braintrust` section (pluginConfig)
 * 3. `bt trace run` invocation settings (tracing only, highest priority)
 *
 * Routing and enablement are never read directly from the environment;
 * `braintrust.json` is the only persistent source, and `bt trace run` is the
 * only per-invocation override.
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
    if (pluginConfig.additional_metadata) {
      defaults.additionalMetadata = pluginConfig.additional_metadata;
    }
  }

  const persistentRoute: DaemonSessionRoute = {
    ...(pluginConfig?.route ?? defaults.route),
    auth: {
      ...pluginConfig?.route?.auth,
      ...(defaults.profile ? { profile: defaults.profile } : {}),
      ...(defaults.orgName ? { org_name: defaults.orgName } : {}),
    },
  };
  if (!pluginConfig?.route || pluginConfig.project !== undefined) {
    persistentRoute.destination = { type: "project_logs", project_name: defaults.projectName };
  }
  if (defaults.additionalMetadata)
    persistentRoute.additional_metadata = defaults.additionalMetadata;
  const traceSettings = resolveDaemonTraceSettings({
    trace_to_braintrust: defaults.tracingEnabled,
    route: persistentRoute,
  });
  const route = traceSettings.route ?? persistentRoute;
  const routeAuth = route.auth;
  const routeDestination = route.destination as { type?: unknown; project_name?: unknown };

  return {
    profile: routeAuth?.profile ?? defaults.profile,
    orgName: routeAuth?.org_name ?? defaults.orgName,
    projectName:
      routeDestination?.type === "project_logs" && typeof routeDestination.project_name === "string"
        ? routeDestination.project_name
        : defaults.projectName,
    tracingEnabled: traceSettings.trace_to_braintrust === true,
    enableTools: process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS
      ? parseBooleanEnv(process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS)
      : defaults.enableTools,
    debug: process.env.BRAINTRUST_DEBUG
      ? parseBooleanEnv(process.env.BRAINTRUST_DEBUG)
      : defaults.debug,
    additionalMetadata: defaults.additionalMetadata,
    route,
  };
}
