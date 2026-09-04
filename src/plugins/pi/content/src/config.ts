import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { type DaemonSessionRoute, resolveDaemonTraceSettings } from "./runtime/daemon-client.ts";
import { loadPiPackageMetadata } from "./pi-package.ts";

export interface PiConfig {
  enabled: boolean;
  profile?: string;
  orgName?: string;
  projectName: string;
  additionalMetadata?: Record<string, unknown>;
  showUi: boolean;
  showTraceLink: boolean;
  route: DaemonSessionRoute;
}

type ConfigRecord = Record<string, unknown>;

const PROJECT_CONFIG_DIR_NAME = loadPiPackageMetadata().configDir;

function record(value: unknown): ConfigRecord | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as ConfigRecord)
    : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value : undefined;
}

function boolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string" && typeof value !== "number") return undefined;
  switch (String(value).trim().toLowerCase()) {
    case "1":
    case "true":
    case "yes":
    case "on":
      return true;
    case "0":
    case "false":
    case "no":
    case "off":
      return false;
    default:
      return undefined;
  }
}

function readConfig(path: string): ConfigRecord | undefined {
  if (!existsSync(path)) return undefined;
  try {
    return record(JSON.parse(readFileSync(path, "utf8")));
  } catch {
    return undefined;
  }
}

function applyConfig(config: PiConfig, source: ConfigRecord | undefined): void {
  if (!source) return;
  const route = record(source.route) as DaemonSessionRoute | undefined;
  if (route?.destination !== undefined) config.route = route;
  const destination = record(route?.destination);
  const profile = nonEmptyString(route?.auth?.profile) ?? nonEmptyString(source.profile);
  const orgName = nonEmptyString(route?.auth?.org_name) ?? nonEmptyString(source.org_name);
  const routeProject =
    destination?.type === "project_logs" ? nonEmptyString(destination.project_name) : undefined;
  const projectName = routeProject ?? nonEmptyString(source.project);
  config.profile = profile ?? config.profile;
  config.orgName = orgName ?? config.orgName;
  config.projectName = projectName ?? config.projectName;
  config.enabled = boolean(source.trace_to_braintrust) ?? config.enabled;
  config.additionalMetadata = record(source.additional_metadata) ?? config.additionalMetadata;
  config.showUi = boolean(source.show_ui) ?? config.showUi;
  config.showTraceLink = boolean(source.show_trace_link) ?? config.showTraceLink;

  const additionalMetadata = record(source.additional_metadata);
  if (profile || orgName) {
    config.route.auth = {
      ...config.route.auth,
      ...(profile ? { profile } : {}),
      ...(orgName ? { org_name: orgName } : {}),
    };
  }
  if (projectName) {
    config.route.destination = { type: "project_logs", project_name: projectName };
  }
  if (additionalMetadata) config.route.additional_metadata = additionalMetadata;
}

export function loadConfig(cwd = process.cwd()): PiConfig {
  const config: PiConfig = {
    enabled: false,
    projectName: "pi",
    showUi: true,
    showTraceLink: true,
    route: {
      destination: { type: "project_logs", project_name: "pi" },
      flush_mode: "flush_on_turn_end",
    },
  };

  applyConfig(config, readConfig(join(homedir(), ".pi", "agent", "braintrust.json")));
  applyConfig(config, readConfig(join(cwd, PROJECT_CONFIG_DIR_NAME, "braintrust.json")));

  config.showUi = boolean(process.env.BRAINTRUST_SHOW_UI) ?? config.showUi;
  config.showTraceLink = boolean(process.env.BRAINTRUST_SHOW_TRACE_LINK) ?? config.showTraceLink;

  const traceSettings = resolveDaemonTraceSettings({
    trace_to_braintrust: config.enabled,
    route: config.route,
  });
  config.enabled = traceSettings.trace_to_braintrust === true;
  config.route = traceSettings.route ?? config.route;

  return config;
}
