import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import * as piCodingAgent from "@earendil-works/pi-coding-agent";
import {
  type DaemonSessionRoute,
  jsonRecord,
  parseJsonRecord,
  parseOptionalBoolean,
  resolveDaemonTraceSettings,
} from "./runtime/daemon-client.ts";

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

const PROJECT_CONFIG_DIR_NAME =
  typeof (piCodingAgent as { CONFIG_DIR_NAME?: unknown }).CONFIG_DIR_NAME === "string"
    ? (piCodingAgent as { CONFIG_DIR_NAME: string }).CONFIG_DIR_NAME
    : ".pi";

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value : undefined;
}

function readConfig(path: string): ConfigRecord | undefined {
  if (!existsSync(path)) return undefined;
  try {
    return jsonRecord(JSON.parse(readFileSync(path, "utf8")));
  } catch {
    return undefined;
  }
}

function applyConfig(config: PiConfig, source: ConfigRecord | undefined): void {
  if (!source) return;
  const route = jsonRecord(source.route) as DaemonSessionRoute | undefined;
  if (route?.destination !== undefined) config.route = route;
  const destination = jsonRecord(route?.destination);
  config.profile = nonEmptyString(route?.auth?.profile) ?? config.profile;
  config.orgName = nonEmptyString(route?.auth?.org_name) ?? config.orgName;
  if (destination?.type === "project_logs") {
    config.projectName = nonEmptyString(destination.project_name) ?? config.projectName;
  }
  config.profile = nonEmptyString(source.profile) ?? config.profile;
  config.orgName = nonEmptyString(source.org_name) ?? config.orgName;
  config.projectName = nonEmptyString(source.project) ?? config.projectName;
  config.enabled = parseOptionalBoolean(source.trace_to_braintrust) ?? config.enabled;
  config.additionalMetadata =
    jsonRecord(source.additional_metadata) ??
    jsonRecord(route?.additional_metadata) ??
    config.additionalMetadata;
  config.showUi = parseOptionalBoolean(source.show_ui) ?? config.showUi;
  config.showTraceLink = parseOptionalBoolean(source.show_trace_link) ?? config.showTraceLink;

  const profile = nonEmptyString(source.profile);
  const orgName = nonEmptyString(source.org_name);
  const projectName = nonEmptyString(source.project);
  const additionalMetadata =
    jsonRecord(source.additional_metadata) ?? jsonRecord(route?.additional_metadata);
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

  config.profile = nonEmptyString(process.env.BRAINTRUST_PROFILE) ?? config.profile;
  config.orgName = nonEmptyString(process.env.BRAINTRUST_ORG_NAME) ?? config.orgName;
  config.projectName = nonEmptyString(process.env.BRAINTRUST_PROJECT) ?? config.projectName;
  config.enabled = parseOptionalBoolean(process.env.TRACE_TO_BRAINTRUST) ?? config.enabled;
  config.additionalMetadata =
    parseJsonRecord(process.env.BRAINTRUST_ADDITIONAL_METADATA) ?? config.additionalMetadata;
  config.showUi = parseOptionalBoolean(process.env.BRAINTRUST_SHOW_UI) ?? config.showUi;
  config.showTraceLink =
    parseOptionalBoolean(process.env.BRAINTRUST_SHOW_TRACE_LINK) ?? config.showTraceLink;

  const persistentRoute: DaemonSessionRoute = {
    ...config.route,
    auth: {
      ...config.route.auth,
      ...(config.profile ? { profile: config.profile } : {}),
      ...(config.orgName ? { org_name: config.orgName } : {}),
    },
  };
  if (process.env.BRAINTRUST_PROJECT !== undefined) {
    persistentRoute.destination = { type: "project_logs", project_name: config.projectName };
  }
  if (config.additionalMetadata) {
    persistentRoute.additional_metadata = config.additionalMetadata;
  } else {
    persistentRoute.additional_metadata = undefined;
  }
  const traceSettings = resolveDaemonTraceSettings({
    trace_to_braintrust: config.enabled,
    route: persistentRoute,
  });
  config.enabled = traceSettings.trace_to_braintrust === true;
  config.route = traceSettings.route ?? persistentRoute;

  return config;
}
