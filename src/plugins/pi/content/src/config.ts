import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import * as piCodingAgent from "@earendil-works/pi-coding-agent";

export interface PiConfig {
  enabled: boolean;
  profile?: string;
  orgName?: string;
  projectName: string;
  additionalMetadata?: Record<string, unknown>;
  showUi: boolean;
  showTraceLink: boolean;
}

type ConfigRecord = Record<string, unknown>;

const PROJECT_CONFIG_DIR_NAME =
  typeof (piCodingAgent as { CONFIG_DIR_NAME?: unknown }).CONFIG_DIR_NAME === "string"
    ? (piCodingAgent as { CONFIG_DIR_NAME: string }).CONFIG_DIR_NAME
    : ".pi";

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
  config.profile = nonEmptyString(source.profile) ?? config.profile;
  config.orgName = nonEmptyString(source.org_name) ?? config.orgName;
  config.projectName = nonEmptyString(source.project) ?? config.projectName;
  config.enabled = boolean(source.trace_to_braintrust) ?? config.enabled;
  config.additionalMetadata = record(source.additional_metadata) ?? config.additionalMetadata;
  config.showUi = boolean(source.show_ui) ?? config.showUi;
  config.showTraceLink = boolean(source.show_trace_link) ?? config.showTraceLink;
}

function environmentMetadata(value: string | undefined): ConfigRecord | undefined {
  if (!value) return undefined;
  try {
    return record(JSON.parse(value));
  } catch {
    return undefined;
  }
}

export function loadConfig(cwd = process.cwd()): PiConfig {
  const config: PiConfig = {
    enabled: false,
    projectName: "pi",
    showUi: true,
    showTraceLink: true,
  };

  applyConfig(config, readConfig(join(homedir(), ".pi", "agent", "braintrust.json")));
  applyConfig(config, readConfig(join(cwd, PROJECT_CONFIG_DIR_NAME, "braintrust.json")));

  config.profile = nonEmptyString(process.env.BRAINTRUST_PROFILE) ?? config.profile;
  config.orgName = nonEmptyString(process.env.BRAINTRUST_ORG_NAME) ?? config.orgName;
  config.projectName = nonEmptyString(process.env.BRAINTRUST_PROJECT) ?? config.projectName;
  config.enabled = boolean(process.env.TRACE_TO_BRAINTRUST) ?? config.enabled;
  config.additionalMetadata =
    environmentMetadata(process.env.BRAINTRUST_ADDITIONAL_METADATA) ?? config.additionalMetadata;
  config.showUi = boolean(process.env.BRAINTRUST_SHOW_UI) ?? config.showUi;
  config.showTraceLink = boolean(process.env.BRAINTRUST_SHOW_TRACE_LINK) ?? config.showTraceLink;

  return config;
}
