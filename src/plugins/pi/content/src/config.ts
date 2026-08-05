import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import * as piCodingAgent from "@earendil-works/pi-coding-agent";
import * as v from "valibot";
import type { ConfigIssue, JsonObject, Logger, LogLevel, TraceConfig } from "./types.ts";
import { ensureDir, writeJsonLog } from "./utils.ts";

const PROJECT_CONFIG_DIR_NAME =
  typeof (piCodingAgent as { CONFIG_DIR_NAME?: unknown }).CONFIG_DIR_NAME === "string"
    ? (piCodingAgent as { CONFIG_DIR_NAME: string }).CONFIG_DIR_NAME
    : ".pi";

const DEFAULT_STATE_DIR = join(homedir(), ".pi", "agent", "state", "braintrust-pi-extension");

const HTTP_URL_SCHEMA = v.pipe(
  v.string(),
  v.url(),
  v.check((value) => {
    try {
      const protocol = new URL(value).protocol;
      return protocol === "http:" || protocol === "https:";
    } catch {
      return false;
    }
  }, "must use http:// or https://"),
);
const JSON_OBJECT_SCHEMA = v.custom<JsonObject>(
  (value) => Boolean(value) && typeof value === "object" && !Array.isArray(value),
  "must be a JSON object",
);
const NON_EMPTY_STRING_SCHEMA = v.pipe(v.string(), v.minLength(1));
const STRING_SCHEMA = v.string();

interface ConfigFileResult {
  value?: JsonObject;
  error?: string;
}

interface ApplyConfigResult {
  parentSpanConfigured: boolean;
  rootSpanConfigured: boolean;
}

function readConfigFile(path: string): ConfigFileResult {
  if (!existsSync(path)) return {};

  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {
        error: "expected a JSON object",
      };
    }
    return { value: parsed as JsonObject };
  } catch (error) {
    return {
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function pushConfigIssue(
  issues: ConfigIssue[],
  path: string,
  message: string,
  severity: ConfigIssue["severity"] = "error",
): void {
  issues.push({ path, message, severity });
}

function validateOptionalString(
  value: unknown,
  issues: ConfigIssue[],
  path: string,
  key: string,
): string | undefined {
  if (value === undefined) return undefined;
  const parsed = v.safeParse(STRING_SCHEMA, value);
  if (!parsed.success) {
    pushConfigIssue(issues, path, `${key} must be a string`);
    return undefined;
  }
  return parsed.output;
}

function validateOptionalNonEmptyString(
  value: unknown,
  issues: ConfigIssue[],
  path: string,
  key: string,
): string | undefined {
  if (value === undefined) return undefined;
  const parsed = v.safeParse(NON_EMPTY_STRING_SCHEMA, value);
  if (!parsed.success) {
    pushConfigIssue(issues, path, `${key} must be a non-empty string`);
    return undefined;
  }
  return parsed.output;
}

function validateOptionalBoolean(
  value: unknown,
  issues: ConfigIssue[],
  path: string,
  key: string,
): boolean | undefined {
  if (value === undefined) return undefined;
  if (typeof value === "boolean") return value;

  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint") {
    const normalized = String(value).trim().toLowerCase();
    if (["true", "1", "yes", "y", "on"].includes(normalized)) return true;
    if (["false", "0", "no", "n", "off"].includes(normalized)) return false;
  }

  pushConfigIssue(issues, path, `${key} must be a boolean`);
  return undefined;
}

function validateOptionalUrl(
  value: unknown,
  issues: ConfigIssue[],
  path: string,
  key: string,
): string | undefined {
  if (value === undefined) return undefined;

  const stringValue = validateOptionalString(value, issues, path, key);
  if (stringValue === undefined) return undefined;

  const parsed = v.safeParse(HTTP_URL_SCHEMA, stringValue);
  if (!parsed.success) {
    pushConfigIssue(issues, path, `${key} must be a valid http(s) URL`);
    return undefined;
  }

  return parsed.output;
}

function validateOptionalMetadata(
  value: unknown,
  issues: ConfigIssue[],
  path: string,
  key: string,
): JsonObject | undefined {
  if (value === undefined) return undefined;

  const parsed = v.safeParse(JSON_OBJECT_SCHEMA, value);
  if (!parsed.success) {
    pushConfigIssue(issues, path, `${key} must be a JSON object`);
    return undefined;
  }

  return parsed.output as JsonObject;
}

function applyConfig(
  target: TraceConfig,
  source: JsonObject | undefined,
  path: string,
  issues: ConfigIssue[],
): ApplyConfigResult {
  if (!source) {
    return {
      parentSpanConfigured: false,
      rootSpanConfigured: false,
    };
  }

  const apiKey = validateOptionalString(source.api_key, issues, path, "api_key");
  if (apiKey !== undefined) target.apiKey = apiKey;

  const apiUrl = validateOptionalUrl(source.api_url, issues, path, "api_url");
  if (apiUrl !== undefined) target.apiUrl = apiUrl;

  const appUrl = validateOptionalUrl(source.app_url, issues, path, "app_url");
  if (appUrl !== undefined) target.appUrl = appUrl;

  const orgName = validateOptionalNonEmptyString(source.org_name, issues, path, "org_name");
  if (orgName !== undefined) target.orgName = orgName;

  const projectName = validateOptionalNonEmptyString(source.project, issues, path, "project");
  if (projectName !== undefined) target.projectName = projectName;

  const enabled = validateOptionalBoolean(
    source.trace_to_braintrust,
    issues,
    path,
    "trace_to_braintrust",
  );
  if (enabled !== undefined) target.enabled = enabled;

  const debug = validateOptionalBoolean(source.debug, issues, path, "debug");
  if (debug !== undefined) target.debug = debug;

  const logFile = validateOptionalString(source.log_file, issues, path, "log_file");
  if (logFile !== undefined) target.logFile = logFile;

  const stateDir = validateOptionalNonEmptyString(source.state_dir, issues, path, "state_dir");
  if (stateDir !== undefined) target.stateDir = stateDir;

  const parentSpanId = validateOptionalNonEmptyString(
    source.parent_span_id,
    issues,
    path,
    "parent_span_id",
  );
  if (parentSpanId !== undefined) target.parentSpanId = parentSpanId;

  const rootSpanId = validateOptionalNonEmptyString(
    source.root_span_id,
    issues,
    path,
    "root_span_id",
  );
  if (rootSpanId !== undefined) target.rootSpanId = rootSpanId;

  const additionalMetadata = validateOptionalMetadata(
    source.additional_metadata,
    issues,
    path,
    "additional_metadata",
  );
  if (additionalMetadata !== undefined) target.additionalMetadata = additionalMetadata;

  const showUi = validateOptionalBoolean(source.show_ui, issues, path, "show_ui");
  if (showUi !== undefined) target.showUi = showUi;

  const showTraceLink = validateOptionalBoolean(
    source.show_trace_link,
    issues,
    path,
    "show_trace_link",
  );
  if (showTraceLink !== undefined) target.showTraceLink = showTraceLink;

  return {
    parentSpanConfigured: parentSpanId !== undefined,
    rootSpanConfigured: rootSpanId !== undefined,
  };
}

export function loadConfig(cwd = process.cwd()): TraceConfig {
  const config: TraceConfig = {
    enabled: false,
    apiKey: "",
    apiUrl: undefined,
    appUrl: "https://www.braintrust.dev",
    orgName: undefined,
    projectName: "pi",
    debug: false,
    logFile: undefined,
    stateDir: DEFAULT_STATE_DIR,
    additionalMetadata: {},
    parentSpanId: undefined,
    rootSpanId: undefined,
    showUi: true,
    showTraceLink: true,
    configIssues: [],
  };

  const globalConfigPath = join(homedir(), ".pi", "agent", "braintrust.json");
  const projectConfigPath = join(cwd, PROJECT_CONFIG_DIR_NAME, "braintrust.json");

  let parentSpanConfigured = false;
  let rootSpanConfigured = false;

  const globalConfig = readConfigFile(globalConfigPath);
  if (globalConfig.error) {
    pushConfigIssue(config.configIssues, globalConfigPath, globalConfig.error);
  }
  {
    const applied = applyConfig(config, globalConfig.value, globalConfigPath, config.configIssues);
    parentSpanConfigured ||= applied.parentSpanConfigured;
    rootSpanConfigured ||= applied.rootSpanConfigured;
  }

  const projectConfig = readConfigFile(projectConfigPath);
  if (projectConfig.error) {
    pushConfigIssue(config.configIssues, projectConfigPath, projectConfig.error);
  }
  {
    const applied = applyConfig(
      config,
      projectConfig.value,
      projectConfigPath,
      config.configIssues,
    );
    parentSpanConfigured ||= applied.parentSpanConfigured;
    rootSpanConfigured ||= applied.rootSpanConfigured;
  }

  const envApiKey = validateOptionalString(
    process.env.BRAINTRUST_API_KEY,
    config.configIssues,
    "BRAINTRUST_API_KEY",
    "BRAINTRUST_API_KEY",
  );
  if (envApiKey !== undefined) config.apiKey = envApiKey;

  const envApiUrl = validateOptionalUrl(
    process.env.BRAINTRUST_API_URL,
    config.configIssues,
    "BRAINTRUST_API_URL",
    "BRAINTRUST_API_URL",
  );
  if (envApiUrl !== undefined) config.apiUrl = envApiUrl;

  const envAppUrl = validateOptionalUrl(
    process.env.BRAINTRUST_APP_URL,
    config.configIssues,
    "BRAINTRUST_APP_URL",
    "BRAINTRUST_APP_URL",
  );
  if (envAppUrl !== undefined) config.appUrl = envAppUrl;

  const envOrgName = validateOptionalNonEmptyString(
    process.env.BRAINTRUST_ORG_NAME,
    config.configIssues,
    "BRAINTRUST_ORG_NAME",
    "BRAINTRUST_ORG_NAME",
  );
  if (envOrgName !== undefined) config.orgName = envOrgName;

  const envProjectName = validateOptionalNonEmptyString(
    process.env.BRAINTRUST_PROJECT,
    config.configIssues,
    "BRAINTRUST_PROJECT",
    "BRAINTRUST_PROJECT",
  );
  if (envProjectName !== undefined) config.projectName = envProjectName;

  const envEnabled = validateOptionalBoolean(
    process.env.TRACE_TO_BRAINTRUST,
    config.configIssues,
    "TRACE_TO_BRAINTRUST",
    "TRACE_TO_BRAINTRUST",
  );
  if (envEnabled !== undefined) config.enabled = envEnabled;

  const envDebug = validateOptionalBoolean(
    process.env.BRAINTRUST_DEBUG,
    config.configIssues,
    "BRAINTRUST_DEBUG",
    "BRAINTRUST_DEBUG",
  );
  if (envDebug !== undefined) config.debug = envDebug;

  const envLogFile = validateOptionalString(
    process.env.BRAINTRUST_LOG_FILE,
    config.configIssues,
    "BRAINTRUST_LOG_FILE",
    "BRAINTRUST_LOG_FILE",
  );
  if (envLogFile !== undefined) config.logFile = envLogFile;

  const envStateDir = validateOptionalNonEmptyString(
    process.env.BRAINTRUST_STATE_DIR,
    config.configIssues,
    "BRAINTRUST_STATE_DIR",
    "BRAINTRUST_STATE_DIR",
  );
  if (envStateDir !== undefined) config.stateDir = envStateDir;

  const envShowUi = validateOptionalBoolean(
    process.env.BRAINTRUST_SHOW_UI,
    config.configIssues,
    "BRAINTRUST_SHOW_UI",
    "BRAINTRUST_SHOW_UI",
  );
  if (envShowUi !== undefined) config.showUi = envShowUi;

  const envShowTraceLink = validateOptionalBoolean(
    process.env.BRAINTRUST_SHOW_TRACE_LINK,
    config.configIssues,
    "BRAINTRUST_SHOW_TRACE_LINK",
    "BRAINTRUST_SHOW_TRACE_LINK",
  );
  if (envShowTraceLink !== undefined) config.showTraceLink = envShowTraceLink;

  const envParentSpanId = validateOptionalNonEmptyString(
    process.env.PI_PARENT_SPAN_ID,
    config.configIssues,
    "PI_PARENT_SPAN_ID",
    "PI_PARENT_SPAN_ID",
  );
  if (envParentSpanId !== undefined) {
    config.parentSpanId = envParentSpanId;
    parentSpanConfigured = true;
  }

  const envRootSpanId = validateOptionalNonEmptyString(
    process.env.PI_ROOT_SPAN_ID,
    config.configIssues,
    "PI_ROOT_SPAN_ID",
    "PI_ROOT_SPAN_ID",
  );
  if (envRootSpanId !== undefined) {
    config.rootSpanId = envRootSpanId;
    rootSpanConfigured = true;
  }

  if (process.env.BRAINTRUST_ADDITIONAL_METADATA !== undefined) {
    try {
      const parsed = JSON.parse(process.env.BRAINTRUST_ADDITIONAL_METADATA) as unknown;
      const metadata = validateOptionalMetadata(
        parsed,
        config.configIssues,
        "BRAINTRUST_ADDITIONAL_METADATA",
        "BRAINTRUST_ADDITIONAL_METADATA",
      );
      if (metadata !== undefined) config.additionalMetadata = metadata;
    } catch (error) {
      pushConfigIssue(
        config.configIssues,
        "BRAINTRUST_ADDITIONAL_METADATA",
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  if (config.parentSpanId && !config.rootSpanId) config.rootSpanId = config.parentSpanId;
  if (config.rootSpanId && !config.parentSpanId) config.parentSpanId = config.rootSpanId;

  if (
    parentSpanConfigured &&
    rootSpanConfigured &&
    config.parentSpanId &&
    config.rootSpanId &&
    config.parentSpanId === config.rootSpanId
  ) {
    pushConfigIssue(
      config.configIssues,
      "parent_span_id/root_span_id",
      "parent_span_id and root_span_id are identical; set only one unless the parent span is also the trace root",
      "warning",
    );
  }

  if (config.enabled && !config.apiKey) {
    pushConfigIssue(
      config.configIssues,
      "BRAINTRUST_API_KEY",
      "TRACE_TO_BRAINTRUST is enabled but BRAINTRUST_API_KEY is not set",
      "warning",
    );
  }

  ensureDir(config.stateDir);
  return config;
}

export function createLogger(config: TraceConfig): Logger {
  const explicitLogFile =
    config.logFile && config.logFile !== "true" && config.logFile !== "auto"
      ? config.logFile
      : join(config.stateDir, "braintrust-pi-extension.log");
  const infoLoggingEnabled = config.debug || Boolean(config.logFile);

  let logDirEnsured = false;

  function shouldLog(level: LogLevel): boolean {
    return level === "warn" || level === "error" || infoLoggingEnabled;
  }

  function ensureLogDir(): void {
    if (logDirEnsured) return;
    ensureDir(dirname(explicitLogFile));
    logDirEnsured = true;
  }

  let pendingWrite = Promise.resolve();

  function emit(level: LogLevel, message: string, data?: unknown): void {
    if (!shouldLog(level)) return;
    ensureLogDir();
    pendingWrite = pendingWrite
      .catch(() => {})
      .then(async () => {
        try {
          await writeJsonLog(explicitLogFile, level, message, data);
        } catch {
          // Logging is best-effort and must never affect pi session execution.
        }
      });
  }

  return {
    filePath: explicitLogFile,
    debug(message, data) {
      if (config.debug) emit("debug", message, data);
    },
    info(message, data) {
      emit("info", message, data);
    },
    warn(message, data) {
      emit("warn", message, data);
    },
    error(message, data) {
      emit("error", message, data);
    },
    async flush() {
      await pendingWrite.catch(() => {});
    },
  };
}
