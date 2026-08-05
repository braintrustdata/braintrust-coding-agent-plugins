import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createLogger, loadConfig } from "./config.ts";
import type { TraceConfig } from "./types.ts";

const ENV_KEYS = [
  "HOME",
  "BRAINTRUST_API_KEY",
  "BRAINTRUST_API_URL",
  "BRAINTRUST_APP_URL",
  "BRAINTRUST_ORG_NAME",
  "BRAINTRUST_PROJECT",
  "TRACE_TO_BRAINTRUST",
  "BRAINTRUST_DEBUG",
  "BRAINTRUST_LOG_FILE",
  "BRAINTRUST_STATE_DIR",
  "BRAINTRUST_SHOW_UI",
  "BRAINTRUST_SHOW_TRACE_LINK",
  "PI_PARENT_SPAN_ID",
  "PI_ROOT_SPAN_ID",
  "BRAINTRUST_ADDITIONAL_METADATA",
] as const;

const originalEnv = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
const tempDirs: string[] = [];

beforeEach(() => {
  for (const key of ENV_KEYS) {
    delete process.env[key];
  }
});

afterEach(() => {
  for (const key of ENV_KEYS) {
    const value = originalEnv[key];
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }

  while (tempDirs.length > 0) {
    rmSync(tempDirs.pop()!, { recursive: true, force: true });
  }
});

function makeTempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

describe("loadConfig", () => {
  it("applies global config, project config, then env overrides", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");
    const projectStateDir = join(home, "project-state");
    const envStateDir = join(home, "env-state");

    process.env.HOME = home;
    process.env.BRAINTRUST_PROJECT = "from-env";
    process.env.TRACE_TO_BRAINTRUST = "false";
    process.env.BRAINTRUST_ADDITIONAL_METADATA = '{"origin":"env","team":"platform"}';
    process.env.BRAINTRUST_STATE_DIR = envStateDir;

    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      api_key: "global-key",
      api_url: "https://global.example",
      project: "from-global",
      trace_to_braintrust: false,
      debug: true,
      additional_metadata: { origin: "global" },
    });

    writeJson(join(cwd, ".pi", "braintrust.json"), {
      api_url: "https://project.example",
      project: "from-project",
      trace_to_braintrust: true,
      state_dir: projectStateDir,
      additional_metadata: { origin: "project" },
    });

    const config = loadConfig(cwd);

    expect(config.apiKey).toBe("global-key");
    expect(config.apiUrl).toBe("https://project.example");
    expect(config.projectName).toBe("from-env");
    expect(config.enabled).toBe(false);
    expect(config.additionalMetadata).toEqual({ origin: "env", team: "platform" });
    expect(config.stateDir).toBe(envStateDir);
    expect(config.configIssues).toEqual([]);
    expect(existsSync(envStateDir)).toBe(true);
  });

  it("uses pi CONFIG_DIR_NAME for project-local config", async () => {
    vi.resetModules();
    vi.doMock("@earendil-works/pi-coding-agent", () => ({
      CONFIG_DIR_NAME: ".custom-pi",
    }));
    try {
      const { loadConfig: loadConfigWithCustomDir } = await import("./config.ts");

      const home = makeTempDir("pi-extension-home-");
      const cwd = join(home, "workspace");

      process.env.HOME = home;
      process.env.BRAINTRUST_STATE_DIR = join(home, "state");

      writeJson(join(cwd, ".custom-pi", "braintrust.json"), {
        project: "from-custom-config-dir",
      });

      const config = loadConfigWithCustomDir(cwd);

      expect(config.projectName).toBe("from-custom-config-dir");
    } finally {
      vi.doUnmock("@earendil-works/pi-coding-agent");
      vi.resetModules();
    }
  });

  it("falls back to .pi when older pi hosts do not export CONFIG_DIR_NAME", async () => {
    vi.resetModules();
    vi.doMock("@earendil-works/pi-coding-agent", () => ({
      CONFIG_DIR_NAME: undefined,
    }));
    try {
      const { loadConfig: loadConfigWithoutConfigDir } = await import("./config.ts");

      const home = makeTempDir("pi-extension-home-");
      const cwd = join(home, "workspace");

      process.env.HOME = home;
      process.env.BRAINTRUST_STATE_DIR = join(home, "state");

      writeJson(join(cwd, ".pi", "braintrust.json"), {
        project: "from-default-config-dir",
      });

      const config = loadConfigWithoutConfigDir(cwd);

      expect(config.projectName).toBe("from-default-config-dir");
    } finally {
      vi.doUnmock("@earendil-works/pi-coding-agent");
      vi.resetModules();
    }
  });

  it("records config file parse errors without throwing away other valid config sources", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");

    process.env.HOME = home;

    mkdirSync(dirname(join(home, ".pi", "agent", "braintrust.json")), { recursive: true });
    writeFileSync(
      join(home, ".pi", "agent", "braintrust.json"),
      '{\n  "trace_to_braintrust": true,\n  "api_key": "bad-json",\n}\n',
      "utf8",
    );

    writeJson(join(cwd, ".pi", "braintrust.json"), {
      trace_to_braintrust: true,
      project: "from-project",
    });

    const config = loadConfig(cwd);

    expect(config.enabled).toBe(true);
    expect(config.projectName).toBe("from-project");
    expect(config.configIssues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          path: join(home, ".pi", "agent", "braintrust.json"),
          severity: "error",
          message: expect.stringContaining("JSON"),
        }),
        expect.objectContaining({
          path: "BRAINTRUST_API_KEY",
          severity: "warning",
        }),
      ]),
    );
  });

  it("ignores malformed config value types without crashing", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");

    process.env.HOME = home;
    process.env.BRAINTRUST_ADDITIONAL_METADATA = "not-json";

    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      api_key: { nested: true },
      project: ["wrong-type"],
      trace_to_braintrust: "definitely",
      debug: { nope: true },
      state_dir: { bad: true },
      additional_metadata: ["bad"],
      parent_span_id: { bad: true },
      root_span_id: ["bad"],
    });

    const config = loadConfig(cwd);

    expect(config.apiKey).toBe("");
    expect(config.projectName).toBe("pi");
    expect(config.enabled).toBe(false);
    expect(config.debug).toBe(false);
    expect(config.additionalMetadata).toEqual({});
    expect(config.parentSpanId).toBeUndefined();
    expect(config.rootSpanId).toBeUndefined();
    expect(config.configIssues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          path: join(home, ".pi", "agent", "braintrust.json"),
          message: expect.stringContaining("additional_metadata"),
          severity: "error",
        }),
        expect.objectContaining({
          path: "BRAINTRUST_ADDITIONAL_METADATA",
          severity: "error",
        }),
      ]),
    );
    expect(config.stateDir.endsWith(join(".pi", "agent", "state", "braintrust-pi-extension"))).toBe(
      true,
    );
    expect(existsSync(config.stateDir)).toBe(true);
  });

  it("mirrors the parent span id to the root span id when only parent is provided", () => {
    const home = makeTempDir("pi-extension-home-");
    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");
    process.env.PI_PARENT_SPAN_ID = "parent-123";

    const config = loadConfig(home);

    expect(config.parentSpanId).toBe("parent-123");
    expect(config.rootSpanId).toBe("parent-123");
  });

  it("mirrors the root span id to the parent span id when only root is provided", () => {
    const home = makeTempDir("pi-extension-home-");
    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");
    process.env.PI_ROOT_SPAN_ID = "root-123";

    const config = loadConfig(home);

    expect(config.rootSpanId).toBe("root-123");
    expect(config.parentSpanId).toBe("root-123");
  });

  it("keeps lower-precedence URLs when higher-precedence URL values are invalid", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");

    process.env.HOME = home;
    process.env.BRAINTRUST_APP_URL = "ftp://braintrust.example";

    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      api_url: "https://global.example",
    });

    writeJson(join(cwd, ".pi", "braintrust.json"), {
      api_url: "not-a-url",
    });

    const config = loadConfig(cwd);

    expect(config.apiUrl).toBe("https://global.example");
    expect(config.appUrl).toBe("https://www.braintrust.dev");
    expect(config.configIssues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          path: join(cwd, ".pi", "braintrust.json"),
          message: "api_url must be a valid http(s) URL",
          severity: "error",
        }),
        expect.objectContaining({
          path: "BRAINTRUST_APP_URL",
          message: "BRAINTRUST_APP_URL must be a valid http(s) URL",
          severity: "error",
        }),
      ]),
    );
  });

  it("warns when parent_span_id and root_span_id are both explicitly set to the same value", () => {
    const home = makeTempDir("pi-extension-home-");
    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");
    process.env.PI_PARENT_SPAN_ID = "span-123";
    process.env.PI_ROOT_SPAN_ID = "span-123";

    const config = loadConfig(home);

    expect(config.configIssues).toContainEqual({
      path: "parent_span_id/root_span_id",
      message:
        "parent_span_id and root_span_id are identical; set only one unless the parent span is also the trace root",
      severity: "warning",
    });
  });

  it("defaults showUi and showTraceLink to true", () => {
    const home = makeTempDir("pi-extension-home-");
    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");

    const config = loadConfig(home);

    expect(config.showUi).toBe(true);
    expect(config.showTraceLink).toBe(true);
  });

  it("applies show_ui and show_trace_link from config files", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");

    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");

    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      show_ui: false,
    });

    const config = loadConfig(cwd);

    expect(config.showUi).toBe(false);
    expect(config.showTraceLink).toBe(true);
  });

  it("overrides show_ui and show_trace_link from environment variables", () => {
    const home = makeTempDir("pi-extension-home-");
    const cwd = join(home, "workspace");

    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");
    process.env.BRAINTRUST_SHOW_UI = "true";
    process.env.BRAINTRUST_SHOW_TRACE_LINK = "false";

    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      show_ui: false,
    });

    const config = loadConfig(cwd);

    expect(config.showUi).toBe(true);
    expect(config.showTraceLink).toBe(false);
  });

  it("warns when tracing is enabled without an API key", () => {
    const home = makeTempDir("pi-extension-home-");
    process.env.HOME = home;
    process.env.BRAINTRUST_STATE_DIR = join(home, "state");
    process.env.TRACE_TO_BRAINTRUST = "true";

    const config = loadConfig(home);

    expect(config.enabled).toBe(true);
    expect(config.configIssues).toContainEqual({
      path: "BRAINTRUST_API_KEY",
      message: "TRACE_TO_BRAINTRUST is enabled but BRAINTRUST_API_KEY is not set",
      severity: "warning",
    });
  });
});

describe("createLogger", () => {
  function makeLoggerConfig(overrides: Partial<TraceConfig> = {}): TraceConfig {
    return {
      enabled: true,
      apiKey: "key",
      apiUrl: undefined,
      appUrl: "https://www.braintrust.dev",
      orgName: undefined,
      projectName: "pi",
      debug: true,
      logFile: undefined,
      stateDir: makeTempDir("pi-extension-state-"),
      additionalMetadata: {},
      parentSpanId: undefined,
      rootSpanId: undefined,
      showUi: true,
      showTraceLink: true,
      configIssues: [],
      ...overrides,
    };
  }

  it("writes json log lines to the default log file when debug is enabled", async () => {
    const config = makeLoggerConfig();

    const logger = createLogger(config);
    logger.debug("debug message", { nested: { value: 1 } });
    logger.warn("warn message");
    await logger.flush();

    const lines = readFileSync(logger.filePath, "utf8").trim().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({
      level: "debug",
      message: "debug message",
      data: { nested: { value: 1 } },
    });
    expect(JSON.parse(lines[1])).toMatchObject({
      level: "warn",
      message: "warn message",
    });
  });

  it("writes warnings and errors to the default log file when debug is disabled", async () => {
    const config = makeLoggerConfig({ debug: false, logFile: undefined });
    const logger = createLogger(config);

    logger.debug("debug message");
    logger.info("info message");
    logger.warn("warn message", { reason: "test" });
    logger.error("error message");
    await logger.flush();

    const lines = readFileSync(logger.filePath, "utf8").trim().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({
      level: "warn",
      message: "warn message",
      data: { reason: "test" },
    });
    expect(JSON.parse(lines[1])).toMatchObject({
      level: "error",
      message: "error message",
    });
  });
});
