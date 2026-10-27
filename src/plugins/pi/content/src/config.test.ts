import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "./config.ts";

const ENVIRONMENT_KEYS = [
  "BRAINTRUST_PROFILE",
  "BRAINTRUST_ORG_NAME",
  "BRAINTRUST_PROJECT",
  "BRAINTRUST_ADDITIONAL_METADATA",
  "BRAINTRUST_SHOW_UI",
  "BRAINTRUST_SHOW_TRACE_LINK",
  "TRACE_TO_BRAINTRUST",
  "BT_TRACE_INVOCATION_SETTINGS",
  "PI_CODING_AGENT_DIR",
] as const;

const originalEnvironment = new Map<string, string | undefined>();
// os.homedir() reads HOME on POSIX and USERPROFILE on Windows.
const HOME_KEYS = ["HOME", "USERPROFILE"] as const;
const originalHome = new Map<string, string | undefined>();
let home: string;
let cwd: string;

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value));
}

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), "pi-config-home-"));
  cwd = mkdtempSync(join(tmpdir(), "pi-config-project-"));
  for (const key of HOME_KEYS) {
    originalHome.set(key, process.env[key]);
    process.env[key] = home;
  }
  for (const key of ENVIRONMENT_KEYS) {
    originalEnvironment.set(key, process.env[key]);
    delete process.env[key];
  }
});

afterEach(() => {
  rmSync(home, { recursive: true, force: true });
  rmSync(cwd, { recursive: true, force: true });
  for (const key of ENVIRONMENT_KEYS) {
    const value = originalEnvironment.get(key);
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  for (const key of HOME_KEYS) {
    const value = originalHome.get(key);
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

const DEFAULT_CONFIG = {
  enabled: false,
  projectName: "pi",
  showUi: true,
  showTraceLink: true,
  profile: undefined,
  orgName: undefined,
  additionalMetadata: undefined,
  route: {
    destination: { type: "project_logs", project_name: "pi" },
    flush_mode: "flush_on_turn_end",
  },
};

describe("loadConfig", () => {
  it("defaults to disabled tracing with the daemon default profile", () => {
    expect(loadConfig(cwd)).toEqual(DEFAULT_CONFIG);
  });

  it("layers global config with project config overriding it", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: true,
      profile: "global",
      org_name: "global-org",
      project: "global-project",
      additional_metadata: { source: "global" },
    });
    writeJson(join(cwd, ".pi", "braintrust.json"), {
      profile: "project",
      project: "project-project",
      show_trace_link: false,
    });

    expect(loadConfig(cwd)).toEqual({
      enabled: true,
      profile: "project",
      orgName: "global-org",
      projectName: "project-project",
      additionalMetadata: { source: "global" },
      showUi: true,
      showTraceLink: false,
      route: {
        auth: { profile: "project", org_name: "global-org" },
        destination: { type: "project_logs", project_name: "project-project" },
        flush_mode: "flush_on_turn_end",
        additional_metadata: { source: "global" },
      },
    });
  });

  it("honors the host's explicit agent directory without changing project precedence", () => {
    process.env.PI_CODING_AGENT_DIR = join(home, ".omp", "profiles", "work", "agent");
    writeJson(join(process.env.PI_CODING_AGENT_DIR, "braintrust.json"), {
      trace_to_braintrust: true,
      profile: "host-profile",
      project: "profile-project",
    });
    expect(loadConfig(cwd)).toMatchObject({
      enabled: true,
      profile: "host-profile",
      projectName: "profile-project",
    });
    writeJson(join(cwd, ".pi", "braintrust.json"), { project: "project-override" });
    expect(loadConfig(cwd)).toMatchObject({
      profile: "host-profile",
      projectName: "project-override",
    });
  });

  it("uses the running host's config directory for global and project settings", async () => {
    vi.doMock("./pi-package.ts", () => ({
      runningPiPackage: () => ({ configDir: ".omp", version: "18.12.6" }),
    }));
    vi.resetModules();
    try {
      // Reload the module boundary so its cached config directory sees this host.
      const { loadConfig: loadHostConfig } = await import("./config.ts");
      // `bt trace enable pi` writes Pi's global file; hosts still honor it.
      writeJson(join(home, ".pi", "agent", "braintrust.json"), {
        trace_to_braintrust: true,
        project: "bt-written",
      });
      expect(loadHostConfig(cwd)).toMatchObject({ enabled: true, projectName: "bt-written" });
      writeJson(join(home, ".omp", "agent", "braintrust.json"), {
        profile: "host-profile",
        project: "host-project",
      });
      expect(loadHostConfig(cwd)).toMatchObject({
        enabled: true,
        profile: "host-profile",
        projectName: "host-project",
      });
      writeJson(join(cwd, ".omp", "braintrust.json"), { project: "project-override" });
      expect(loadHostConfig(cwd)).toMatchObject({
        profile: "host-profile",
        projectName: "project-override",
      });
    } finally {
      vi.doUnmock("./pi-package.ts");
      vi.resetModules();
    }
  });

  it("prefers a canonical route over stale top-level routing", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: true,
      profile: "stale-profile",
      org_name: "stale-org",
      project: "stale-project",
      route: {
        auth: { profile: "route-profile", org_name: "route-org" },
        destination: { type: "project_logs", project_name: "route-project" },
      },
    });

    expect(loadConfig(cwd)).toMatchObject({
      profile: "route-profile",
      orgName: "route-org",
      projectName: "route-project",
      route: {
        auth: { profile: "route-profile", org_name: "route-org" },
        destination: { type: "project_logs", project_name: "route-project" },
      },
    });
  });

  it("lets environment variables control only UI behavior, not routing or enablement", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: false,
      project: "global-project",
    });
    process.env.BRAINTRUST_PROFILE = "run";
    process.env.BRAINTRUST_ORG_NAME = "run-org";
    process.env.BRAINTRUST_PROJECT = "run-project";
    process.env.BRAINTRUST_ADDITIONAL_METADATA = '{"source":"run"}';
    process.env.TRACE_TO_BRAINTRUST = "true";
    process.env.BRAINTRUST_SHOW_UI = "0";
    process.env.BRAINTRUST_SHOW_TRACE_LINK = "no";

    expect(loadConfig(cwd)).toMatchObject({
      enabled: false,
      profile: undefined,
      orgName: undefined,
      projectName: "global-project",
      additionalMetadata: undefined,
      showUi: false,
      showTraceLink: false,
    });
  });

  it("ignores malformed files and values", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: "sometimes",
      project: [],
      additional_metadata: [],
    });

    expect(loadConfig(cwd)).toEqual(DEFAULT_CONFIG);
  });

  it("uses managed-run settings without replacing the agent's global file", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: true,
      profile: "global",
      project: "global-project",
    });
    process.env.BT_TRACE_INVOCATION_SETTINGS = JSON.stringify({
      trace_to_braintrust: true,
      route: {
        auth: { profile: "run", org_name: "run-org" },
        destination: { type: "project_logs", project_name: "run-project" },
      },
    });

    expect(loadConfig(cwd)).toMatchObject({
      enabled: true,
      route: {
        auth: { profile: "run", org_name: "run-org" },
        destination: { type: "project_logs", project_name: "run-project" },
      },
    });
    expect(
      JSON.parse(readFileSync(join(home, ".pi", "agent", "braintrust.json"), "utf8")).project,
    ).toBe("global-project");
  });

  it("preserves a structured persistent destination", () => {
    const destination = {
      type: "parent_span",
      components: { object_id: "object", row_id: "row", span_id: "span" },
    };
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: true,
      route: { destination, flush_mode: "flush_on_turn_end" },
    });

    expect(loadConfig(cwd).route).toMatchObject({ destination });
  });

  it("does not fall back to persistent settings for a malformed managed run", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "global" } },
    });
    process.env.BT_TRACE_INVOCATION_SETTINGS = "{";

    expect(loadConfig(cwd).enabled).toBe(false);
  });
});
