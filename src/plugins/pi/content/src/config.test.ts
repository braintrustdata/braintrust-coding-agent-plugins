import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadConfig } from "./config.ts";

const ENVIRONMENT_KEYS = [
  "BRAINTRUST_PROFILE",
  "BRAINTRUST_ORG_NAME",
  "BRAINTRUST_PROJECT",
  "BRAINTRUST_ADDITIONAL_METADATA",
  "BRAINTRUST_SHOW_UI",
  "BRAINTRUST_SHOW_TRACE_LINK",
  "TRACE_TO_BRAINTRUST",
] as const;

const originalEnvironment = new Map<string, string | undefined>();
const originalHome = process.env.HOME;
let home: string;
let cwd: string;

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value));
}

beforeEach(() => {
  home = mkdtempSync(join(tmpdir(), "pi-config-home-"));
  cwd = mkdtempSync(join(tmpdir(), "pi-config-project-"));
  process.env.HOME = home;
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
  if (originalHome === undefined) delete process.env.HOME;
  else process.env.HOME = originalHome;
});

describe("loadConfig", () => {
  it("defaults to disabled tracing with the daemon default profile", () => {
    expect(loadConfig(cwd)).toEqual({
      enabled: false,
      projectName: "pi",
      showUi: true,
      showTraceLink: true,
    });
  });

  it("layers global, project, and invocation selection", () => {
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
    process.env.BRAINTRUST_PROFILE = "run";
    process.env.BRAINTRUST_ORG_NAME = "run-org";
    process.env.BRAINTRUST_PROJECT = "run-project";
    process.env.BRAINTRUST_ADDITIONAL_METADATA = '{"source":"run"}';

    expect(loadConfig(cwd)).toEqual({
      enabled: true,
      profile: "run",
      orgName: "run-org",
      projectName: "run-project",
      additionalMetadata: { source: "run" },
      showUi: true,
      showTraceLink: false,
    });
  });

  it("accepts invocation-local enablement and UI overrides", () => {
    process.env.TRACE_TO_BRAINTRUST = "true";
    process.env.BRAINTRUST_SHOW_UI = "0";
    process.env.BRAINTRUST_SHOW_TRACE_LINK = "no";

    expect(loadConfig(cwd)).toMatchObject({
      enabled: true,
      showUi: false,
      showTraceLink: false,
    });
  });

  it("ignores malformed files, values, and metadata", () => {
    writeJson(join(home, ".pi", "agent", "braintrust.json"), {
      trace_to_braintrust: "sometimes",
      project: [],
      additional_metadata: [],
    });
    writeFileSync(join(cwd, ".pi-invalid"), "ignored");
    process.env.BRAINTRUST_ADDITIONAL_METADATA = "not-json";

    expect(loadConfig(cwd)).toEqual({
      enabled: false,
      projectName: "pi",
      showUi: true,
      showTraceLink: true,
    });
  });
});
