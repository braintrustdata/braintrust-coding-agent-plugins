import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadConfig, type PluginConfig, parseBooleanEnv } from "./config";

describe("parseBooleanEnv", () => {
  it("accepts true and 1 case-insensitively", () => {
    expect(parseBooleanEnv("true")).toBe(true);
    expect(parseBooleanEnv("TRUE")).toBe(true);
    expect(parseBooleanEnv("1")).toBe(true);
  });

  it("rejects other and missing values", () => {
    expect(parseBooleanEnv(undefined)).toBe(false);
    expect(parseBooleanEnv("false")).toBe(false);
    expect(parseBooleanEnv("yes")).toBe(false);
  });
});

describe("loadConfig", () => {
  const keys = [
    "TRACE_TO_BRAINTRUST",
    "BRAINTRUST_DEBUG",
    "BRAINTRUST_ORG_NAME",
    "BRAINTRUST_PROFILE",
    "BRAINTRUST_PROJECT",
    "BRAINTRUST_ADDITIONAL_METADATA",
    "BRAINTRUST_OPENCODE_ENABLE_TOOLS",
    "BT_TRACE_INVOCATION_SETTINGS",
  ];
  const original: Record<string, string | undefined> = {};

  beforeEach(() => {
    for (const key of keys) {
      original[key] = process.env[key];
      delete process.env[key];
    }
  });

  afterEach(() => {
    for (const key of keys) {
      if (original[key] === undefined) delete process.env[key];
      else process.env[key] = original[key];
    }
  });

  it("defaults to the bt default profile and the opencode project", () => {
    expect(loadConfig()).toEqual({
      profile: undefined,
      orgName: undefined,
      projectName: "opencode",
      tracingEnabled: false,
      enableTools: true,
      debug: false,
      additionalMetadata: undefined,
      route: {
        auth: {},
        destination: { type: "project_logs", project_name: "opencode" },
        flush_mode: "fire_and_forget",
      },
    });
  });

  it("loads routing and behavior from plugin configuration", () => {
    const pluginConfig: PluginConfig = {
      profile: "work",
      org_name: "acme",
      project: "agents",
      trace_to_braintrust: true,
      enable_tools: false,
      debug: true,
      additional_metadata: { team: "platform" },
    };
    expect(loadConfig(pluginConfig)).toEqual({
      profile: "work",
      orgName: "acme",
      projectName: "agents",
      tracingEnabled: true,
      enableTools: false,
      debug: true,
      additionalMetadata: { team: "platform" },
      route: {
        auth: { profile: "work", org_name: "acme" },
        destination: { type: "project_logs", project_name: "agents" },
        flush_mode: "fire_and_forget",
        additional_metadata: { team: "platform" },
      },
    });
  });

  it("prefers a canonical route over stale top-level routing", () => {
    expect(
      loadConfig({
        profile: "stale-profile",
        org_name: "stale-org",
        project: "stale-project",
        route: {
          auth: { profile: "route-profile", org_name: "route-org" },
          destination: { type: "project_logs", project_name: "route-project" },
        },
      }),
    ).toMatchObject({
      profile: "route-profile",
      orgName: "route-org",
      projectName: "route-project",
      route: {
        auth: { profile: "route-profile", org_name: "route-org" },
        destination: { type: "project_logs", project_name: "route-project" },
      },
    });
  });

  it("ignores routing and enablement environment variables, using only the config file", () => {
    process.env.BRAINTRUST_PROFILE = "personal";
    process.env.BRAINTRUST_ORG_NAME = "braintrust";
    process.env.BRAINTRUST_PROJECT = "opencode-runs";
    process.env.TRACE_TO_BRAINTRUST = "true";
    process.env.BRAINTRUST_ADDITIONAL_METADATA = '{"ci":true}';

    expect(loadConfig({ profile: "work", project: "other" })).toMatchObject({
      profile: "work",
      projectName: "other",
      tracingEnabled: false,
      additionalMetadata: undefined,
    });
  });

  it("still lets environment variables control unrelated plugin behavior", () => {
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "false";
    process.env.BRAINTRUST_DEBUG = "true";

    expect(loadConfig()).toMatchObject({
      enableTools: false,
      debug: true,
    });
  });

  it("preserves a structured persistent destination", () => {
    const destination = {
      type: "parent_span",
      components: { object_id: "object", row_id: "row", span_id: "span" },
    };
    expect(
      loadConfig({
        trace_to_braintrust: true,
        route: { destination, flush_mode: "flush_on_turn_end" },
      }).route,
    ).toEqual({ destination, auth: {}, flush_mode: "flush_on_turn_end" });
  });

  it("does not fall back to persistent settings for a malformed managed run", () => {
    process.env.BT_TRACE_INVOCATION_SETTINGS = "{";
    expect(
      loadConfig({
        trace_to_braintrust: true,
        route: { destination: { type: "project_logs", project_name: "global" } },
      }),
    ).toMatchObject({ tracingEnabled: false });
  });
});
