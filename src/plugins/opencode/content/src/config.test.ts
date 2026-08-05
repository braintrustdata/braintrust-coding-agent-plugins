import { afterEach, beforeEach, describe, expect, it } from "bun:test"
import { loadConfig, type PluginConfig, parseBooleanEnv } from "./config"

describe("parseBooleanEnv", () => {
  it("accepts true and 1 case-insensitively", () => {
    expect(parseBooleanEnv("true")).toBe(true)
    expect(parseBooleanEnv("TRUE")).toBe(true)
    expect(parseBooleanEnv("1")).toBe(true)
  })

  it("rejects other and missing values", () => {
    expect(parseBooleanEnv(undefined)).toBe(false)
    expect(parseBooleanEnv("false")).toBe(false)
    expect(parseBooleanEnv("yes")).toBe(false)
  })
})

describe("loadConfig", () => {
  const keys = [
    "TRACE_TO_BRAINTRUST",
    "BRAINTRUST_DEBUG",
    "BRAINTRUST_ORG_NAME",
    "BRAINTRUST_PROFILE",
    "BRAINTRUST_PROJECT",
    "BRAINTRUST_ADDITIONAL_METADATA",
    "BRAINTRUST_OPENCODE_ENABLE_TOOLS",
  ]
  const original: Record<string, string | undefined> = {}

  beforeEach(() => {
    for (const key of keys) {
      original[key] = process.env[key]
      delete process.env[key]
    }
  })

  afterEach(() => {
    for (const key of keys) {
      if (original[key] === undefined) delete process.env[key]
      else process.env[key] = original[key]
    }
  })

  it("defaults to the bt default profile and the opencode project", () => {
    expect(loadConfig()).toEqual({
      profile: undefined,
      orgName: undefined,
      projectName: "opencode",
      tracingEnabled: false,
      enableTools: true,
      debug: false,
      additionalMetadata: undefined,
    })
  })

  it("loads routing and behavior from plugin configuration", () => {
    const pluginConfig: PluginConfig = {
      profile: "work",
      org_name: "acme",
      project: "agents",
      trace_to_braintrust: true,
      enable_tools: false,
      debug: true,
      additional_metadata: { team: "platform" },
    }
    expect(loadConfig(pluginConfig)).toEqual({
      profile: "work",
      orgName: "acme",
      projectName: "agents",
      tracingEnabled: true,
      enableTools: false,
      debug: true,
      additionalMetadata: { team: "platform" },
    })
  })

  it("lets environment selection override plugin configuration", () => {
    process.env.BRAINTRUST_PROFILE = "personal"
    process.env.BRAINTRUST_ORG_NAME = "braintrust"
    process.env.BRAINTRUST_PROJECT = "opencode-runs"
    process.env.TRACE_TO_BRAINTRUST = "true"
    process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS = "false"
    process.env.BRAINTRUST_ADDITIONAL_METADATA = '{"ci":true}'

    expect(loadConfig({ profile: "work", project: "other" })).toMatchObject({
      profile: "personal",
      orgName: "braintrust",
      projectName: "opencode-runs",
      tracingEnabled: true,
      enableTools: false,
      additionalMetadata: { ci: true },
    })
  })

  it("ignores invalid optional metadata", () => {
    process.env.BRAINTRUST_ADDITIONAL_METADATA = "not-json"
    expect(loadConfig({ additional_metadata: { fallback: true } }).additionalMetadata).toEqual({
      fallback: true,
    })
  })
})
