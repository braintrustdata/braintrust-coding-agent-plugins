import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import type { BraintrustConfig } from "../config";
import { BtCliToolsClient } from "./bt-cli";

const config: BraintrustConfig = {
  profile: "work",
  orgName: "acme",
  projectName: "agents",
  tracingEnabled: true,
  enableTools: true,
  debug: false,
};

describe("BtCliToolsClient", () => {
  it("passes profile and org selection to bt for project listing", async () => {
    const calls: string[][] = [];
    const client = new BtCliToolsClient(config, async (args) => {
      calls.push(args);
      return '[{"id":"project-1","name":"agents"}]';
    });

    expect(await client.listProjects()).toEqual([{ id: "project-1", name: "agents" }]);
    expect(calls).toEqual([
      [
        "projects",
        "list",
        "--json",
        "--no-input",
        "--prefer-profile",
        "--profile",
        "work",
        "--org",
        "acme",
      ],
    ]);
  });

  it("resolves the selected project and delegates SQL to bt", async () => {
    const calls: string[][] = [];
    const client = new BtCliToolsClient(config, async (args) => {
      calls.push(args);
      return args[0] === "projects"
        ? '[{"id":"project-1","name":"agents"}]'
        : '{"data":[{"id":"row-1"}]}';
    });

    expect(await client.queryLogs("SELECT * FROM logs LIMIT 1")).toEqual({
      data: [{ id: "row-1" }],
    });
    expect(calls[1]).toContain("SELECT * FROM project_logs('project-1') LIMIT 1");
    expect(calls[1]).toContain("--project");
    expect(calls[1]).toContain("agents");
  });

  it("delegates experiment listing and applies the requested limit", async () => {
    const client = new BtCliToolsClient(config, async () => '[{"id":"one"},{"id":"two"}]');
    expect(await client.listExperiments(1)).toEqual([{ id: "one" }]);
  });

  it("delegates manual logs through bt sync push and removes the temporary input", async () => {
    let inputPath = "";
    let inputContents = "";
    const client = new BtCliToolsClient(config, async (args) => {
      if (args[0] === "projects") return '[{"id":"project-1","name":"agents"}]';
      inputPath = args[args.indexOf("--in") + 1] ?? "";
      inputContents = readFileSync(inputPath, "utf8");
      return '{"uploaded_rows":1}';
    });

    const id = await client.logData({
      id: "row-1",
      span_id: "span-1",
      root_span_id: "span-1",
      input: "hello",
      span_attributes: { name: "Manual Log", type: "task" },
    });

    expect(id).toBe("row-1");
    expect(JSON.parse(inputContents)).toMatchObject({ id: "row-1", input: "hello" });
    expect(inputPath).toContain("bt-opencode-tools-");
    expect(existsSync(inputPath)).toBe(false);
  });
});
