import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { legacyContinuationFor } from "./legacy-session.ts";

const previousStateDir = process.env.BRAINTRUST_STATE_DIR;
const stateDirs: string[] = [];

afterEach(async () => {
  if (previousStateDir === undefined) delete process.env.BRAINTRUST_STATE_DIR;
  else process.env.BRAINTRUST_STATE_DIR = previousStateDir;
  await Promise.all(stateDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("legacy Pi session state", () => {
  it("reads the published 0.9.0 legacy state shape for the matching session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "braintrust-pi-state-"));
    stateDirs.push(stateDir);
    process.env.BRAINTRUST_STATE_DIR = stateDir;
    await writeFile(
      join(stateDir, "sessions.json"),
      JSON.stringify({
        version: 1,
        sessions: {
          "file:/tmp/live.jsonl": {
            rootSpanId: "legacy-root",
            traceRootSpanId: "legacy-trace",
            parentSpanId: "upstream",
            totalTurns: 3,
            totalToolCalls: 7,
          },
        },
      }),
    );

    // @braintrust/pi-extension 0.9.0 persisted this exact sessions.json
    // shape, including camelCase fields and an absolute file session key.
    await expect(legacyContinuationFor("/tmp/live.jsonl")).resolves.toEqual({
      span: "legacy-root",
      trace: "legacy-trace",
      parent: "upstream",
      turns: 3,
      tools: 7,
    });
    await expect(
      legacyContinuationFor(relative(process.cwd(), "/tmp/live.jsonl")),
    ).resolves.toMatchObject({
      span: "legacy-root",
    });
    await expect(legacyContinuationFor("/tmp/other.jsonl")).resolves.toBeUndefined();
  });

  it("fails open when legacy state is malformed or incomplete", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "braintrust-pi-state-"));
    stateDirs.push(stateDir);
    process.env.BRAINTRUST_STATE_DIR = stateDir;
    await writeFile(join(stateDir, "sessions.json"), "not json");
    await expect(legacyContinuationFor("/tmp/live.jsonl")).resolves.toBeUndefined();
  });
});
