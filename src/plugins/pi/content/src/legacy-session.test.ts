import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
  it("returns only valid continuation state for the matching session file", async () => {
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

    expect(legacyContinuationFor("/tmp/live.jsonl")).toEqual({
      root_span_id: "legacy-root",
      trace_root_span_id: "legacy-trace",
      parent_span_id: "upstream",
      total_turns: 3,
      total_tool_calls: 7,
    });
    expect(legacyContinuationFor("/tmp/other.jsonl")).toBeUndefined();
  });

  it("fails open when legacy state is malformed or incomplete", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "braintrust-pi-state-"));
    stateDirs.push(stateDir);
    process.env.BRAINTRUST_STATE_DIR = stateDir;
    await writeFile(join(stateDir, "sessions.json"), "not json");
    expect(legacyContinuationFor("/tmp/live.jsonl")).toBeUndefined();
  });
});
