import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

export interface LegacyContinuation {
  span: string;
  trace: string;
  parent?: string;
  turns: number;
  tools: number;
}

/**
 * Read the small, local state file written by the pre-daemon Pi extension.
 * This is deliberately a compatibility boundary: invalid or unavailable state
 * must leave a new daemon session untouched.
 */
export async function legacyContinuationFor(
  sessionFile: string | undefined,
): Promise<LegacyContinuation | undefined> {
  if (!sessionFile) return undefined;
  const stateDir =
    process.env.BRAINTRUST_STATE_DIR ??
    join(homedir(), ".pi", "agent", "state", "braintrust-pi-extension");
  const stateFile = join(stateDir, "sessions.json");
  try {
    const parsed: unknown = JSON.parse(await readFile(stateFile, "utf8"));
    const sessions = objectValue(parsed)?.sessions;
    // Version 0.9.0 stored the key with an absolute session-file path.
    const session = objectValue(objectValue(sessions)?.[`file:${resolve(sessionFile)}`]);
    const rootSpanId = stringValue(session?.rootSpanId);
    if (!rootSpanId) return undefined;
    const traceRootSpanId = stringValue(session?.traceRootSpanId) ?? rootSpanId;
    const totalTurns = countValue(session?.totalTurns);
    const totalToolCalls = countValue(session?.totalToolCalls);
    if (totalTurns === undefined || totalToolCalls === undefined) return undefined;
    return {
      span: rootSpanId,
      trace: traceRootSpanId,
      ...(stringValue(session?.parentSpanId) ? { parent: stringValue(session?.parentSpanId) } : {}),
      turns: totalTurns,
      tools: totalToolCalls,
    };
  } catch {
    return undefined;
  }
}

function objectValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function countValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}
