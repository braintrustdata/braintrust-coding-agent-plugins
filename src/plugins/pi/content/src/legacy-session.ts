import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface LegacyContinuation {
  root_span_id: string;
  trace_root_span_id: string;
  parent_span_id?: string;
  total_turns: number;
  total_tool_calls: number;
}

/**
 * Read the small, local state file written by the pre-daemon Pi extension.
 * This is deliberately a compatibility boundary: invalid or unavailable state
 * must leave a new daemon session untouched.
 */
export function legacyContinuationFor(
  sessionFile: string | undefined,
): LegacyContinuation | undefined {
  if (!sessionFile) return undefined;
  const stateDir =
    process.env.BRAINTRUST_STATE_DIR ??
    join(homedir(), ".pi", "agent", "state", "braintrust-pi-extension");
  const stateFile = join(stateDir, "sessions.json");
  if (!existsSync(stateFile)) return undefined;

  try {
    const parsed: unknown = JSON.parse(readFileSync(stateFile, "utf8"));
    const sessions = objectValue(parsed)?.sessions;
    const session = objectValue(objectValue(sessions)?.[`file:${sessionFile}`]);
    const rootSpanId = stringValue(session?.rootSpanId);
    if (!rootSpanId) return undefined;
    const traceRootSpanId = stringValue(session?.traceRootSpanId) ?? rootSpanId;
    const totalTurns = countValue(session?.totalTurns);
    const totalToolCalls = countValue(session?.totalToolCalls);
    if (totalTurns === undefined || totalToolCalls === undefined) return undefined;
    return {
      root_span_id: rootSpanId,
      trace_root_span_id: traceRootSpanId,
      ...(stringValue(session?.parentSpanId)
        ? { parent_span_id: stringValue(session?.parentSpanId) }
        : {}),
      total_turns: totalTurns,
      total_tool_calls: totalToolCalls,
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
