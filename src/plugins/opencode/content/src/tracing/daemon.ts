import { randomUUID } from "node:crypto";
import type { Hooks, PluginInput } from "@opencode-ai/plugin";
import type { Event } from "@opencode-ai/sdk";
import { DaemonClient, type DaemonSessionRoute } from "../runtime/daemon-client";
import { PLUGIN_VERSION } from "../version";

type Logger = (message: string, extra?: Record<string, unknown>) => void;

interface TracingRouteConfig {
  profile?: string;
  orgName?: string;
  projectName: string;
  additionalMetadata?: Record<string, unknown>;
  route: DaemonSessionRoute;
}

const FORWARDED_NATIVE_EVENTS = new Set([
  "session.created",
  "session.idle",
  "session.compacted",
  "session.deleted",
  "session.error",
  "message.part.updated",
  "message.updated",
  "permission.asked",
  "permission.replied",
]);

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function nonemptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function nativeSessionId(event: string, payload: Record<string, unknown>): string | undefined {
  const properties = record(payload.properties);
  const input = record(payload.input);
  const info = record(properties.info);
  const part = record(properties.part);
  const permission = record(properties.permission);
  const request = record(properties.request);
  const tool = record(permission.tool);
  return (
    nonemptyString(input.sessionID) ??
    nonemptyString(payload.sessionID) ??
    nonemptyString(properties.sessionID) ??
    nonemptyString(info.sessionID) ??
    nonemptyString(part.sessionID) ??
    nonemptyString(permission.sessionID) ??
    nonemptyString(request.sessionID) ??
    nonemptyString(tool.sessionID) ??
    (event === "session.created" ? nonemptyString(info.id) : undefined)
  );
}

function permissionId(payload: Record<string, unknown>): string | undefined {
  const properties = record(payload.properties);
  return (
    nonemptyString(properties.requestID) ??
    nonemptyString(properties.permissionID) ??
    nonemptyString(record(properties.permission).id) ??
    nonemptyString(record(properties.info).id) ??
    nonemptyString(properties.id)
  );
}

export function createDaemonTracingHooks(
  input: PluginInput,
  config: TracingRouteConfig,
  log: Logger,
): Partial<Hooks> {
  // A native top-level session keeps the same daemon identity across OpenCode
  // process runs. Subagent events use their top-level parent's stream so the
  // translator can preserve the native session graph inside one trace.
  const fallbackSessionId = randomUUID();
  const roots = new Map<string, string>();
  const permissionRoots = new Map<string, string>();
  const started = new Set<string>();
  const queues = new Map<string, Promise<void>>();
  const daemon = new DaemonClient({
    source: "opencode",
    pluginVersion: PLUGIN_VERSION,
    warn: (message) => log("Braintrust tracing unavailable", { message }),
  });

  const forward = async (event: string, payload: unknown, sessionId?: string) => {
    const nativePayload = record(payload);
    const nativeId = nativeSessionId(event, nativePayload);
    if (event === "session.created" && nativeId) {
      const info = record(record(nativePayload.properties).info);
      const parentId = nonemptyString(info.parentID);
      roots.set(nativeId, parentId ? (roots.get(parentId) ?? parentId) : nativeId);
    }
    const requestId = event.startsWith("permission.") ? permissionId(nativePayload) : undefined;
    const daemonSessionId =
      sessionId ??
      (nativeId
        ? (roots.get(nativeId) ?? nativeId)
        : ((requestId && permissionRoots.get(requestId)) ?? fallbackSessionId));
    if (event === "permission.asked" && requestId) {
      permissionRoots.set(requestId, daemonSessionId);
    }
    const envelope = (name: string) => ({
      source: "opencode",
      source_version: process.env.OPENCODE_VERSION,
      session_id: daemonSessionId,
      event: name,
      ts_ms: Date.now(),
      payload: {
        ...nativePayload,
        directory: input.directory,
        worktree: input.worktree,
      },
      route: config.route,
    });
    // OpenCode may resume an existing native session without emitting another
    // session.created. A synthetic lifecycle boundary on the first event of
    // this process refreshes the registry's process mapping. The translator
    // ignores this event; the native payload remains unmodified.
    const first = !started.has(daemonSessionId);
    started.add(daemonSessionId);
    const previous = queues.get(daemonSessionId) ?? Promise.resolve();
    const next = previous
      .catch(() => {})
      .then(async () => {
        if (first && event !== "session.created" && event !== "server.instance.disposed") {
          await daemon.log(envelope("session_start"));
        }
        await daemon.log(envelope(event));
      });
    queues.set(daemonSessionId, next);
    try {
      await next;
    } finally {
      if (queues.get(daemonSessionId) === next) queues.delete(daemonSessionId);
    }
    if (event === "permission.replied" && requestId) {
      permissionRoots.delete(requestId);
    }
  };

  return {
    event: async ({ event }: { event: Event }) => {
      if (event.type === "server.instance.disposed") {
        // Give the daemon a durable terminal event before disconnecting. It owns
        // the potentially slow backend flush, so OpenCode shutdown is not blocked.
        await Promise.all(
          [...started].map((sessionId) =>
            forward(event.type, { properties: event.properties }, sessionId),
          ),
        );
        await daemon.close();
        return;
      }
      if (!FORWARDED_NATIVE_EVENTS.has(event.type)) return;
      await forward(event.type, { properties: event.properties });
    },
    "chat.message": async (hookInput, hookOutput) =>
      forward("chat.message", { input: hookInput, output: hookOutput }),
    "experimental.chat.system.transform": async (hookInput, hookOutput) =>
      forward("experimental.chat.system.transform", {
        input: hookInput,
        output: hookOutput,
      }),
    "tool.execute.before": async (hookInput, hookOutput) =>
      forward("tool.execute.before", { input: hookInput, output: hookOutput }),
    "tool.execute.after": async (hookInput, hookOutput) =>
      forward("tool.execute.after", { input: hookInput, result: hookOutput }),
  };
}
