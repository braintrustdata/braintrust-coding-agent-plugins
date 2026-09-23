import { createHash, randomUUID } from "node:crypto";
import { resolve } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { loadConfig } from "./config.ts";
import { legacyContinuationFor, type LegacyContinuation } from "./legacy-session.ts";
import { loadPiPackageMetadata } from "./pi-package.ts";
import { claimManagedTracingInstance, DaemonClient } from "./runtime/daemon-client.ts";
import { EXTENSION_VERSION } from "./version.ts";

const STATUS_KEY = "braintrust-tracing";
const WIDGET_KEY = "braintrust-trace-link";
const UI_STATUS_TIMEOUT_MS = 250;
const PI_VERSION = loadPiPackageMetadata().version;

function sessionKeyFor(
  sessionFile: string | undefined,
  sessionId: string | undefined,
  cwd: string,
): string {
  if (sessionFile) return `file:${sessionFile}`;
  const projectKey = createHash("sha256").update(resolve(cwd)).digest("hex").slice(0, 12);
  return `ephemeral:${projectKey}:${sessionId ?? randomUUID()}`;
}

function nativePayload(value: unknown): unknown {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return { serialization_error: "Pi event payload was not JSON serializable" };
  }
}

type JsonObject = Record<string, unknown>;

function asObject(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;
}

function streamingUpdateKind(value: unknown): string | undefined {
  const event = asObject(value);
  const update = asObject(event?.assistantMessageEvent);
  if (typeof update?.type === "string") return update.type;
  return typeof event?.type === "string" && event.type !== "message_update"
    ? event.type
    : undefined;
}

function sessionDescriptor(ctx: ExtensionContext): {
  sessionId: string;
  sessionFile?: string;
  nativeSessionId?: string;
  legacyContinuation?: LegacyContinuation;
} {
  const sessionFile = ctx.sessionManager.getSessionFile();
  const nativeSessionId = ctx.sessionManager.getSessionId();
  return {
    sessionId: sessionKeyFor(sessionFile, nativeSessionId, ctx.cwd),
    sessionFile,
    nativeSessionId,
    legacyContinuation: legacyContinuationFor(sessionFile),
  };
}

export default function braintrustPiExtension(pi: ExtensionAPI): void {
  const config = loadConfig(process.cwd());
  if (!config.enabled) return;
  if (!claimManagedTracingInstance("pi")) return;

  let sessionId: string | undefined;
  let legacyContinuation: LegacyContinuation | undefined;
  let lastContext: ExtensionContext | undefined;
  let awaitingFirstToken = false;
  let uiGeneration = 0;
  const client = new DaemonClient({
    source: "pi",
    pluginVersion: EXTENSION_VERSION,
    warn: (message) => {
      if (lastContext?.hasUI && config.showUi) {
        lastContext.ui.setStatus(STATUS_KEY, `Braintrust tracing unavailable: ${message}`);
      }
    },
  });
  // status.get waits for the daemon's entire ingress queue to settle. Keep UI
  // lookups off the event client so a busy daemon cannot block event capture.
  const statusClient = new DaemonClient({
    source: "pi",
    pluginVersion: EXTENSION_VERSION,
    requestTimeoutMs: UI_STATUS_TIMEOUT_MS,
  });

  const remember = (ctx: ExtensionContext): ReturnType<typeof sessionDescriptor> => {
    lastContext = ctx;
    const descriptor = sessionDescriptor(ctx);
    if (sessionId !== descriptor.sessionId) uiGeneration += 1;
    sessionId = descriptor.sessionId;
    legacyContinuation = descriptor.legacyContinuation;
    return descriptor;
  };

  const refreshUi = async (ctx: ExtensionContext): Promise<void> => {
    if (!ctx.hasUI || !config.showUi || !sessionId) return;
    const refreshGeneration = uiGeneration;
    const refreshSessionId = sessionId;
    const status = await statusClient.status(refreshSessionId);
    if (uiGeneration !== refreshGeneration || sessionId !== refreshSessionId) return;
    const daemonSession = status?.sessions.find(
      (session) => session.session_id === refreshSessionId,
    );
    if (daemonSession?.last_error) {
      ctx.ui.setStatus(STATUS_KEY, `Braintrust: ${daemonSession.last_error}`);
    } else {
      ctx.ui.setStatus(STATUS_KEY, "Braintrust: tracing");
    }
    ctx.ui.setWidget(
      WIDGET_KEY,
      config.showTraceLink && daemonSession?.permalink
        ? ["Braintrust trace", daemonSession.permalink]
        : undefined,
      { placement: "belowEditor" },
    );
  };

  const forward = async (
    name: string,
    event: unknown,
    ctx?: ExtensionContext,
    updateUi = false,
  ): Promise<void> => {
    const descriptor = ctx ? remember(ctx) : undefined;
    if (!sessionId) return;
    await client.log({
      source: "pi",
      ...(PI_VERSION ? { source_version: PI_VERSION } : {}),
      session_id: sessionId,
      event: name,
      ts_ms: Date.now(),
      payload: {
        event: nativePayload(event),
        extension_version: EXTENSION_VERSION,
        session_file: descriptor?.sessionFile,
        native_session_id: descriptor?.nativeSessionId,
        legacy_continuation: descriptor?.legacyContinuation ?? legacyContinuation,
        cwd: ctx?.cwd,
        model: nativePayload(ctx?.model),
      },
      route: config.route,
    });
    if (updateUi && ctx) void refreshUi(ctx);
  };

  pi.on("session_start", async (event, ctx) => {
    await forward("session_start", event, ctx);
    void refreshUi(ctx);
  });
  pi.on("input", async (event) => forward("input", event));
  pi.on("before_agent_start", async (event, ctx) => forward("before_agent_start", event, ctx));
  pi.on("context", async (event, ctx) => {
    awaitingFirstToken = true;
    await forward("context", event, ctx);
  });
  pi.on("before_provider_request", async (event) => forward("before_provider_request", event));
  pi.on("after_provider_response", async (event) => forward("after_provider_response", event));
  pi.on("message_update", async (event) => {
    if (!awaitingFirstToken) return;
    const kind = streamingUpdateKind(event);
    if (!kind || !["text_delta", "thinking_delta", "text", "thinking"].includes(kind)) return;
    awaitingFirstToken = false;
    await forward("message_update", event);
  });
  pi.on("thinking_level_select", async (event) => forward("thinking_level_select", event));
  pi.on("message_end", async (event) => {
    awaitingFirstToken = false;
    await forward("message_end", event);
  });
  pi.on("tool_execution_start", async (event) => forward("tool_execution_start", event));
  pi.on("tool_execution_end", async (event, ctx) => forward("tool_execution_end", event, ctx));
  pi.on("session_before_compact", async (event, ctx) =>
    forward("session_before_compact", event, ctx),
  );
  pi.on("session_compact", async (event, ctx) => forward("session_compact", event, ctx, true));
  pi.on("session_before_tree", async (event, ctx) => forward("session_before_tree", event, ctx));
  pi.on("session_tree", async (event, ctx) => forward("session_tree", event, ctx, true));
  // All delivery flushing belongs to the daemon, including session shutdown.
  pi.on("agent_end", async (event) => forward("agent_end", event));
  pi.on("session_shutdown", async (event, ctx) => {
    await forward("session_shutdown", event, ctx);
    // Invalidate fire-and-forget refreshes before clearing the UI so a status
    // request that finishes during shutdown cannot restore stale state.
    uiGeneration += 1;
    sessionId = undefined;
    legacyContinuation = undefined;
    lastContext = undefined;
    if (ctx.hasUI) {
      ctx.ui.setStatus(STATUS_KEY, undefined);
      ctx.ui.setWidget(WIDGET_KEY, undefined);
    }
    await client.close();
    await statusClient.close();
  });
}
