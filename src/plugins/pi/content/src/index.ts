import { createHash, randomUUID } from "node:crypto";
import { resolve } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { loadConfig } from "./config.ts";
import { legacyContinuationFor, type LegacyContinuation } from "./legacy-session.ts";
import { runningPiPackage } from "./pi-package.ts";
import { claimManagedTracingInstance, DaemonClient } from "./runtime/daemon-client.ts";
import { EXTENSION_VERSION } from "./version.ts";

const STATUS_KEY = "braintrust-tracing";
const WIDGET_KEY = "braintrust-trace-link";
const UI_STATUS_TIMEOUT_MS = 250;
// The daemon uploads a turn after Pi reports agent_end: about a second later
// with flush_on_turn_end, and when its 30 s batch window closes with
// fire_and_forget. Poll for the trace link long enough to cover both.
const TRACE_LINK_POLL_INTERVAL_MS = 2_000;
const TRACE_LINK_POLL_TIMEOUT_MS = 40_000;
const PI_VERSION = runningPiPackage().version;
const PI_HOST_NAME = runningPiPackage().name;

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
} {
  const sessionFile = ctx.sessionManager.getSessionFile();
  const nativeSessionId = ctx.sessionManager.getSessionId();
  return {
    sessionId: sessionKeyFor(sessionFile, nativeSessionId, ctx.cwd),
    sessionFile,
    nativeSessionId,
  };
}

export default function braintrustPiExtension(pi: ExtensionAPI): void {
  const config = loadConfig(process.cwd());
  if (!config.enabled) return;
  if (!claimManagedTracingInstance("pi")) return;

  let sessionId: string | undefined;
  let legacyContinuation: LegacyContinuation | undefined;
  let legacyContinuationSessionId: string | undefined;
  let lastContext: ExtensionContext | undefined;
  let awaitingFirstToken = false;
  let lastStreamingKeepaliveAt = 0;
  let uiGeneration = 0;
  // The session_start refresh runs before the daemon has delivered a trace,
  // so agent runs poll status until the current session shows its link.
  let traceLinkShown = false;
  let traceLinkPollGeneration: number | undefined;
  let traceLinkPollTimer: ReturnType<typeof setTimeout> | undefined;
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
  // Pi applies a 30 second deadline to extension handlers. Daemon capture is
  // durable but can wait behind disk or session work, so serialize forwarding
  // in the extension and let ordinary Pi callbacks return immediately.
  let forwarding = Promise.resolve();

  const updateSession = async (
    ctx: ExtensionContext,
  ): Promise<ReturnType<typeof sessionDescriptor>> => {
    lastContext = ctx;
    const descriptor = sessionDescriptor(ctx);
    if (sessionId !== descriptor.sessionId) {
      uiGeneration += 1;
      traceLinkShown = false;
    }
    sessionId = descriptor.sessionId;
    if (legacyContinuationSessionId !== descriptor.sessionId) {
      // The legacy file is immutable migration input. Read it asynchronously
      // once per Pi session before forwarding its first event.
      legacyContinuation = await legacyContinuationFor(descriptor.sessionFile);
      legacyContinuationSessionId = descriptor.sessionId;
    }
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
    const permalink = config.showTraceLink ? daemonSession?.permalink : undefined;
    ctx.ui.setWidget(WIDGET_KEY, permalink ? ["Braintrust trace", permalink] : undefined, {
      placement: "belowEditor",
    });
    traceLinkShown = Boolean(permalink);
  };

  const pollTraceLink = (ctx: ExtensionContext): void => {
    if (!ctx.hasUI || !config.showUi || !config.showTraceLink) return;
    if (traceLinkShown || traceLinkPollGeneration === uiGeneration) return;
    const generation = uiGeneration;
    const deadline = Date.now() + TRACE_LINK_POLL_TIMEOUT_MS;
    traceLinkPollGeneration = generation;
    const poll = async (): Promise<void> => {
      traceLinkPollTimer = undefined;
      if (generation === uiGeneration) await refreshUi(ctx);
      if (traceLinkShown || generation !== uiGeneration || Date.now() >= deadline) {
        if (traceLinkPollGeneration === generation) traceLinkPollGeneration = undefined;
        return;
      }
      traceLinkPollTimer = setTimeout(() => void poll(), TRACE_LINK_POLL_INTERVAL_MS);
      traceLinkPollTimer.unref?.();
    };
    void poll();
  };

  const forward = async (
    name: string,
    event: unknown,
    ctx?: ExtensionContext,
    updateUi = false,
    timestamp = Date.now(),
  ): Promise<void> => {
    const descriptor = ctx ? await updateSession(ctx) : undefined;
    if (!sessionId) return;
    await client.log({
      source: "pi",
      ...(PI_VERSION ? { source_version: PI_VERSION } : {}),
      session_id: sessionId,
      event: name,
      ts_ms: timestamp,
      payload: {
        event: nativePayload(event),
        extension_version: EXTENSION_VERSION,
        host_name: PI_HOST_NAME,
        session_file: descriptor?.sessionFile,
        native_session_id: descriptor?.nativeSessionId,
        legacy_resume: legacyContinuation,
        cwd: ctx?.cwd,
        model: nativePayload(ctx?.model),
      },
      route: config.route,
    });
    if (updateUi && ctx) void refreshUi(ctx);
  };

  const enqueueForward = (
    name: string,
    event: unknown,
    ctx?: ExtensionContext,
    updateUi = false,
  ): Promise<void> => {
    const timestamp = Date.now();
    forwarding = forwarding
      .then(() => forward(name, event, ctx, updateUi, timestamp))
      .catch((error) => {
        if (lastContext?.hasUI && config.showUi) {
          lastContext.ui.setStatus(STATUS_KEY, `Braintrust tracing unavailable: ${String(error)}`);
        }
      });
    return forwarding;
  };

  pi.on("session_start", (event, ctx) => {
    void enqueueForward("session_start", event, ctx).then(() => refreshUi(ctx));
  });
  pi.on("input", (event) => void enqueueForward("input", event));
  pi.on("before_agent_start", (event, ctx) => {
    void enqueueForward("before_agent_start", event, ctx);
  });
  pi.on("context", (event, ctx) => {
    awaitingFirstToken = true;
    lastStreamingKeepaliveAt = 0;
    void enqueueForward("context", event, ctx);
  });
  pi.on("before_provider_request", (event) => {
    void enqueueForward("before_provider_request", event);
  });
  pi.on("after_provider_response", (event) => {
    void enqueueForward("after_provider_response", event);
  });
  pi.on("message_update", (event) => {
    const kind = streamingUpdateKind(event);
    const tokenKinds = ["text_delta", "thinking_delta", "text", "thinking"];
    const isFirstToken = awaitingFirstToken && !!kind && tokenKinds.includes(kind);
    if (!kind || (!tokenKinds.includes(kind) && kind !== "toolcall_delta")) return;
    const now = Date.now();
    if (!isFirstToken && now - lastStreamingKeepaliveAt < 10_000) return;
    if (isFirstToken) awaitingFirstToken = false;
    lastStreamingKeepaliveAt = now;
    void enqueueForward("message_update", event);
  });
  pi.on("thinking_level_select", (event) => {
    void enqueueForward("thinking_level_select", event);
  });
  pi.on("message_end", (event) => {
    awaitingFirstToken = false;
    lastStreamingKeepaliveAt = 0;
    void enqueueForward("message_end", event);
  });
  pi.on("tool_execution_start", (event) => {
    void enqueueForward("tool_execution_start", event);
  });
  pi.on("tool_execution_end", (event, ctx) => {
    void enqueueForward("tool_execution_end", event, ctx);
  });
  pi.on("session_before_compact", (event, ctx) => {
    void enqueueForward("session_before_compact", event, ctx);
  });
  pi.on("session_compact", (event, ctx) => {
    void enqueueForward("session_compact", event, ctx, true);
  });
  pi.on("session_before_tree", (event, ctx) => {
    void enqueueForward("session_before_tree", event, ctx);
  });
  pi.on("session_tree", (event, ctx) => {
    void enqueueForward("session_tree", event, ctx, true);
  });
  // All delivery flushing belongs to the daemon, including session shutdown.
  pi.on("agent_end", (event, ctx) => {
    void enqueueForward("agent_end", event).then(() => pollTraceLink(ctx));
  });
  pi.on("session_shutdown", async (event, ctx) => {
    // Shutdown is the final lifecycle boundary; drain prior events and persist
    // this one before releasing the daemon clients.
    await enqueueForward("session_shutdown", event, ctx);
    // Invalidate fire-and-forget refreshes before clearing the UI so a status
    // request that finishes during shutdown cannot restore stale state.
    uiGeneration += 1;
    clearTimeout(traceLinkPollTimer);
    traceLinkPollTimer = undefined;
    sessionId = undefined;
    legacyContinuation = undefined;
    legacyContinuationSessionId = undefined;
    lastContext = undefined;
    if (ctx.hasUI) {
      ctx.ui.setStatus(STATUS_KEY, undefined);
      ctx.ui.setWidget(WIDGET_KEY, undefined);
    }
    await client.close();
    await statusClient.close();
  });
}
