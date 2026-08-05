import {
  VERSION as PI_VERSION,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { loadConfig } from "./config.ts";
import { DaemonClient } from "./runtime/daemon-client.ts";
import { sessionKeyFor } from "./utils.ts";
import { EXTENSION_VERSION } from "./version.ts";

const STATUS_KEY = "braintrust-tracing";
const WIDGET_KEY = "braintrust-trace-link";

function nativePayload(value: unknown): unknown {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return { serialization_error: "Pi event payload was not JSON serializable" };
  }
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

  let sessionId: string | undefined;
  let lastContext: ExtensionContext | undefined;
  const client = new DaemonClient({
    source: "pi",
    pluginVersion: EXTENSION_VERSION,
    warn: (message) => {
      if (lastContext?.hasUI && config.showUi) {
        lastContext.ui.setStatus(STATUS_KEY, `Braintrust tracing unavailable: ${message}`);
      }
    },
  });

  const remember = (ctx: ExtensionContext): ReturnType<typeof sessionDescriptor> => {
    lastContext = ctx;
    const descriptor = sessionDescriptor(ctx);
    sessionId = descriptor.sessionId;
    return descriptor;
  };

  const refreshUi = async (ctx: ExtensionContext): Promise<void> => {
    if (!ctx.hasUI || !config.showUi || !sessionId) return;
    const status = await client.status(sessionId);
    const daemonSession = status?.sessions.find((session) => session.session_id === sessionId);
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
    flush = false,
  ): Promise<void> => {
    const descriptor = ctx ? remember(ctx) : undefined;
    if (!sessionId) return;
    await client.log({
      source: "pi",
      source_version: PI_VERSION,
      session_id: sessionId,
      event: name,
      ts_ms: Date.now(),
      payload: {
        event: nativePayload(event),
        extension_version: EXTENSION_VERSION,
        session_file: descriptor?.sessionFile,
        native_session_id: descriptor?.nativeSessionId,
        cwd: ctx?.cwd,
        model: nativePayload(ctx?.model),
        trace_settings: {
          project: config.projectName,
          additional_metadata: config.additionalMetadata,
          parent_span_id: config.parentSpanId,
          root_span_id: config.rootSpanId,
        },
      },
    });
    if (flush) {
      await client.flush(sessionId);
      if (ctx) await refreshUi(ctx);
    }
  };

  pi.on("session_start", async (event, ctx) => {
    await forward("session_start", event, ctx);
    await refreshUi(ctx);
  });
  pi.on("input", async (event) => forward("input", event));
  pi.on("before_agent_start", async (event, ctx) => forward("before_agent_start", event, ctx));
  pi.on("context", async (event, ctx) => forward("context", event, ctx));
  pi.on("before_provider_request", async (event) => forward("before_provider_request", event));
  pi.on("after_provider_response", async (event) => forward("after_provider_response", event));
  pi.on("message_update", async (event) => forward("message_update", event));
  pi.on("thinking_level_select", async (event) => forward("thinking_level_select", event));
  pi.on("message_end", async (event) => forward("message_end", event));
  pi.on("tool_execution_start", async (event) => forward("tool_execution_start", event));
  pi.on("tool_execution_end", async (event, ctx) => forward("tool_execution_end", event, ctx));
  pi.on("session_before_compact", async (event, ctx) =>
    forward("session_before_compact", event, ctx),
  );
  pi.on("session_compact", async (event, ctx) => forward("session_compact", event, ctx, true));
  pi.on("session_before_tree", async (event, ctx) => forward("session_before_tree", event, ctx));
  pi.on("session_tree", async (event, ctx) => forward("session_tree", event, ctx, true));
  pi.on("agent_end", async (event) => forward("agent_end", event, undefined, true));
  pi.on("session_shutdown", async (event, ctx) => {
    await forward("session_shutdown", event, ctx, true);
    if (ctx.hasUI) {
      ctx.ui.setStatus(STATUS_KEY, undefined);
      ctx.ui.setWidget(WIDGET_KEY, undefined);
    }
    client.close();
    sessionId = undefined;
    lastContext = undefined;
  });
}
