import { randomUUID } from "node:crypto";
import type { Hooks, PluginInput } from "@opencode-ai/plugin";
import type { Event } from "@opencode-ai/sdk";
import { DaemonClient } from "../runtime/daemon-client";
import { PLUGIN_VERSION } from "../version";

type Logger = (message: string, extra?: Record<string, unknown>) => void;

interface TracingRouteConfig {
  profile?: string;
  orgName?: string;
  projectName: string;
  additionalMetadata?: Record<string, unknown>;
}

const FORWARDED_NATIVE_EVENTS = new Set([
  "session.created",
  "session.idle",
  "session.deleted",
  "session.error",
  "message.part.updated",
  "message.updated",
  "permission.asked",
  "permission.replied",
]);

export function createDaemonTracingHooks(
  input: PluginInput,
  config: TracingRouteConfig,
  log: Logger,
): Partial<Hooks> {
  // One transport stream per plugin instance. Native OpenCode session IDs and
  // parent relationships stay untouched in each payload for the daemon to
  // interpret.
  const daemonSessionId = randomUUID();
  const daemon = new DaemonClient({
    source: "opencode",
    pluginVersion: PLUGIN_VERSION,
    warn: (message) => log("Braintrust tracing unavailable", { message }),
  });

  const forward = async (event: string, payload: unknown) => {
    await daemon.log({
      source: "opencode",
      source_version: process.env.OPENCODE_VERSION,
      session_id: daemonSessionId,
      event,
      ts_ms: Date.now(),
      payload: {
        ...(payload as Record<string, unknown>),
        directory: input.directory,
        worktree: input.worktree,
      },
      route: {
        auth: {
          ...(config.profile ? { profile: config.profile } : {}),
          ...(config.orgName ? { org_name: config.orgName } : {}),
        },
        destination: {
          type: "project_logs",
          project_name: config.projectName,
        },
        flush_mode: "fire_and_forget",
        ...(config.additionalMetadata ? { additional_metadata: config.additionalMetadata } : {}),
      },
    });
    if (event === "session.idle" || event === "session.deleted" || event === "session.error") {
      await daemon.flush(daemonSessionId);
    }
  };

  return {
    event: async ({ event }: { event: Event }) => {
      if (event.type === "server.instance.disposed") {
        await daemon.flush(daemonSessionId);
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
