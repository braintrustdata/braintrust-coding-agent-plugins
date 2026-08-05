import type { Hooks, PluginInput } from "@opencode-ai/plugin"
import type { Event } from "@opencode-ai/sdk"
import { DaemonClient } from "../runtime/daemon-client"
import { PLUGIN_VERSION } from "../version"

type Logger = (message: string, extra?: Record<string, unknown>) => void

function eventSessionId(event: Event): { id?: string; parentId?: string } {
  const properties = event.properties as Record<string, unknown>
  const info = properties.info as Record<string, unknown> | undefined
  return {
    id: (properties.sessionID ?? info?.id ?? properties.id) as string | undefined,
    parentId: info?.parentID as string | undefined,
  }
}

/**
 * OpenCode can emit child sessions through the same plugin instance. Route a
 * child through its root daemon actor so the Rust translator can preserve the
 * native parent/child hierarchy without sharing mutable state across actors.
 */
class SessionRouter {
  private roots = new Map<string, string>()

  route(sessionId: string, parentId?: string): string {
    const root = parentId
      ? (this.roots.get(parentId) ?? parentId)
      : (this.roots.get(sessionId) ?? sessionId)
    this.roots.set(sessionId, root)
    return root
  }
}

export function createDaemonTracingHooks(input: PluginInput, log: Logger): Partial<Hooks> {
  const router = new SessionRouter()
  const daemon = new DaemonClient({
    source: "opencode",
    pluginVersion: PLUGIN_VERSION,
    warn: (message) => log("Braintrust tracing unavailable", { message }),
  })

  const forward = async (
    event: string,
    nativeSessionId: string,
    payload: unknown,
    parentId?: string,
  ) => {
    const sessionId = router.route(nativeSessionId, parentId)
    await daemon.log({
      source: "opencode",
      source_version: process.env.OPENCODE_VERSION,
      session_id: sessionId,
      event,
      ts_ms: Date.now(),
      payload: {
        ...(payload as Record<string, unknown>),
        directory: input.directory,
        worktree: input.worktree,
      },
    })
    if (event === "session.idle" || event === "session.deleted" || event === "session.error") {
      await daemon.flush(sessionId)
    }
  }

  return {
    event: async ({ event }: { event: Event }) => {
      const session = eventSessionId(event)
      if (!session.id) return
      await forward(event.type, session.id, { properties: event.properties }, session.parentId)
    },
    "chat.message": async (hookInput, hookOutput) =>
      forward("chat.message", hookInput.sessionID, { input: hookInput, output: hookOutput }),
    "experimental.chat.system.transform": async (hookInput, hookOutput) =>
      forward("experimental.chat.system.transform", hookInput.sessionID, {
        input: hookInput,
        output: hookOutput,
      }),
    "tool.execute.before": async (hookInput, hookOutput) =>
      forward("tool.execute.before", hookInput.sessionID, { input: hookInput, output: hookOutput }),
    "tool.execute.after": async (hookInput, hookOutput) =>
      forward("tool.execute.after", hookInput.sessionID, { input: hookInput, result: hookOutput }),
  }
}
