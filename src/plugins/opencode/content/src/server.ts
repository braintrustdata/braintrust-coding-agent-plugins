/** OpenCode 2 server entrypoint. OpenCode 1 continues to load the root export. */
import { loadConfig } from "./config";
import { claimManagedTracingInstance, DaemonClient } from "./runtime/daemon-client";
import { readPluginConfig } from "./tracing/plugin";
import { PLUGIN_VERSION } from "./version";
import { tool, type ToolContext } from "@opencode-ai/plugin";
import { homedir } from "node:os";
import { join } from "node:path";
import { createBraintrustTools } from "./tools";
import { BtCliToolsClient } from "./tools/bt-cli";

interface NativeEvent {
  type: string;
  created?: number;
  location?: { directory?: string };
  data?: { sessionID?: string; parentID?: string };
}

interface Context {
  app: { version: string };
  location: { directory: string };
  event: { subscribe(options: { signal: AbortSignal }): AsyncIterable<NativeEvent> };
  session: {
    get(input: { sessionID: string }): Promise<{ parentID?: string | null }>;
    hook(
      name: "context",
      callback: (event: {
        sessionID: string;
        system: Array<{ text: string }>;
        messages: unknown[];
      }) => void,
    ): Promise<unknown>;
  };
  tool: {
    transform(
      callback: (editor: {
        add(definition: {
          name: string;
          description: string;
          input: object;
          execute(input: unknown): Promise<{ content: string }>;
        }): void;
      }) => void,
    ): Promise<unknown>;
  };
}

const forwarded = new Set([
  "session.created",
  "session.inbox.enqueued",
  "session.inbox.delivered",
  "session.inbox.cancelled",
  "session.execution.started",
  "session.execution.succeeded",
  "session.execution.failed",
  "session.execution.interrupted",
  "session.step.started",
  "session.step.ended",
  "session.step.failed",
  "session.text.ended",
  "session.reasoning.ended",
  "session.tool.input.started",
  "session.tool.called",
  "session.tool.success",
  "session.tool.failed",
  "permission.asked",
  "permission.replied",
  "session.compaction.started",
  "session.compaction.ended",
  "session.compaction.failed",
  "session.deleted",
]);

export default {
  id: "braintrust.trace",
  async setup(ctx: Context) {
    const config = loadConfig(await readPluginConfig(ctx.location));
    if (config.enableTools) {
      const definitions = createBraintrustTools(new BtCliToolsClient(config));
      await ctx.tool.transform((editor) => {
        for (const [name, definition] of Object.entries(definitions)) {
          editor.add({
            name,
            description: definition.description,
            input: tool.schema.toJSONSchema(tool.schema.object(definition.args)),
            async execute(input) {
              const result = await definition.execute(
                input as Record<string, unknown>,
                {} as ToolContext,
              );
              return { content: typeof result === "string" ? result : result.output };
            },
          });
        }
      });
    }
    if (!config.tracingEnabled || !claimManagedTracingInstance("opencode")) return;
    const daemon = new DaemonClient({
      source: "opencode",
      pluginVersion: PLUGIN_VERSION,
      opencodeRecovery: {
        database: join(
          process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"),
          "opencode",
          "opencode.db",
        ),
        since_ms: Date.now(),
      },
    });
    void daemon.prepare();
    const abort = new AbortController();
    const roots = new Map<string, string>();
    const started = new Set<string>();
    const nativeSessions = new Set<string>();
    const lookups = new Map<string, Promise<string>>();
    let cancelLookups: () => void = () => {};
    const cancelled = new Promise<undefined>((resolve) => {
      cancelLookups = () => resolve(undefined);
    });
    const rootFor = (session: string): Promise<string> => {
      const cached = roots.get(session);
      if (cached) return Promise.resolve(cached);
      const pending = lookups.get(session);
      if (pending) return pending;
      const lookup = (async () => {
        const path: string[] = [];
        const seen = new Set<string>();
        let current = session;
        while (!roots.has(current) && !seen.has(current)) {
          seen.add(current);
          path.push(current);
          try {
            const info = await Promise.race([ctx.session.get({ sessionID: current }), cancelled]);
            if (!info?.parentID) break;
            current = info.parentID;
          } catch {
            break;
          }
        }
        const root = roots.get(current) ?? current;
        for (const id of path) roots.set(id, root);
        return root;
      })();
      lookups.set(session, lookup);
      void lookup.then(
        () => lookups.delete(session),
        () => lookups.delete(session),
      );
      return lookup;
    };
    const contexts = new Map<string, Array<{ ts_ms: number; payload: unknown }>>();
    await ctx.session.hook("context", (event) => {
      try {
        nativeSessions.add(event.sessionID);
        void rootFor(event.sessionID);
        const queue = contexts.get(event.sessionID) ?? [];
        const payload = JSON.parse(
          JSON.stringify({
            data: {
              sessionID: event.sessionID,
              system: event.system.map((part) => part.text),
              messages: event.messages,
            },
            location: ctx.location,
          }),
        ) as unknown;
        queue.push({ ts_ms: Date.now(), payload });
        contexts.set(event.sessionID, queue);
      } catch {
        // A malformed capture must not interrupt the agent's model request.
      }
    });
    let stopping = false;
    let wakePump: (() => void) | undefined;
    const task = (async () => {
      const iterator = ctx.event.subscribe({ signal: abort.signal })[Symbol.asyncIterator]();
      while (!stopping) {
        const stopped = new Promise<undefined>((resolve) => {
          wakePump = () => resolve(undefined);
        });
        const next = await Promise.race([iterator.next(), stopped]);
        wakePump = undefined;
        if (!next || next.done) break;
        const event = next.value;
        if (!forwarded.has(event.type)) continue;
        const session = event.data?.sessionID;
        if (!session) continue;
        nativeSessions.add(session);
        if (event.type === "session.created") {
          const parent = event.data?.parentID;
          roots.set(session, parent ? await rootFor(parent) : session);
        }
        const root = roots.get(session) ?? (await rootFor(session));
        const envelope = {
          source: "opencode",
          source_version: ctx.app.version,
          session_id: root,
          event: event.type,
          ts_ms: event.created ?? Date.now(),
          payload: event,
          route: config.route,
        };
        const first = !started.has(root);
        if (first) {
          started.add(root);
        }
        const context =
          event.type === "session.step.started" ? contexts.get(session)?.shift() : undefined;
        if (contexts.get(session)?.length === 0) contexts.delete(session);
        if (
          event.type === "session.execution.succeeded" ||
          event.type === "session.execution.failed" ||
          event.type === "session.execution.interrupted" ||
          event.type === "session.deleted"
        )
          contexts.delete(session);
        if (first && event.type !== "session.created") {
          void daemon.logPipelined({ ...envelope, event: "session_start" });
        }
        if (context) void daemon.logPipelined({ ...envelope, event: "model.context", ...context });
        void daemon.logPipelined(envelope);
      }
    })().catch(() => {});
    return async () => {
      stopping = true;
      cancelLookups();
      wakePump?.();
      abort.abort();
      await task;
      for (const [session, pending] of contexts) {
        const root = roots.get(session) ?? session;
        for (const context of pending) {
          void daemon.logPipelined({
            source: "opencode",
            source_version: ctx.app.version,
            session_id: root,
            event: "model.context",
            route: config.route,
            ...context,
          });
        }
      }
      for (const session of nativeSessions) {
        const root = roots.get(session) ?? session;
        void daemon.logPipelined({
          source: "opencode",
          source_version: ctx.app.version,
          session_id: root,
          event: "session.capture.closed",
          ts_ms: Date.now(),
          payload: { type: "session.capture.closed", data: { sessionID: session } },
          route: config.route,
        });
      }
      await daemon.detach();
      void daemon.close();
    };
  },
};
