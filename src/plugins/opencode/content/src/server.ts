/** OpenCode 2 server entrypoint. OpenCode 1 continues to load the root export. */
import { loadConfig } from "./config";
import { claimManagedTracingInstance, DaemonClient } from "./runtime/daemon-client";
import { readPluginConfig } from "./tracing/plugin";
import { PLUGIN_VERSION } from "./version";
import { tool, type ToolContext } from "@opencode-ai/plugin";
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
  "session.deleted",
]);

export default {
  id: "braintrust.trace",
  async setup(ctx: Context) {
    const config = loadConfig(await readPluginConfig(ctx.location));
    if (config.enableTools && !process.env.BT_TRACE_MANAGED_RUN_ID) {
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
    const daemon = new DaemonClient({ source: "opencode", pluginVersion: PLUGIN_VERSION });
    const abort = new AbortController();
    const roots = new Map<string, string>();
    const started = new Set<string>();
    let pending = Promise.resolve();
    const task = (async () => {
      for await (const event of ctx.event.subscribe({ signal: abort.signal })) {
        if (!forwarded.has(event.type)) continue;
        const session = event.data?.sessionID;
        if (!session) continue;
        if (event.type === "session.created") {
          const parent = event.data?.parentID;
          roots.set(session, parent ? (roots.get(parent) ?? parent) : session);
        }
        const root = roots.get(session) ?? session;
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
        pending = pending.then(async () => {
          if (first && event.type !== "session.created") {
            await daemon.log({ ...envelope, event: "session_start" });
          }
          await daemon.log(envelope);
        });
      }
    })().catch(() => {});
    return async () => {
      abort.abort();
      await task;
      await pending;
      await daemon.close();
    };
  },
};
