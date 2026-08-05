import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createAssistantMessageEventStream,
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  type Model,
  type SimpleStreamOptions,
  type ToolCall,
} from "@earendil-works/pi-ai";
import * as piCodingAgent from "@earendil-works/pi-coding-agent";
import {
  createAgentSession,
  DefaultResourceLoader,
  SessionManager,
  type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import braintrustPiExtension from "./legacy-processor.ts";

const mockState = vi.hoisted(() => ({
  startSpans: [] as Array<Record<string, unknown>>,
  logSpans: [] as Array<Record<string, unknown>>,
  endSpans: [] as Array<Record<string, unknown>>,
  updateSpans: [] as Array<Record<string, unknown>>,
  initializeCalls: 0,
  flushCalls: 0,
  failInitialize: false,
}));

vi.mock("./client.ts", () => {
  class MockBraintrustClient {
    async initialize(): Promise<void> {
      mockState.initializeCalls += 1;
      if (mockState.failInitialize) {
        throw new Error("simulated Braintrust init failure");
      }
    }

    startSpan(args: Record<string, unknown>): Record<string, unknown> {
      mockState.startSpans.push(args);
      return {
        id: `record-${String(args.spanId)}`,
        spanId: args.spanId,
        rootSpanId: args.rootSpanId,
      };
    }

    logSpan(span: Record<string, unknown> | undefined, event: Record<string, unknown>): void {
      mockState.logSpans.push({ span, event });
    }

    endSpan(span: Record<string, unknown> | undefined, endedAt?: number): void {
      mockState.endSpans.push({ span, endedAt });
    }

    getSpanLink(span: Record<string, unknown> | undefined): string | undefined {
      if (!span) return undefined;
      return `https://www.braintrust.dev/app/test-org/p/pi/logs?oid=${String(span.id)}`;
    }

    async getSpanPermalink(span: Record<string, unknown> | undefined): Promise<string | undefined> {
      return this.getSpanLink(span);
    }

    updateSpan(args: Record<string, unknown>): void {
      mockState.updateSpans.push(args);
    }

    async flush(): Promise<void> {
      mockState.flushCalls += 1;
    }
  }

  return {
    BraintrustClient: MockBraintrustClient,
  };
});

const ENV_KEYS = [
  "HOME",
  "TRACE_TO_BRAINTRUST",
  "BRAINTRUST_API_KEY",
  "BRAINTRUST_STATE_DIR",
  "BRAINTRUST_PROJECT",
  "BRAINTRUST_ORG_NAME",
  "BRAINTRUST_ADDITIONAL_METADATA",
] as const;

const originalEnv = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
const originalProcessCwd = process.cwd();
const tempDirs: string[] = [];

const TEST_API = "pi-extension-test-api" as Api;

const TEST_MODEL: Model<Api> = {
  id: "pi-extension-test-model",
  name: "PI Extension Test Model",
  api: TEST_API,
  provider: "pi-extension-test-provider",
  baseUrl: "https://example.invalid",
  reasoning: false,
  input: ["text"],
  cost: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
  },
  contextWindow: 32_000,
  maxTokens: 4_096,
};

beforeEach(() => {
  mockState.startSpans.length = 0;
  mockState.logSpans.length = 0;
  mockState.endSpans.length = 0;
  mockState.updateSpans.length = 0;
  mockState.initializeCalls = 0;
  mockState.flushCalls = 0;
  mockState.failInitialize = false;

  for (const key of ENV_KEYS) {
    delete process.env[key];
  }
});

afterEach(() => {
  for (const key of ENV_KEYS) {
    const value = originalEnv[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }

  process.chdir(originalProcessCwd);

  while (tempDirs.length > 0) {
    rmSync(tempDirs.pop()!, { recursive: true, force: true });
  }
});

function makeTempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

// Injected by CI (see .github/workflows/ci.yml). When unset (e.g. local dev) we
// assume the currently installed pi is at least as new as any version we branch
// on below.
const PI_COMPAT_VERSION = process.env.PI_COMPAT_VERSION;

function piCompatAtLeast(target: string): boolean {
  if (!PI_COMPAT_VERSION) return true;
  const parse = (v: string) =>
    v
      .split("-")[0]
      .split(".")
      .map((part) => Number.parseInt(part, 10) || 0);
  const actual = parse(PI_COMPAT_VERSION);
  const wanted = parse(target);
  for (let i = 0; i < Math.max(actual.length, wanted.length); i += 1) {
    const a = actual[i] ?? 0;
    const w = wanted[i] ?? 0;
    if (a !== w) return a > w;
  }
  return true;
}

function buildAssistantMessage(model: Model<Api>): AssistantMessage {
  return {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0,
      },
    },
    stopReason: "stop",
    timestamp: Date.now(),
  };
}

function userText(context: Context): string {
  for (let index = context.messages.length - 1; index >= 0; index -= 1) {
    const message = context.messages[index];
    if (!message || message.role !== "user") continue;
    if (typeof message.content === "string") return message.content;
    return message.content
      .filter((part) => part.type === "text")
      .map((part) => part.text)
      .join("\n");
  }
  return "";
}

function hasToolResults(context: Context): boolean {
  return context.messages.some((message) => message.role === "toolResult");
}

function lastToolResult(context: Context) {
  return [...context.messages].reverse().find((message) => message.role === "toolResult");
}

function pushText(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  text: string,
): void {
  output.content.push({ type: "text", text: "" });
  const contentIndex = output.content.length - 1;
  stream.push({ type: "text_start", contentIndex, partial: output });
  const block = output.content[contentIndex];
  if (block?.type === "text") {
    block.text += text;
  }
  stream.push({ type: "text_delta", contentIndex, delta: text, partial: output });
  stream.push({ type: "text_end", contentIndex, content: text, partial: output });
}

function pushToolCall(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  toolCall: ToolCall,
): void {
  output.content.push(toolCall);
  const contentIndex = output.content.length - 1;
  stream.push({ type: "toolcall_start", contentIndex, partial: output });
  stream.push({
    type: "toolcall_delta",
    contentIndex,
    delta: JSON.stringify(toolCall.arguments),
    partial: output,
  });
  stream.push({ type: "toolcall_end", contentIndex, toolCall, partial: output });
}

function streamTestModel(
  model: Model<Api>,
  context: Context,
  _options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();

  queueMicrotask(() => {
    const output = buildAssistantMessage(model);
    stream.push({ type: "start", partial: output });

    const prompt = userText(context);
    if (prompt.includes("deferred-tools")) {
      const toolResult = lastToolResult(context);
      if (toolResult?.role === "toolResult" && toolResult.toolName === "tool_search") {
        pushToolCall(stream, output, {
          type: "toolCall",
          id: "calculator-1",
          name: "Calculator",
          arguments: { expression: "100 + 500" },
        });
        output.stopReason = "toolUse";
      } else if (toolResult?.role === "toolResult" && toolResult.toolName === "Calculator") {
        pushText(stream, output, "600");
        output.stopReason = "stop";
      } else {
        pushToolCall(stream, output, {
          type: "toolCall",
          id: "tool-search-1",
          name: "tool_search",
          arguments: { query: "calculator" },
        });
        output.stopReason = "toolUse";
      }
      stream.push({ type: "done", reason: output.stopReason, message: output });
      stream.end();
      return;
    }

    if (hasToolResults(context)) {
      pushText(stream, output, "parallel tools finished");
      output.stopReason = "stop";
      stream.push({ type: "done", reason: "stop", message: output });
      stream.end();
      return;
    }

    if (prompt.includes("parallel-tools")) {
      pushToolCall(stream, output, {
        type: "toolCall",
        id: "tool-1",
        name: "bash",
        arguments: {
          command: "sleep 0.05; echo slow",
        },
      });
      pushToolCall(stream, output, {
        type: "toolCall",
        id: "tool-2",
        name: "bash",
        arguments: {
          command: "echo fast",
        },
      });
      output.stopReason = "toolUse";
      stream.push({ type: "done", reason: "toolUse", message: output });
      stream.end();
      return;
    }

    pushText(stream, output, "plain response");
    output.stopReason = "stop";
    stream.push({ type: "done", reason: "stop", message: output });
    stream.end();
  });

  return stream;
}

function testHarnessExtension(pi: ExtensionAPI): void {
  pi.registerTool({
    name: "Calculator",
    label: "Calculator",
    description: "Evaluate a simple arithmetic expression.",
    parameters: Type.Object({ expression: Type.String() }),
    async execute(_toolCallId, params) {
      return {
        content: [{ type: "text", text: params.expression === "100 + 500" ? "600" : "0" }],
        details: {},
      };
    },
  });

  pi.registerTool({
    name: "tool_search",
    label: "Tool Search",
    description: "Find and activate tools for a capability.",
    parameters: Type.Object({ query: Type.String() }),
    async execute() {
      const active = pi.getActiveTools();
      if (!active.includes("Calculator")) pi.setActiveTools([...active, "Calculator"]);
      return {
        content: [{ type: "text", text: "Loaded Calculator" }],
        details: { matches: ["Calculator"] },
      };
    },
  });

  pi.registerProvider("pi-extension-test-provider", {
    baseUrl: TEST_MODEL.baseUrl,
    apiKey: "pi-extension-test-key",
    api: TEST_API,
    streamSimple: streamTestModel,
    models: [TEST_MODEL],
  });

  pi.on("session_start", () => {
    pi.setActiveTools(pi.getActiveTools().filter((name) => name !== "Calculator"));
  });

  pi.registerCommand("test-reload", {
    description: "Reload the runtime for integration tests",
    handler: async (_args, ctx) => {
      await ctx.reload();
      return;
    },
  });
}

async function waitForAsyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

interface TestSessionController {
  prompt(text: string): Promise<void>;
  newSession(): Promise<boolean>;
  switchSession(sessionPath: string): Promise<boolean>;
  fork(entryId: string): Promise<{ cancelled: boolean; selectedText: string }>;
  dispose(): Promise<void>;
  readonly sessionFile: string | undefined;
  readonly sessionManager: SessionManager;
}

async function createHarness(options?: {
  rootDir?: string;
  sessionManager?: SessionManager;
  sessionMode?: "inMemory" | "persistent";
  sessionFile?: string;
  sessionsDir?: string;
}) {
  const home = options?.rootDir ?? makeTempDir("pi-extension-home-");
  const cwd = join(home, "workspace");
  const agentDir = join(home, "agent");
  const stateDir = join(home, "state");

  mkdirSync(cwd, { recursive: true });
  mkdirSync(agentDir, { recursive: true });

  process.env.HOME = home;
  process.env.TRACE_TO_BRAINTRUST = "true";
  process.env.BRAINTRUST_API_KEY = "test-key";
  process.env.BRAINTRUST_PROJECT = "pi";
  process.env.BRAINTRUST_ORG_NAME = "test-org";
  process.env.BRAINTRUST_STATE_DIR = stateDir;

  const sessionManager =
    options?.sessionManager ??
    (options?.sessionFile
      ? SessionManager.open(options.sessionFile, options.sessionsDir)
      : options?.sessionMode === "persistent"
        ? SessionManager.create(cwd, options.sessionsDir)
        : SessionManager.inMemory(cwd));

  const compat = piCodingAgent as any;
  if (
    typeof compat.createAgentSessionRuntime === "function" &&
    typeof compat.createAgentSessionServices === "function" &&
    typeof compat.createAgentSessionFromServices === "function"
  ) {
    const runtime = await compat.createAgentSessionRuntime(
      async ({
        cwd: runtimeCwd,
        agentDir: runtimeAgentDir,
        sessionManager: runtimeSessionManager,
        sessionStartEvent,
      }: {
        cwd: string;
        agentDir: string;
        sessionManager: SessionManager;
        sessionStartEvent?: unknown;
      }) => {
        const services = await compat.createAgentSessionServices({
          cwd: runtimeCwd,
          agentDir: runtimeAgentDir,
          resourceLoaderOptions: {
            extensionFactories: [testHarnessExtension, braintrustPiExtension],
          },
        });

        return {
          ...(await compat.createAgentSessionFromServices({
            services,
            sessionManager: runtimeSessionManager,
            sessionStartEvent,
            model: TEST_MODEL,
          })),
          services,
          diagnostics: services.diagnostics,
        };
      },
      {
        cwd,
        agentDir,
        sessionManager,
      },
    );

    const bindRuntimeSession = async (): Promise<void> => {
      await runtime.session.bindExtensions({});
    };

    await bindRuntimeSession();

    const session: TestSessionController = {
      prompt: (text) => runtime.session.prompt(text),
      newSession: async () => {
        const result = await runtime.newSession();
        if (!result.cancelled) {
          await bindRuntimeSession();
        }
        return !result.cancelled;
      },
      switchSession: async (sessionPath) => {
        const result = await runtime.switchSession(sessionPath);
        if (!result.cancelled) {
          await bindRuntimeSession();
        }
        return !result.cancelled;
      },
      fork: async (entryId) => {
        const result = await runtime.fork(entryId);
        if (!result.cancelled) {
          await bindRuntimeSession();
        }
        return {
          cancelled: result.cancelled,
          selectedText: result.selectedText ?? "",
        };
      },
      dispose: async () => {
        await runtime.dispose();
      },
      get sessionFile() {
        return runtime.session.sessionFile;
      },
      get sessionManager() {
        return runtime.session.sessionManager as SessionManager;
      },
    };

    return { agentDir, cwd, session, stateDir };
  }

  // TODO: Remove this legacy fallback once our supported pi compatibility window no
  // longer includes pi <0.65.0, which introduced the session runtime API and the
  // session_start-only post-transition model.
  const resourceLoader = new DefaultResourceLoader({
    cwd,
    agentDir,
    extensionFactories: [testHarnessExtension, braintrustPiExtension],
  });
  await resourceLoader.reload();

  const { session: legacySession } = await createAgentSession({
    cwd,
    agentDir,
    model: TEST_MODEL,
    resourceLoader,
    sessionManager,
  });
  const legacyRuntimeSession = legacySession as typeof legacySession & {
    newSession(): Promise<boolean>;
    switchSession(sessionPath: string): Promise<boolean>;
    fork(entryId: string): Promise<{ cancelled: boolean; selectedText?: string }>;
    sessionManager: SessionManager;
  };

  const session: TestSessionController = {
    prompt: (text) => legacyRuntimeSession.prompt(text),
    newSession: () => legacyRuntimeSession.newSession(),
    switchSession: (sessionPath) => legacyRuntimeSession.switchSession(sessionPath),
    fork: async (entryId) => {
      const result = await legacyRuntimeSession.fork(entryId);
      return {
        cancelled: result.cancelled,
        selectedText: result.selectedText ?? "",
      };
    },
    dispose: async () => {
      legacyRuntimeSession.dispose();
    },
    get sessionFile() {
      return legacyRuntimeSession.sessionFile;
    },
    get sessionManager() {
      return legacyRuntimeSession.sessionManager;
    },
  };

  return { agentDir, cwd, session, stateDir };
}

function rootTaskSpans(): Array<Record<string, unknown>> {
  return mockState.startSpans.filter(
    (span) => span.type === "task" && span.parentSpanId === undefined,
  );
}

describe("braintrustPiExtension integration", () => {
  it("restores persisted trace state when reopening the same pi session", async () => {
    const rootDir = makeTempDir("pi-extension-home-");
    const sessionsDir = makeTempDir("pi-extension-sessions-");
    const first = await createHarness({
      rootDir,
      sessionMode: "persistent",
      sessionsDir,
    });
    await first.session.prompt("create a traced turn");
    const firstSessionFile = first.session.sessionFile;

    expect(firstSessionFile).toBeTruthy();
    expect(rootTaskSpans()).toHaveLength(1);

    await first.session.dispose();
    await waitForAsyncWork();

    const reopened = await createHarness({
      rootDir,
      sessionFile: firstSessionFile!,
      sessionsDir,
    });
    await reopened.session.prompt("resume the same traced session");
    await reopened.session.dispose();
    await waitForAsyncWork();

    expect(rootTaskSpans()).toHaveLength(1);
    expect(
      mockState.startSpans.filter(
        (span) => span.type === "task" && span.parentSpanId !== undefined,
      ),
    ).toHaveLength(2);
  });

  it("keeps one root span across session switch, fork, and resume flows", async () => {
    const sessionsDir = makeTempDir("pi-extension-sessions-");
    const { session } = await createHarness({
      sessionMode: "persistent",
      sessionsDir,
    });

    await session.prompt("session a");
    const sessionAFile = session.sessionFile;

    const switched = await session.newSession();
    expect(switched).toBe(true);
    await session.prompt("session b");
    const sessionBFile = session.sessionFile;

    const forkEntryId = session.sessionManager
      .getBranch()
      .find((entry) => entry.type === "message")?.id;
    expect(forkEntryId).toBeTruthy();
    const forked = await session.fork(forkEntryId!);
    expect(forked.cancelled).toBe(false);
    await session.prompt("session c from fork");
    const sessionCFile = session.sessionFile;

    expect(sessionAFile).toBeTruthy();
    expect(sessionBFile).toBeTruthy();
    expect(sessionCFile).toBeTruthy();
    expect(sessionCFile).not.toBe(sessionBFile);

    const resumed = await session.switchSession(sessionAFile!);
    expect(resumed).toBe(true);
    await session.prompt("back on session a");

    await session.dispose();
    await waitForAsyncWork();

    expect(rootTaskSpans()).toHaveLength(3);
    expect(
      rootTaskSpans().find(
        (span) =>
          (span.metadata as Record<string, unknown> | undefined)?.opened_via === "session_fork",
      ),
    ).toMatchObject({
      metadata: {
        opened_via: "session_fork",
        parent_session_file: sessionBFile,
      },
    });
  });

  it("preserves pi's parallel tool end ordering when creating tool spans", async () => {
    const { session } = await createHarness();

    await session.prompt("parallel-tools");
    await session.dispose();
    await waitForAsyncWork();

    const llmSpans = mockState.startSpans.filter((span) => span.type === "llm");
    const toolSpans = mockState.startSpans.filter((span) => span.type === "tool");
    const firstLlmSpanId = llmSpans[0]?.spanId;

    expect(toolSpans).toHaveLength(2);
    // pi < 0.68.1 emits `tool_execution_end` in assistant source order, so the
    // extension logs tool spans as [tool-1, tool-2]. Starting with pi 0.68.1 the
    // agent emits parallel tool completions eagerly (completion order), so the
    // fast `tool-2` finishes before the slow `tool-1` and spans are logged as
    // [tool-2, tool-1]. See pi-coding-agent changelog 0.68.1 / issue #3503.
    // TODO: drop the pi < 0.68.1 branch once we stop testing against it.
    const expectedToolCallIdOrder = piCompatAtLeast("0.68.1")
      ? ["tool-2", "tool-1"]
      : ["tool-1", "tool-2"];
    expect(
      toolSpans.map((span) => (span.metadata as Record<string, unknown> | undefined)?.tool_call_id),
    ).toEqual(expectedToolCallIdOrder);
    expect(toolSpans.map((span) => span.parentSpanId)).toEqual([firstLlmSpanId, firstLlmSpanId]);

    const firstLlmLog = mockState.logSpans.find(
      (entry) => (entry.span as { spanId?: unknown } | undefined)?.spanId === firstLlmSpanId,
    );
    const firstLlmEvent = firstLlmLog?.event as Record<string, unknown> | undefined;
    const firstLlmMetrics = firstLlmEvent?.metrics as Record<string, unknown> | undefined;
    expect(firstLlmMetrics?.time_to_first_token).toEqual(expect.any(Number));
  });

  it("traces tools activated during a deferred tool-loading turn", async () => {
    const { session } = await createHarness();

    await session.prompt("deferred-tools");
    await session.dispose();
    await waitForAsyncWork();

    const toolSpans = mockState.startSpans.filter((span) => span.type === "tool");
    expect(toolSpans.map((span) => span.name)).toEqual(["tool_search", "Calculator"]);

    if (piCompatAtLeast("0.80.7")) {
      expect(toolSpans[0]?.metadata).toMatchObject({
        activated_tools: ["Calculator"],
        activated_tool_count: 1,
      });
      const llmSpans = mockState.startSpans.filter((span) => span.type === "llm");
      expect(llmSpans[1]?.metadata).toMatchObject({
        activated_tools: ["Calculator"],
      });
    }
  });

  it("stops tracing new work after Braintrust initialization fails", async () => {
    mockState.failInitialize = true;

    const { session } = await createHarness();
    await waitForAsyncWork();
    await session.prompt("plain-response");
    await session.dispose();
    await waitForAsyncWork();

    expect(mockState.initializeCalls).toBe(1);
    expect(mockState.startSpans).toEqual([]);
    expect(mockState.flushCalls).toBe(0);
  });
});
