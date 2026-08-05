export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue | undefined;
}

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface ConfigIssue {
  path: string;
  message: string;
  severity: "error" | "warning";
}

export interface TraceConfig {
  enabled: boolean;
  apiKey: string;
  apiUrl?: string;
  appUrl: string;
  orgName?: string;
  projectName: string;
  debug: boolean;
  logFile?: string;
  stateDir: string;
  additionalMetadata: JsonObject;
  parentSpanId?: string;
  rootSpanId?: string;
  showUi: boolean;
  showTraceLink: boolean;
  configIssues: ConfigIssue[];
}

export interface Logger {
  filePath: string;
  debug(message: string, data?: unknown): void;
  info(message: string, data?: unknown): void;
  warn(message: string, data?: unknown): void;
  error(message: string, data?: unknown): void;
  flush(): Promise<void>;
}

export interface PersistedSessionState {
  rootSpanId: string;
  rootSpanRecordId?: string;
  traceRootSpanId?: string;
  parentSpanId?: string;
  traceUrl?: string;
  startedAt: number;
  totalTurns?: number;
  totalToolCalls?: number;
  lastSeenAt?: number;
  sessionFile?: string;
}

export interface StateStore {
  get(sessionKey: string): PersistedSessionState | undefined;
  set(sessionKey: string, value: PersistedSessionState): PersistedSessionState;
  patch(sessionKey: string, patch: Partial<PersistedSessionState>): PersistedSessionState;
  delete(sessionKey: string): void;
  schedulePersist(delayMs?: number): void;
  flush(): Promise<void>;
}

export interface TextContentLike {
  type: "text";
  text?: string;
}

export interface ThinkingContentLike {
  type: "thinking";
  thinking?: string;
  thinkingSignature?: string;
  redacted?: boolean;
}

export interface ToolCallContentLike {
  type: "toolCall";
  id: string;
  name?: string;
  arguments?: Record<string, unknown>;
}

export interface ImageContentLike {
  type: "image";
  mimeType?: string;
}

export type ContentPartLike =
  | TextContentLike
  | ThinkingContentLike
  | ToolCallContentLike
  | ImageContentLike
  | {
      type?: string;
      [key: string]: unknown;
    };

export interface ImageLike {
  mimeType?: string;
  source?: {
    mediaType?: string;
  };
}

export interface UsageLike {
  input?: number;
  output?: number;
  cacheRead?: number;
  cacheWrite?: number;
  cacheWrite1h?: number;
  reasoning?: number;
  totalTokens?: number;
  cost?: {
    input?: number;
    output?: number;
    cacheRead?: number;
    cacheWrite?: number;
    total?: number;
  };
}

export interface UserMessageLike {
  role: "user";
  content?: unknown;
}

export interface AssistantMessageLike {
  role: "assistant";
  content?: ContentPartLike[];
  api?: string;
  provider?: string;
  model?: string;
  responseModel?: string;
  responseId?: string;
  usage?: UsageLike;
  stopReason?: string;
  errorMessage?: string;
  timestamp?: number;
}

export interface ToolResultMessageLike {
  role: "toolResult";
  toolCallId?: string;
  toolName?: string;
  content?: unknown;
  details?: unknown;
  isError?: boolean;
}

export type AgentMessageLike =
  | UserMessageLike
  | AssistantMessageLike
  | ToolResultMessageLike
  | {
      role?: string;
      content?: unknown;
    };

export interface NormalizedUserMessage {
  role: "user";
  content: string;
}

export interface NormalizedAssistantMessage {
  role: "assistant";
  content: string;
  tool_calls?: Array<{
    id?: string;
    type: "function";
    function: {
      name?: string;
      arguments: string;
    };
  }>;
  reasoning?: Array<{
    id: string;
    content: string;
  }>;
}

export interface NormalizedToolMessage {
  role: "tool";
  content: string;
  tool_call_id?: string;
  name?: string;
  is_error: boolean;
}

export type NormalizedAgentMessage =
  | NormalizedUserMessage
  | NormalizedAssistantMessage
  | NormalizedToolMessage;
