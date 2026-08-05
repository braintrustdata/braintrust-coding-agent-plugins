import * as childProcess from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { appendFile, mkdir } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";
import type {
  AgentMessageLike,
  AssistantMessageLike,
  ContentPartLike,
  ImageLike,
  NormalizedAgentMessage,
  NormalizedAssistantMessage,
  ToolResultMessageLike,
} from "./types.ts";

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function ensureDir(path: string | undefined): void {
  if (!path) return;
  mkdirSync(path, { recursive: true });
}

export function toUnixSeconds(timestampMs: number): number {
  return timestampMs / 1000;
}

export function generateUuid(): string {
  return randomUUID();
}

export function sessionKeyFor(
  sessionFile: string | undefined,
  sessionId: string | undefined,
  cwd = process.cwd(),
): string {
  if (sessionFile) return `file:${sessionFile}`;
  const projectKey = shortHash(resolve(cwd));
  return `ephemeral:${projectKey}:${sessionId ?? generateUuid()}`;
}

export function coerceToString(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return undefined;
}

export function shortHash(value: unknown): string {
  return createHash("sha256").update(safeStringify(value)).digest("hex").slice(0, 12);
}

export function parseBoolean(value: unknown, fallback = false): boolean {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const normalized = coerceToString(value)?.trim().toLowerCase();
  if (!normalized) return fallback;
  if (["true", "1", "yes", "y", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

export function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

export function safeStringify(value: unknown): string {
  const primitive = coerceToString(value);
  if (primitive !== undefined) return primitive;
  try {
    return JSON.stringify(value) ?? "";
  } catch {
    return "";
  }
}

export function truncateString(value: unknown, maxLength = 10_000): string {
  const text = typeof value === "string" ? value : safeStringify(value);
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}… [truncated ${text.length - maxLength} chars]`;
}

export function truncateValue(value: unknown, maxLength = 10_000): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") return truncateString(value, maxLength);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.map((item) => truncateValue(item, maxLength));
  if (isPlainObject(value)) {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      result[key] = truncateValue(item, maxLength);
    }
    return result;
  }
  return truncateString(safeStringify(value), maxLength);
}

function contentPartToText(item: ContentPartLike): string {
  if (item.type === "text") {
    return typeof item.text === "string" ? item.text : "";
  }

  if (item.type === "thinking") {
    return item.redacted
      ? "[thinking redacted]"
      : typeof item.thinking === "string"
        ? item.thinking
        : "";
  }

  if (item.type === "image") {
    return `[image:${typeof item.mimeType === "string" ? item.mimeType : "unknown"}]`;
  }

  if (item.type === "toolCall") {
    const toolName = typeof item.name === "string" ? item.name : "unknown";
    return `[toolCall:${toolName}] ${safeStringify(item.arguments ?? {})}`;
  }

  return `[${typeof item.type === "string" ? item.type : "content"}] ${safeStringify(item)}`;
}

export function contentToText(content: unknown): string {
  if (content === null || content === undefined) return "";
  if (typeof content === "string") return truncateString(content);
  if (!Array.isArray(content)) return truncateString(safeStringify(content));

  const lines: string[] = [];
  for (const item of content) {
    if (!isPlainObject(item)) {
      lines.push(safeStringify(item));
      continue;
    }

    lines.push(contentPartToText(item as ContentPartLike));
  }

  return truncateString(lines.filter(Boolean).join("\n"));
}

export function normalizeUserContent(content: unknown): string {
  return contentToText(content);
}

export function normalizeAssistantMessage(
  message: AssistantMessageLike,
): NormalizedAssistantMessage {
  const textParts: string[] = [];
  const thinkingParts: string[] = [];
  const toolCalls: NonNullable<NormalizedAssistantMessage["tool_calls"]> = [];

  for (const part of message.content ?? []) {
    if (!isPlainObject(part)) continue;

    if (part.type === "text") {
      textParts.push(typeof part.text === "string" ? part.text : "");
      continue;
    }

    if (part.type === "thinking") {
      if (part.redacted) {
        thinkingParts.push("[thinking redacted]");
      } else if (typeof part.thinking === "string" && part.thinking.length > 0) {
        thinkingParts.push(part.thinking);
      }
      continue;
    }

    if (part.type === "toolCall") {
      toolCalls.push({
        id: typeof part.id === "string" ? part.id : undefined,
        type: "function",
        function: {
          name: typeof part.name === "string" ? part.name : undefined,
          arguments: truncateString(safeStringify(part.arguments ?? {})),
        },
      });
    }
  }

  const normalized: NormalizedAssistantMessage = {
    role: "assistant",
    content: truncateString(textParts.join("\n")),
  };

  if (toolCalls.length > 0) normalized.tool_calls = toolCalls;
  if (thinkingParts.length > 0) {
    normalized.reasoning = [{ id: "thinking", content: truncateString(thinkingParts.join("\n")) }];
  }

  return normalized;
}

export function normalizeAgentMessage(
  message: AgentMessageLike,
): NormalizedAgentMessage | undefined {
  if (!message || typeof message !== "object" || !("role" in message)) return undefined;

  if (message.role === "user") {
    return {
      role: "user",
      content: normalizeUserContent(message.content),
    };
  }

  if (message.role === "assistant") {
    return normalizeAssistantMessage(message as AssistantMessageLike);
  }

  if (message.role === "toolResult") {
    const toolMessage = message as ToolResultMessageLike;
    return {
      role: "tool",
      content: contentToText(toolMessage.content),
      tool_call_id: toolMessage.toolCallId,
      name: toolMessage.toolName,
      is_error: Boolean(toolMessage.isError),
    };
  }

  return undefined;
}

export function normalizeContextMessages(
  messages: readonly AgentMessageLike[] | undefined,
): NormalizedAgentMessage[] {
  return (messages ?? [])
    .map(normalizeAgentMessage)
    .filter((message): message is NormalizedAgentMessage => Boolean(message));
}

export function normalizeToolResult(result: unknown): unknown {
  if (result === null || result === undefined) return undefined;
  if (typeof result === "string") return truncateString(result);

  if (isPlainObject(result)) {
    const normalized: Record<string, unknown> = {};

    if ("content" in result && result.content !== undefined) {
      normalized.content = contentToText(result.content);
    }

    if ("details" in result && result.details !== undefined) {
      normalized.details = truncateValue(result.details);
    }

    if ("isError" in result) {
      normalized.isError = Boolean(result.isError);
    }

    if (Object.keys(normalized).length > 0) return normalized;
  }

  return truncateValue(result);
}

export function extractErrorText(value: unknown, fallback: string | undefined): string | undefined {
  if (!value) return fallback;
  if (typeof value === "string") return truncateString(value);
  if (isPlainObject(value)) {
    if (typeof value.errorMessage === "string") return truncateString(value.errorMessage);
    if (typeof value.message === "string") return truncateString(value.message);
    if (Array.isArray(value.content)) {
      const text = contentToText(value.content);
      if (text) return text;
    }
  }
  return fallback;
}

export function formatToolSpanName(toolName: string, args: unknown = {}): string {
  const objectArgs = isPlainObject(args) ? args : {};
  const pathLike =
    objectArgs.path ??
    objectArgs.file ??
    objectArgs.filePath ??
    objectArgs.target ??
    objectArgs.sessionDir;
  if (typeof pathLike === "string") {
    return `${toolName}: ${basename(pathLike)}`;
  }

  if (toolName === "bash" && typeof objectArgs.command === "string") {
    const command = objectArgs.command.replace(/\s+/g, " ").trim();
    return `bash: ${truncateString(command, 60)}`;
  }

  return toolName;
}

export function buildTurnInput(
  prompt: string,
  images: readonly ImageLike[] | undefined = [],
): string {
  const input = [prompt ?? ""];
  for (const image of images ?? []) {
    const type = image?.source?.mediaType ?? image?.mimeType ?? "image";
    input.push(`[${type}]`);
  }
  return truncateString(input.filter(Boolean).join("\n"));
}

const gitRemoteRepoCache = new Map<string, string | undefined>();

export interface GitMetadata {
  git_origin_url?: string;
  git_branch?: string;
  git_commit_sha?: string;
}

export function redactGitRemoteUrl(remoteUrl: string): string {
  const trimmed = remoteUrl.trim();
  if (!trimmed) return trimmed;

  try {
    const parsed = new URL(trimmed);
    if (parsed.username || parsed.password) {
      parsed.username = "";
      parsed.password = "";
    }
    return parsed.toString();
  } catch {
    return trimmed;
  }
}

function runGit(cwd: string, args: string[]): string | undefined {
  try {
    const result = childProcess.spawnSync("git", ["-C", cwd, ...args], {
      encoding: "utf8",
      env: { ...process.env, GIT_OPTIONAL_LOCKS: "0" },
      timeout: 500,
      windowsHide: true,
    });

    if (result.status === 0 && typeof result.stdout === "string") {
      const value = result.stdout.trim();
      return value.length > 0 ? value : undefined;
    }
  } catch {
    return undefined;
  }

  return undefined;
}

function parseGitRemoteRepo(remoteUrl: string): string | undefined {
  const trimmed = remoteUrl.trim();
  if (!trimmed) return undefined;

  const scpLikeMatch = trimmed.match(/^[^@\s]+@[^:\s]+:(.+)$/);
  const path = scpLikeMatch
    ? scpLikeMatch[1]
    : (() => {
        try {
          return new URL(trimmed).pathname;
        } catch {
          return undefined;
        }
      })();

  if (!path) return undefined;

  const segments = path
    .replace(/^\/+/, "")
    .replace(/\.git$/i, "")
    .split("/")
    .filter(Boolean);

  if (segments.length < 2) return undefined;
  return `${segments.at(-2)}/${segments.at(-1)}`;
}

export function gitMetadataForCwd(cwd: string | undefined): GitMetadata {
  if (!cwd) return {};

  const origin = runGit(cwd, ["remote", "get-url", "origin"]);
  const branch = runGit(cwd, ["symbolic-ref", "--quiet", "--short", "HEAD"]);
  const commit = runGit(cwd, ["rev-parse", "HEAD"]);

  return {
    ...(origin ? { git_origin_url: redactGitRemoteUrl(origin) } : {}),
    ...(branch ? { git_branch: branch } : {}),
    ...(commit ? { git_commit_sha: commit } : {}),
  };
}

export function repoSlugForCwd(cwd: string): string | undefined {
  const resolvedCwd = cwd || process.cwd();
  if (gitRemoteRepoCache.has(resolvedCwd)) {
    return gitRemoteRepoCache.get(resolvedCwd);
  }

  let repo: string | undefined;

  const origin = runGit(resolvedCwd, ["remote", "get-url", "origin"]);
  if (origin) repo = parseGitRemoteRepo(origin);

  gitRemoteRepoCache.set(resolvedCwd, repo);
  return repo;
}

export function rootSpanName(cwd: string): string {
  return `pi: ${repoSlugForCwd(cwd) ?? basename(cwd || process.cwd())}`;
}

export async function writeJsonLog(
  filePath: string,
  level: string,
  message: string,
  data?: unknown,
): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
  await appendFile(
    filePath,
    `${JSON.stringify({ timestamp: new Date().toISOString(), level, message, data: truncateValue(data) })}\n`,
    "utf8",
  );
}
