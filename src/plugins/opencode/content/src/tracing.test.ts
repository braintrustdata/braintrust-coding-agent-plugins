/**
 * Tests for tracing hooks
 *
 * Uses real OpenCode SDK events
 */

import { afterEach, describe, expect, it } from "bun:test"
import { execFileSync } from "node:child_process"
import * as fs from "node:fs"
import * as os from "node:os"
import * as path from "node:path"
import { TestClock } from "./clock"
import { EventProcessor, extractToolOutput } from "./event-processor"
import { FileLogger } from "./file-logger"
import { replayLogFile, replayLogFileToTree } from "./replay"
import { spansToTree, TestSpanCollector } from "./span-sink"
import {
  assertEventsProduceTree,
  chatMessage,
  childSessionCreated,
  eventsToTree,
  messageCompleted,
  reasoningPart,
  session,
  sessionCreated,
  sessionDeleted,
  sessionError,
  sessionIdle,
  systemTransform,
  textPart,
  toolCallCompletedPart,
  toolCallErrorPart,
  toolCallPart,
  toolExecute,
} from "./test-helpers"
import { createTracingHooks } from "./tracing"

const tempDirs: string[] = []

afterEach(() => {
  while (tempDirs.length > 0) {
    fs.rmSync(tempDirs.pop()!, { recursive: true, force: true })
  }
})

function expectUnixSecondsTimestamp(value: number | undefined): void {
  expect(value).toBeDefined()
  expect(value!).toBeGreaterThan(1_000_000_000)
  expect(value!).toBeLessThan(10_000_000_000)
}

function makeGitRepo(): { dir: string; commit: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-root-git-"))
  tempDirs.push(dir)
  execFileSync("git", ["init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir })
  execFileSync("git", ["config", "user.name", "Test User"], { cwd: dir })
  fs.writeFileSync(path.join(dir, "README.md"), "hello\n")
  execFileSync("git", ["add", "README.md"], { cwd: dir })
  execFileSync("git", ["commit", "-m", "init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["branch", "-M", "main"], { cwd: dir })
  execFileSync("git", ["remote", "add", "origin", "https://token@github.com/acme/app.git"], {
    cwd: dir,
  })
  const commit = execFileSync("git", ["rev-parse", "HEAD"], { cwd: dir, encoding: "utf8" }).trim()
  return { dir, commit }
}

describe("Event to Span Transformation", () => {
  it("session -> turn -> llm", async () => {
    const sessionId = "ses_1"
    const messageId = "msg_1"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hello, world!"),
        textPart(sessionId, messageId, "Hi there!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 10, completion_tokens: 5, tokens: 15 },
              },
            ],
          },
        ],
      },
    )
  })

  it("tracks OpenCode cache read/write tokens on LLM spans", async () => {
    const sessionId = "ses_cache"
    const messageId = "msg_cache"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Use the cached context"),
        textPart(sessionId, messageId, "Done."),
        messageCompleted(sessionId, messageId, {
          tokens: { input: 10, output: 5, cache: { read: 3, write: 2 } },
        }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: {
                  prompt_tokens: 15,
                  completion_tokens: 5,
                  tokens: 20,
                  prompt_cached_tokens: 3,
                  prompt_cache_creation_tokens: 2,
                },
              },
            ],
          },
        ],
      },
    )
  })

  it("stores modelID on turn metadata from chat.message hook", async () => {
    const sessionId = "ses_turn_model"
    const messageId = "msg_turn_model"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hello, world!", {
          providerID: "anthropic",
          modelID: "claude-3-haiku",
        }),
        textPart(sessionId, messageId, "Hi there!"),
        messageCompleted(sessionId, messageId, {
          tokens: { input: 10, output: 5 },
          model: { providerID: "anthropic", modelID: "claude-3-haiku" },
        }),
        sessionIdle(sessionId),
      ),
    )

    expect(tree).not.toBeNull()
    const turn = tree?.children[0]
    expect(turn?.name).toBe("Turn 1")
    expect(turn?.metadata?.model).toBe("claude-3-haiku")
  })

  it("session -> multiple turns", async () => {
    const sessionId = "ses_multi"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        // Turn 1
        chatMessage("What is 2+2?"),
        textPart(sessionId, "msg_1", "2+2 equals 4."),
        messageCompleted(sessionId, "msg_1", { tokens: { input: 8, output: 6 } }),
        sessionIdle(sessionId),
        // Turn 2
        chatMessage("What about 3+3?"),
        textPart(sessionId, "msg_2", "3+3 equals 6."),
        messageCompleted(sessionId, "msg_2", { tokens: { input: 12, output: 7 } }),
        sessionIdle(sessionId),
        // Turn 3
        chatMessage("And 4+4?"),
        textPart(sessionId, "msg_3", "4+4 equals 8."),
        messageCompleted(sessionId, "msg_3", { tokens: { input: 15, output: 8 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 8, completion_tokens: 6, tokens: 14 },
              },
            ],
          },
          {
            span_attributes: { name: "Turn 2", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 12, completion_tokens: 7, tokens: 19 },
              },
            ],
          },
          {
            span_attributes: { name: "Turn 3", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 15, completion_tokens: 8, tokens: 23 },
              },
            ],
          },
        ],
      },
    )
  })

  it("session -> turn -> tool use (read)", async () => {
    const sessionId = "ses_tool"
    const messageId = "msg_1"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the config file"),
        // LLM decides to call read tool
        toolCallPart(sessionId, messageId, "call_1", "read", {
          filePath: "/home/user/project/src/config.ts",
        }),
        toolExecute(
          "call_1",
          "read",
          "/home/user/project/src/config.ts",
          { filePath: "/home/user/project/src/config.ts" },
          "export const config = { debug: true }",
        ),
        textPart(
          sessionId,
          messageId,
          "I've read the config file. It exports a config object with debug: true.",
        ),
        messageCompleted(sessionId, messageId, { tokens: { input: 20, output: 15 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "read: config.ts", type: "tool" },
              },
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 20, completion_tokens: 15, tokens: 35 },
              },
            ],
          },
        ],
      },
    )
  })

  it("normalizes native skill tool calls as skill load spans", async () => {
    const sessionId = "ses_skill_tool"
    const messageId = "msg_1"
    const skillSession = session(
      sessionId,
      sessionCreated(sessionId),
      chatMessage("Use the review skill"),
      toolCallPart(sessionId, messageId, "call_skill", "skill", { name: "review" }),
      toolExecute("call_skill", "skill", "review", { name: "review" }, "Loaded skill review"),
      textPart(sessionId, messageId, "Loaded the review skill."),
      messageCompleted(sessionId, messageId, { tokens: { input: 20, output: 15 } }),
      sessionIdle(sessionId),
    )

    await assertEventsProduceTree(skillSession, {
      span_attributes: { name: "OpenCode: test-project", type: "task" },
      children: [
        {
          span_attributes: { name: "Turn 1", type: "task" },
          children: [
            {
              span_attributes: { name: "skill: review", type: "tool" },
              metadata: {
                tool_name: "skill",
                tool_kind: "skill",
                call_id: "call_skill",
                skill_name: "review",
              },
            },
            {
              span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
            },
          ],
        },
      ],
    })

    const tree = await eventsToTree(skillSession)
    const turn = tree?.children[0]
    const skillSpan = turn?.children.find((child) => child.name === "skill: review")
    expect(turn?.metadata?.loaded_skill_names).toBeUndefined()
    expect(turn?.metadata?.loaded_skills).toBeUndefined()
    expect(skillSpan?.metadata?.skill_load_trigger).toBeUndefined()
  })

  it("marks /skills skill tool calls as explicit", async () => {
    const sessionId = "ses_explicit_skill_tool"
    const messageId = "msg_1"
    const skillSession = session(
      sessionId,
      sessionCreated(sessionId),
      chatMessage("/skills review"),
      toolCallPart(sessionId, messageId, "call_skill", "skill", { name: "review" }),
      toolExecute("call_skill", "skill", "review", { name: "review" }, "Loaded skill review"),
      textPart(sessionId, messageId, "Loaded the review skill."),
      messageCompleted(sessionId, messageId, { tokens: { input: 20, output: 15 } }),
      sessionIdle(sessionId),
    )

    const tree = await eventsToTree(skillSession)
    const turn = tree?.children[0]
    const skillSpan = turn?.children.find((child) => child.name === "skill: review")

    expect(turn?.metadata?.loaded_skill_names).toEqual(["review"])
    expect(turn?.metadata?.loaded_skills).toEqual([{ name: "review" }])
    expect(skillSpan?.metadata?.skill_load_trigger).toBe("explicit")
  })

  it("backfills bare /skills commands from the selected skill tool call", async () => {
    const sessionId = "ses_bare_explicit_skill_tool"
    const messageId = "msg_1"
    const skillSession = session(
      sessionId,
      sessionCreated(sessionId),
      chatMessage("/skills"),
      toolCallPart(sessionId, messageId, "call_skill", "skill", { name: "review" }),
      toolExecute("call_skill", "skill", "review", { name: "review" }, "Loaded skill review"),
      textPart(sessionId, messageId, "Loaded the review skill."),
      messageCompleted(sessionId, messageId, { tokens: { input: 20, output: 15 } }),
      sessionIdle(sessionId),
    )

    const tree = await eventsToTree(skillSession)
    const turn = tree?.children[0]
    const skillSpan = turn?.children.find((child) => child.name === "skill: review")

    expect(turn?.metadata?.loaded_skill_names).toEqual(["review"])
    expect(turn?.metadata?.loaded_skills).toEqual([{ name: "review" }])
    expect(skillSpan?.metadata?.skill_load_trigger).toBe("explicit")
  })

  it("session -> turn -> multiple tool calls", async () => {
    const sessionId = "ses_multi_tool"
    const messageId = "msg_1"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the config and then edit it"),
        // LLM calls read first
        toolCallPart(sessionId, messageId, "call_1", "read", { filePath: "/project/config.ts" }),
        toolExecute(
          "call_1",
          "read",
          "/project/config.ts",
          { filePath: "/project/config.ts" },
          "export const debug = false",
        ),
        // LLM calls edit
        toolCallPart(sessionId, messageId, "call_2", "edit", {
          filePath: "/project/config.ts",
          oldString: "false",
          newString: "true",
        }),
        toolExecute(
          "call_2",
          "edit",
          "/project/config.ts",
          { filePath: "/project/config.ts", oldString: "false", newString: "true" },
          "Edit applied successfully",
        ),
        textPart(sessionId, messageId, "Done! I changed debug from false to true."),
        messageCompleted(sessionId, messageId, { tokens: { input: 30, output: 12 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              // Tool spans come first (processed before LLM span is created)
              {
                span_attributes: { name: "read: config.ts", type: "tool" },
              },
              {
                span_attributes: { name: "edit: config.ts", type: "tool" },
              },
              // LLM span comes last (created when messageCompleted is processed)
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 30, completion_tokens: 12, tokens: 42 },
              },
            ],
          },
        ],
      },
    )
  })
})

describe("Metric timestamps use Unix seconds", () => {
  it("converts EventProcessor span metrics from milliseconds to seconds", async () => {
    const sessionId = "ses_seconds"
    const messageId = "msg_seconds"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the config"),
        toolCallPart(sessionId, messageId, "call_seconds", "read", { filePath: "/config.ts" }),
        toolExecute(
          "call_seconds",
          "read",
          "/config.ts",
          { filePath: "/config.ts" },
          "export const debug = true",
        ),
        textPart(sessionId, messageId, "Done."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
        sessionDeleted(sessionId),
      ),
    )

    expect(tree).not.toBeNull()
    const turn = tree?.children[0]
    const toolSpan = turn?.children.find((c) => c.type === "tool")
    const llmSpan = turn?.children.find((c) => c.type === "llm")

    expectUnixSecondsTimestamp(tree?.metrics?.start)
    expectUnixSecondsTimestamp(tree?.metrics?.end)
    expectUnixSecondsTimestamp(turn?.metrics?.start)
    expectUnixSecondsTimestamp(turn?.metrics?.end)
    expectUnixSecondsTimestamp(toolSpan?.metrics?.start)
    expectUnixSecondsTimestamp(toolSpan?.metrics?.end)
    expectUnixSecondsTimestamp(llmSpan?.metrics?.start)
    expectUnixSecondsTimestamp(llmSpan?.metrics?.end)
  })

  it("adds minimal git attribution metadata to production root spans", async () => {
    const repo = makeGitRepo()
    const sessionId = "ses_hooks_git"
    const collector = new TestSpanCollector()
    const hooks = createTracingHooks(
      collector,
      {
        client: {
          app: {
            log: async () => undefined,
          },
        },
        worktree: repo.dir,
        directory: repo.dir,
      } as any,
      {
        apiKey: "",
        apiUrl: "https://api.braintrust.dev",
        appUrl: "https://www.braintrust.dev",
        projectName: "test-project",
        tracingEnabled: true,
        debug: false,
        enableTools: false,
      },
    )

    const eventHook = hooks.event as (args: { event: unknown }) => Promise<void>
    await eventHook({ event: sessionCreated(sessionId) })

    expect(collector.getSpans()[0]?.metadata).toMatchObject({
      git_origin_url: "https://github.com/acme/app.git",
      git_branch: "main",
      git_commit_sha: repo.commit,
    })
  })

  it("converts createTracingHooks span metrics from milliseconds to seconds", async () => {
    const sessionId = "ses_hooks_seconds"
    const messageId = "msg_hooks_seconds"
    const collector = new TestSpanCollector()
    const hooks = createTracingHooks(
      collector,
      {
        client: {
          app: {
            log: async () => undefined,
          },
        },
        worktree: "/tmp/test-project",
        directory: "/tmp/test-project",
      } as any,
      {
        apiKey: "",
        apiUrl: "https://api.braintrust.dev",
        appUrl: "https://www.braintrust.dev",
        projectName: "test-project",
        tracingEnabled: true,
        enableTools: true,
        debug: false,
      },
    )

    const eventHook = hooks.event as (args: { event: unknown }) => Promise<void>
    const chatMessageHook = hooks["chat.message"] as (
      input: unknown,
      output: unknown,
    ) => Promise<void>
    const toolBeforeHook = hooks["tool.execute.before"] as (
      input: unknown,
      output: unknown,
    ) => Promise<void>
    const toolAfterHook = hooks["tool.execute.after"] as (
      input: unknown,
      output: unknown,
    ) => Promise<void>

    await eventHook({ event: sessionCreated(sessionId) })
    await chatMessageHook(
      {
        sessionID: sessionId,
        agent: "assistant",
        model: { providerID: "anthropic", modelID: "claude-3-haiku" },
      },
      { parts: [{ type: "text", text: "Read the config" }] },
    )
    await toolBeforeHook(
      { tool: "read", sessionID: sessionId, callID: "call_hooks_seconds" },
      { args: { filePath: "/config.ts" } },
    )
    await toolAfterHook(
      { tool: "read", sessionID: sessionId, callID: "call_hooks_seconds" },
      { output: "export const debug = true", title: "/config.ts", metadata: {} },
    )
    await eventHook({ event: textPart(sessionId, messageId, "Done.") })
    await eventHook({
      event: messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
    })
    await eventHook({ event: sessionIdle(sessionId) })
    await eventHook({ event: sessionDeleted(sessionId) })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    const turn = tree?.children[0]
    const toolSpan = turn?.children.find((c) => c.type === "tool")
    const llmSpan = turn?.children.find((c) => c.type === "llm")

    expectUnixSecondsTimestamp(tree?.metrics?.start)
    expectUnixSecondsTimestamp(tree?.metrics?.end)
    expectUnixSecondsTimestamp(turn?.metrics?.start)
    expectUnixSecondsTimestamp(turn?.metrics?.end)
    expectUnixSecondsTimestamp(toolSpan?.metrics?.start)
    expectUnixSecondsTimestamp(toolSpan?.metrics?.end)
    expectUnixSecondsTimestamp(llmSpan?.metrics?.start)
    expectUnixSecondsTimestamp(llmSpan?.metrics?.end)
  })
})

describe("Session Errors", () => {
  it("session error during turn closes spans with error", async () => {
    const sessionId = "ses_error"
    const messageId = "msg_1"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Do something"),
        textPart(sessionId, messageId, "Working on it..."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        // Error occurs during session
        sessionError(sessionId, "ApiError", "Rate limit exceeded"),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        error: /Rate limit exceeded[\s\S]*type: ApiError/,
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            error: /Rate limit exceeded[\s\S]*type: ApiError/,
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
              },
            ],
          },
        ],
      },
    )
  })

  it("session error before any turn still closes root span with error", async () => {
    const sessionId = "ses_error_early"

    await assertEventsProduceTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        // Error occurs immediately, before any chat message
        sessionError(sessionId, "AuthError", "Invalid API key"),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        error: /Invalid API key[\s\S]*type: AuthError/,
        children: [],
      },
    )
  })
})

describe("Subagents (Child Sessions)", () => {
  it("subagent creates child span linked to parent trace", async () => {
    const parentSessionId = "ses_parent"
    const childSessionId = "ses_child"

    await assertEventsProduceTree(
      session(
        parentSessionId,
        // Parent session starts
        sessionCreated(parentSessionId),
        chatMessage("Search the codebase"),
        // Parent LLM response triggers subagent
        textPart(parentSessionId, "msg_1", "Let me search for that..."),
        messageCompleted(parentSessionId, "msg_1", { tokens: { input: 10, output: 5 } }),
        // Child session (subagent) created
        childSessionCreated(childSessionId, parentSessionId, "Find files (@explore subagent)"),
        // Child does some work (note: sessionID specified to target child session)
        chatMessage("Searching...", { sessionID: childSessionId }),
        textPart(childSessionId, "msg_child_1", "Found 3 files"),
        messageCompleted(childSessionId, "msg_child_1", { tokens: { input: 8, output: 4 } }),
        // Child session completes
        sessionIdle(childSessionId),
        // Parent continues
        sessionIdle(parentSessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
              },
              // Subagent appears as child of the turn
              {
                span_attributes: { name: "explore: Find files", type: "task" },
                children: [
                  {
                    span_attributes: { name: "Turn 1", type: "task" },
                    children: [
                      {
                        span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    )
  })

  it("subagent with default title format", async () => {
    const parentSessionId = "ses_parent2"
    const childSessionId = "ses_child2"

    await assertEventsProduceTree(
      session(
        parentSessionId,
        sessionCreated(parentSessionId),
        chatMessage("Do a task"),
        textPart(parentSessionId, "msg_1", "Starting task..."),
        messageCompleted(parentSessionId, "msg_1", { tokens: { input: 5, output: 3 } }),
        // Child with custom title format
        childSessionCreated(
          childSessionId,
          parentSessionId,
          "Research the topic (@general subagent)",
        ),
        chatMessage("Researching...", { sessionID: childSessionId }),
        textPart(childSessionId, "msg_c1", "Done"),
        messageCompleted(childSessionId, "msg_c1", { tokens: { input: 4, output: 2 } }),
        sessionIdle(childSessionId),
        sessionIdle(parentSessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
              },
              {
                // Title parsed from "Research the topic (@general subagent)" -> "general: Research the topic"
                span_attributes: { name: "general: Research the topic", type: "task" },
                children: [
                  {
                    span_attributes: { name: "Turn 1", type: "task" },
                    children: [
                      {
                        span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    )
  })
})

describe("System prompt capture", () => {
  it("prepends system message to LLM span input when hook fires before message completes", async () => {
    const sessionId = "ses_sys_1"
    const messageId = "msg_sys_1"
    const systemContent = "You are a helpful coding assistant. Follow AGENTS.md rules."

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hi"),
        // OpenCode fires this right before the LLM call with the resolved system prompt
        systemTransform([systemContent]),
        textPart(sessionId, messageId, "Hello!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const llmSpan = tree?.children[0]?.children.find((c) => c.type === "llm")
    expect(llmSpan).toBeDefined()
    const input = llmSpan?.input as Array<{ role: string; content: string }>
    expect(input).toEqual([
      { role: "system", content: systemContent },
      { role: "user", content: "Hi" },
    ])
  })

  it("omits system message when the hook never fires (existing behavior preserved)", async () => {
    const sessionId = "ses_sys_absent"
    const messageId = "msg_sys_absent"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hi"),
        // No systemTransform — e.g. an OpenCode version that does not emit it
        textPart(sessionId, messageId, "Hello!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const llmSpan = tree?.children[0]?.children.find((c) => c.type === "llm")
    expect(llmSpan).toBeDefined()
    const input = llmSpan?.input as Array<{ role: string; content: string }>
    expect(input).toEqual([{ role: "user", content: "Hi" }])
    // And definitely no system entry snuck in
    expect(input.some((m) => m.role === "system")).toBe(false)
  })

  it("joins multi-part system arrays with a blank line between parts", async () => {
    const sessionId = "ses_sys_multi"
    const messageId = "msg_sys_multi"
    const parts = ["Base instructions from AGENTS.md", "Additional context from CLAUDE.md"]

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hi"),
        systemTransform(parts),
        textPart(sessionId, messageId, "Hello!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const llmSpan = tree?.children[0]?.children.find((c) => c.type === "llm")
    const input = llmSpan?.input as Array<{ role: string; content: string }>
    expect(input[0]?.role).toBe("system")
    expect(input[0]?.content).toBe(parts.join("\n\n"))
  })

  it("updates system prompt across turns and reflects latest in each LLM span", async () => {
    const sessionId = "ses_sys_turns"
    const firstSystem = "Instructions v1"
    const secondSystem = "Instructions v2 with new context"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        // Turn 1
        chatMessage("First"),
        systemTransform([firstSystem]),
        textPart(sessionId, "msg_1", "Reply 1"),
        messageCompleted(sessionId, "msg_1", { tokens: { input: 5, output: 3 } }),
        sessionIdle(sessionId),
        // Turn 2: OpenCode fires the hook again with an updated prompt
        chatMessage("Second"),
        systemTransform([secondSystem]),
        textPart(sessionId, "msg_2", "Reply 2"),
        messageCompleted(sessionId, "msg_2", { tokens: { input: 6, output: 4 } }),
        sessionIdle(sessionId),
      ),
    )

    const turn1Llm = tree?.children[0]?.children.find((c) => c.type === "llm")
    const turn2Llm = tree?.children[1]?.children.find((c) => c.type === "llm")

    const turn1Input = turn1Llm?.input as Array<{ role: string; content: string }>
    const turn2Input = turn2Llm?.input as Array<{ role: string; content: string }>

    expect(turn1Input[0]).toEqual({ role: "system", content: firstSystem })
    expect(turn2Input[0]).toEqual({ role: "system", content: secondSystem })
  })

  it("ignores empty system arrays and leaves the LLM span input unchanged", async () => {
    const sessionId = "ses_sys_empty"
    const messageId = "msg_sys_empty"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Hi"),
        systemTransform([]),
        textPart(sessionId, messageId, "Hello!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const llmSpan = tree?.children[0]?.children.find((c) => c.type === "llm")
    const input = llmSpan?.input as Array<{ role: string; content: string }>
    expect(input).toEqual([{ role: "user", content: "Hi" }])
  })
})

describe("System prompt capture: production hooks", () => {
  it("createTracingHooks captures system prompt via experimental.chat.system.transform", async () => {
    const sessionId = "ses_hooks_sys"
    const messageId = "msg_hooks_sys"
    const systemContent = "System instructions from AGENTS.md"

    const collector = new TestSpanCollector()
    const hooks = createTracingHooks(
      collector,
      {
        client: { app: { log: async () => undefined } },
        worktree: "/tmp/test-project",
        directory: "/tmp/test-project",
      } as any,
      {
        apiKey: "",
        apiUrl: "https://api.braintrust.dev",
        appUrl: "https://www.braintrust.dev",
        projectName: "test-project",
        tracingEnabled: true,
        enableTools: true,
        debug: false,
      },
    )

    const eventHook = hooks.event as (args: { event: unknown }) => Promise<void>
    const chatMessageHook = hooks["chat.message"] as (
      input: unknown,
      output: unknown,
    ) => Promise<void>
    const systemTransformHook = hooks["experimental.chat.system.transform"] as (
      input: unknown,
      output: unknown,
    ) => Promise<void>

    expect(systemTransformHook).toBeDefined()

    await eventHook({ event: sessionCreated(sessionId) })
    await chatMessageHook(
      {
        sessionID: sessionId,
        agent: "assistant",
        model: { providerID: "anthropic", modelID: "claude-3-haiku" },
      },
      { parts: [{ type: "text", text: "Hi" }] },
    )
    // Plugin hook signature: (input: { sessionID }, output: { system: string[] })
    await systemTransformHook({ sessionID: sessionId }, { system: [systemContent] })
    await eventHook({ event: textPart(sessionId, messageId, "Hello!") })
    await eventHook({
      event: messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
    })
    await eventHook({ event: sessionIdle(sessionId) })
    await eventHook({ event: sessionDeleted(sessionId) })

    const tree = spansToTree(collector.getSpans())
    const llmSpan = tree?.children[0]?.children.find((c) => c.type === "llm")
    const input = llmSpan?.input as Array<{ role: string; content: string }>
    expect(input).toEqual([
      { role: "system", content: systemContent },
      { role: "user", content: "Hi" },
    ])
  })
})

describe("Reasoning/Thinking Content", () => {
  it("LLM span includes reasoning content in output", async () => {
    const sessionId = "ses_reasoning"
    const messageId = "msg_1"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Think about this problem"),
        // Model produces reasoning/thinking first
        reasoningPart(sessionId, messageId, "Let me think step by step about this problem..."),
        // Then produces the actual response
        textPart(sessionId, messageId, "Here is my answer."),
        messageCompleted(sessionId, messageId, {
          tokens: { input: 10, output: 5 },
        }),
        sessionIdle(sessionId),
      ),
    )

    // Find the LLM span
    const turnSpan = tree?.children[0]
    const llmSpan = turnSpan?.children[0]

    expect(llmSpan?.type).toBe("llm")

    // Check that reasoning is included in the output
    // Braintrust expects reasoning as an array of objects with id and content
    const output = llmSpan?.output as Array<{ reasoning?: Array<{ id: string; content: string }> }>
    expect(output).toBeDefined()
    expect(output[0]?.reasoning?.[0]?.content).toBe(
      "Let me think step by step about this problem...",
    )
  })

  it("tool span input is the tool arguments", async () => {
    const sessionId = "ses_tool_input"
    const messageId = "msg_1"
    const toolArgs = { query: "getting started with Braintrust" }

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Search the docs"),
        toolCallPart(sessionId, messageId, "call_1", "braintrust_search_docs", toolArgs),
        toolExecute(
          "call_1",
          "braintrust_search_docs",
          "braintrust_search_docs",
          toolArgs,
          "result text",
        ),
        textPart(sessionId, messageId, "Here are the results."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan?.type).toBe("tool")
    // Tool span input should be the args passed to the tool, not OpenCode metadata
    expect(toolSpan?.input).toEqual(toolArgs)
  })

  it("tool span output is captured from message.part.updated completed state", async () => {
    const sessionId = "ses_tool_output"
    const messageId = "msg_1"
    const toolArgs = { query: "evals" }
    const toolOutput = "Here are the eval results..."

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Search the docs"),
        // Running state (no output yet)
        toolCallPart(sessionId, messageId, "call_1", "braintrust_search_docs", toolArgs),
        // Completed state (has output)
        toolCallCompletedPart(
          sessionId,
          messageId,
          "call_1",
          "braintrust_search_docs",
          toolArgs,
          toolOutput,
        ),
        // tool.execute.after fires with undefined output (as happens with MCP tools)
        toolExecute(
          "call_1",
          "braintrust_search_docs",
          "braintrust_search_docs",
          toolArgs,
          undefined,
        ),
        textPart(sessionId, messageId, "Here are the results."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan?.type).toBe("tool")
    // Output should come from the completed state, not the (undefined) hook result
    expect(toolSpan?.output).toBe(toolOutput)
    expect(toolSpan?.metadata?.tool_approval).toBe("approved")
    // Input should still be the args
    expect(toolSpan?.input).toEqual(toolArgs)
  })

  it("tool span error is captured from message.part.updated error state", async () => {
    const sessionId = "ses_tool_error"
    const messageId = "msg_1"
    const toolArgs = { filePath: "/missing.ts" }

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the file"),
        toolCallPart(sessionId, messageId, "call_1", "read", toolArgs),
        toolCallErrorPart(sessionId, messageId, "call_1", "read", toolArgs, "File not found"),
        toolExecute("call_1", "read", "/missing.ts", toolArgs, undefined),
        textPart(sessionId, messageId, "The file was missing."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan?.output).toBe("File not found")
    expect(toolSpan?.error).toBe("File not found")
    expect(toolSpan?.metadata?.tool_approval).toBe("approved")
  })

  it("permission rejection creates one denied tool span", async () => {
    const sessionId = "ses_tool_denied"
    const messageId = "msg_1"
    const toolArgs = { filePath: "/secret.ts" }

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the file"),
        toolCallPart(sessionId, messageId, "call_1", "read", toolArgs),
        {
          type: "permission.asked",
          properties: {
            permission: {
              id: "perm_1",
              sessionID: sessionId,
              title: "Read secret",
              type: "tool",
              tool: { callID: "call_1", name: "read", input: toolArgs },
            },
          },
        } as any,
        {
          type: "permission.replied",
          properties: {
            id: "perm_1",
            reply: "reject",
          },
        } as any,
        toolExecute("call_1", "read", "/secret.ts", toolArgs, "should not emit"),
        textPart(sessionId, messageId, "I could not read it."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpans = turnSpan?.children.filter((c) => c.type === "tool") ?? []
    expect(toolSpans).toHaveLength(1)
    expect(toolSpans[0]?.metadata?.tool_approval).toBe("denied")
    expect(toolSpans[0]?.metadata?.call_id).toBe("call_1")
    expect(toolSpans[0]?.output).toBeUndefined()
    expect(toolSpans[0]?.error).toBeUndefined()
  })

  it("tool span includes reasoning in metadata", async () => {
    const sessionId = "ses_tool_reasoning"
    const messageId = "msg_1"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Read the file"),
        // Model thinks about what to do
        reasoningPart(sessionId, messageId, "I need to read the config file to understand..."),
        // Then calls a tool
        toolCallPart(sessionId, messageId, "call_1", "read", { filePath: "/config.ts" }),
        toolExecute("call_1", "read", "/config.ts", { filePath: "/config.ts" }, "file contents"),
        textPart(sessionId, messageId, "I read the file."),
        messageCompleted(sessionId, messageId, { tokens: { input: 15, output: 8 } }),
        sessionIdle(sessionId),
      ),
    )

    // Find the tool span
    const turnSpan = tree?.children[0]
    // Tool spans and LLM span are children of turn - find the tool one
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan?.type).toBe("tool")
    expect(toolSpan?.metadata?.reasoning).toBe("I need to read the config file to understand...")
  })
})

describe("API-created sessions (lazy init)", () => {
  it("session without session.created still produces a complete trace", async () => {
    const sessionId = "ses_api_1"
    const messageId = "msg_api_1"

    // No sessionCreated event — simulates an API-created session
    await assertEventsProduceTree(
      session(
        sessionId,
        chatMessage("Hello from API session"),
        textPart(sessionId, messageId, "Hi there!"),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 10, completion_tokens: 5, tokens: 15 },
              },
            ],
          },
        ],
      },
    )
  })

  it("session without session.created supports multiple turns", async () => {
    const sessionId = "ses_api_multi"

    await assertEventsProduceTree(
      session(
        sessionId,
        chatMessage("First message"),
        textPart(sessionId, "msg_1", "First response"),
        messageCompleted(sessionId, "msg_1", { tokens: { input: 5, output: 3 } }),
        sessionIdle(sessionId),
        chatMessage("Second message"),
        textPart(sessionId, "msg_2", "Second response"),
        messageCompleted(sessionId, "msg_2", { tokens: { input: 8, output: 4 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 5, completion_tokens: 3 },
              },
            ],
          },
          {
            span_attributes: { name: "Turn 2", type: "task" },
            children: [
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 8, completion_tokens: 4 },
              },
            ],
          },
        ],
      },
    )
  })

  it("session without session.created supports tool calls", async () => {
    const sessionId = "ses_api_tool"
    const messageId = "msg_api_tool"

    await assertEventsProduceTree(
      session(
        sessionId,
        chatMessage("Read a file"),
        toolCallPart(sessionId, messageId, "call_1", "read", { filePath: "/config.ts" }),
        toolExecute("call_1", "read", "/config.ts", { filePath: "/config.ts" }, "file contents"),
        textPart(sessionId, messageId, "I read the file."),
        messageCompleted(sessionId, messageId, { tokens: { input: 15, output: 8 } }),
        sessionIdle(sessionId),
      ),
      {
        span_attributes: { name: "OpenCode: test-project", type: "task" },
        children: [
          {
            span_attributes: { name: "Turn 1", type: "task" },
            children: [
              { span_attributes: { name: "read: config.ts", type: "tool" } },
              {
                span_attributes: { name: "anthropic/claude-3-haiku", type: "llm" },
                metrics: { prompt_tokens: 15, completion_tokens: 8 },
              },
            ],
          },
        ],
      },
    )
  })

  it("session.created after lazy init is idempotent (no duplicate root spans)", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(collector, { projectName: "test-project" }, { clock })

    const sessionId = "ses_api_idempotent"

    // chat.message fires first (API-created session — no session.created yet)
    clock.tick()
    await processor.processChatMessage(sessionId, "Hello", {
      providerID: "anthropic",
      modelID: "claude-3-haiku",
    })

    // session.created fires late (should be a no-op — state already exists)
    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: sessionId,
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: sessionId },
    })

    const spans = collector.getSpans()
    const { spansToTree } = await import("./span-sink")
    const tree = spansToTree(spans)

    // Root span should exist and have exactly one child turn (no duplicate root spans)
    expect(tree).not.toBeNull()
    expect(tree?.name).toBe("OpenCode: test-project")
    expect(tree?.children.length).toBe(1)
    expect(tree?.children[0]?.name).toBe("Turn 1")

    // Verify there is only one root span (no duplicate from session.created)
    const rootSpans = spans.filter((s) => !s._is_merge && !s.span_parents?.length)
    expect(rootSpans.length).toBe(1)
  })
})

describe("Fail-open: missing or non-string tool output", () => {
  it("tool with undefined output creates span without crashing", async () => {
    const sessionId = "ses_undef_output"
    const messageId = "msg_1"

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Call a custom MCP tool"),
        toolCallPart(sessionId, messageId, "call_1", "mcp_tool", { query: "test" }),
        toolExecute("call_1", "mcp_tool", "mcp_tool", { query: "test" }, undefined),
        textPart(sessionId, messageId, "Done."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan).toBeDefined()
    expect(toolSpan?.type).toBe("tool")
    expect(toolSpan?.name).toBe("mcp_tool: mcp_tool")
    expect(toolSpan?.output).toBeUndefined()
  })

  it("tool with object output (MCP content[]) creates span without crashing", async () => {
    const sessionId = "ses_obj_output"
    const messageId = "msg_1"

    const mcpContent = [
      { type: "text", text: "Here are the results" },
      { type: "image", data: "base64..." },
    ]

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Call an MCP tool that returns content[]"),
        toolCallPart(sessionId, messageId, "call_1", "mcp_search", { q: "foo" }),
        toolExecute("call_1", "mcp_search", "mcp_search", { q: "foo" }, mcpContent as any),
        textPart(sessionId, messageId, "Found results."),
        messageCompleted(sessionId, messageId, { tokens: { input: 12, output: 6 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan).toBeDefined()
    expect(toolSpan?.type).toBe("tool")
    // Object output should be passed through as-is (not truncated with .substring)
    expect(toolSpan?.output).toEqual(mcpContent)
  })

  it("tool with very long string output is truncated to 10000 chars", async () => {
    const sessionId = "ses_long_output"
    const messageId = "msg_1"

    const longOutput = "x".repeat(20000)

    const tree = await eventsToTree(
      session(
        sessionId,
        sessionCreated(sessionId),
        chatMessage("Call a tool with huge output"),
        toolCallPart(sessionId, messageId, "call_1", "read", { filePath: "/big.txt" }),
        toolExecute("call_1", "read", "/big.txt", { filePath: "/big.txt" }, longOutput),
        textPart(sessionId, messageId, "Read the file."),
        messageCompleted(sessionId, messageId, { tokens: { input: 10, output: 5 } }),
        sessionIdle(sessionId),
      ),
    )

    const turnSpan = tree?.children[0]
    const toolSpan = turnSpan?.children.find((c) => c.type === "tool")

    expect(toolSpan).toBeDefined()
    expect(typeof toolSpan?.output).toBe("string")
    expect((toolSpan?.output as string).length).toBe(10000)
  })
})

describe("Fail-open: chat.message with missing output", () => {
  it("chat.message with undefined output does not crash", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(collector, { projectName: "test-project" }, { clock })

    // Create session
    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_no_output",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    // Call processChatMessageRaw with undefined output — this mirrors the
    // production hook receiving an output object without parts
    clock.tick()
    await processor.processChatMessageRaw("ses_no_output", undefined)

    // Session should still be functional — send idle to close
    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_no_output" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    // A turn should have been created with empty input
    expect(tree?.children.length).toBe(1)
    expect(tree?.children[0]?.name).toBe("Turn 1")
    expect(tree?.children[0]?.input).toBeUndefined() // empty string becomes undefined
  })

  it("chat.message with empty parts array does not crash", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(collector, { projectName: "test-project" }, { clock })

    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_empty_parts",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processChatMessageRaw("ses_empty_parts", { parts: [] })

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_empty_parts" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    expect(tree?.children.length).toBe(1)
    expect(tree?.children[0]?.name).toBe("Turn 1")
  })
})

describe("EventProcessor model serialization", () => {
  it("stores bare modelID on turn metadata for processChatMessageRaw", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(collector, { projectName: "test-project" }, { clock })

    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_turn_model_raw",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processChatMessageRaw(
      "ses_turn_model_raw",
      { parts: [{ type: "text", text: "Hello from raw hook" }] },
      { providerID: "anthropic", modelID: "claude-3-haiku-20240307" },
    )

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_turn_model_raw" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    expect(tree?.children).toHaveLength(1)
    expect(tree?.children[0]?.metadata?.model).toBe("claude-3-haiku-20240307")
  })
})

describe("Additional metadata on root span", () => {
  it("includes additional_metadata on root span", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(
      collector,
      {
        projectName: "test-project",
        additionalMetadata: { ci: true, run_id: "abc-123", env: "staging" },
      },
      { clock },
    )

    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_meta",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_meta" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    // Additional metadata should be present
    expect(tree?.metadata?.ci).toBe(true)
    expect(tree?.metadata?.run_id).toBe("abc-123")
    expect(tree?.metadata?.env).toBe("staging")
    // Standard metadata should also be present
    expect(tree?.metadata?.session_id).toBe("ses_meta")
  })

  it("standard metadata overrides additional_metadata on conflict", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(
      collector,
      {
        projectName: "test-project",
        directory: "/real-dir",
        additionalMetadata: {
          session_id: "should-be-overridden",
          directory: "should-be-overridden",
          custom: "kept",
        },
      },
      { clock },
    )

    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_conflict",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_conflict" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    // Standard fields win over additional_metadata
    expect(tree?.metadata?.session_id).toBe("ses_conflict")
    expect(tree?.metadata?.directory).toBe("/real-dir")
    // Non-conflicting additional_metadata is preserved
    expect(tree?.metadata?.custom).toBe("kept")
  })

  it("works without additional_metadata (undefined)", async () => {
    const clock = new TestClock()
    const collector = new TestSpanCollector()
    const processor = new EventProcessor(collector, { projectName: "test-project" }, { clock })

    clock.tick()
    await processor.processEvent({
      type: "session.created",
      properties: {
        info: {
          id: "ses_no_meta",
          projectID: "test-project",
          directory: "/test",
          version: "1.0.0",
          title: "Test",
          time: { created: Date.now(), updated: Date.now() },
        },
      },
    })

    clock.tick()
    await processor.processEvent({
      type: "session.idle",
      properties: { sessionID: "ses_no_meta" },
    })

    const tree = spansToTree(collector.getSpans())
    expect(tree).not.toBeNull()
    expect(tree?.metadata?.session_id).toBe("ses_no_meta")
  })
})

// ---------------------------------------------------------------------------
// Log file replay tests
// ---------------------------------------------------------------------------
//
// Pattern for fixture-based tests:
//   1. Record a real session with LOG_TO_FILE=/path/to/fixture.ndjson
//   2. Copy the file into a fixtures/ directory
//   3. Write a test like the ones below, pointing at the fixture file
//
// The tests in this describe block use a synthetic log file written in-process
// so they run without needing an external fixture.  Replace the setup section
// with `const logPath = path.join(__dirname, "fixtures", "my-session.ndjson")`
// to test against a real captured file.
// ---------------------------------------------------------------------------

describe("Log file replay", () => {
  // Temp files created during tests — cleaned up in afterEach
  const tmpFiles: string[] = []

  afterEach(() => {
    for (const f of tmpFiles) {
      try {
        fs.unlinkSync(f)
      } catch {
        /* ignore */
      }
    }
    tmpFiles.length = 0
  })

  /** Write a synthetic NDJSON log file using FileLogger and the same helpers
   *  used in other tests, then replay it and return the span tree. */
  async function buildAndReplay(
    setup: (logger: FileLogger, sessionID: string) => void | Promise<void>,
    sessionID: string,
  ) {
    const logPath = path.join(os.tmpdir(), `bt-replay-test-${Date.now()}.ndjson`)
    tmpFiles.push(logPath)
    const logger = new FileLogger(logPath)

    await setup(logger, sessionID)

    return replayLogFileToTree(logPath, { projectName: "test-project" })
  }

  it("replays a simple session → turn → llm trace from a log file", async () => {
    const sessionID = "ses_replay_1"
    const messageID = "msg_replay_1"

    const tree = await buildAndReplay((logger) => {
      // Simulate what the plugin would log during a real session

      // session.created event
      logger.logEvent(
        {
          type: "session.created",
          properties: {
            info: {
              id: sessionID,
              projectID: "test-project",
              directory: "/test",
              version: "1.0.0",
              title: "Test",
              time: { created: Date.now(), updated: Date.now() },
            },
          },
        },
        sessionID,
      )

      // chat.message hook
      logger.logChatMessage(
        { sessionID, model: { providerID: "anthropic", modelID: "claude-3-haiku-20240307" } },
        { parts: [{ type: "text", text: "Hello, replay!" }] },
        sessionID,
      )

      // message.part.updated (text)
      logger.logEvent(
        {
          type: "message.part.updated",
          properties: {
            part: {
              id: `prt_${messageID}`,
              sessionID,
              messageID,
              type: "text",
              text: "Hi from replay!",
              time: { end: Date.now() },
            },
          },
        },
        sessionID,
      )

      // message.updated (assistant message completed)
      const now = Date.now()
      logger.logEvent(
        {
          type: "message.updated",
          properties: {
            info: {
              id: messageID,
              sessionID,
              role: "assistant",
              time: { created: now - 500, completed: now },
              parentID: "parent",
              modelID: "claude-3-haiku-20240307",
              providerID: "anthropic",
              mode: "build",
              path: { cwd: "/test", root: "/test" },
              cost: 0.001,
              tokens: { input: 10, output: 5, reasoning: 0, cache: { read: 0, write: 0 } },
            },
          },
        },
        sessionID,
      )

      // session.idle
      logger.logEvent({ type: "session.idle", properties: { sessionID } }, sessionID)
    }, sessionID)

    expect(tree).not.toBeNull()
    expect(tree?.name).toBe("OpenCode: test-project")
    expect(tree?.type).toBe("task")
    expect(tree?.children).toHaveLength(1)

    const turn = tree?.children[0]
    expect(turn?.name).toBe("Turn 1")
    expect(turn?.type).toBe("task")
    expect(turn?.input).toBe("Hello, replay!")
    expect(turn?.children).toHaveLength(1)

    const llm = turn?.children[0]
    expect(llm?.type).toBe("llm")
    expect(llm?.name).toBe("anthropic/claude-3-haiku-20240307")
    expect(llm?.metadata.model).toBe("claude-3-haiku-20240307")
    expect(llm?.metrics?.prompt_tokens).toBe(10)
    expect(llm?.metrics?.completion_tokens).toBe(5)
  })

  it("replays a session with a tool call", async () => {
    const sessionID = "ses_replay_tool"
    const messageID = "msg_replay_tool"
    const callID = "call_replay_1"

    const tree = await buildAndReplay((logger) => {
      logger.logEvent(
        {
          type: "session.created",
          properties: {
            info: {
              id: sessionID,
              projectID: "test-project",
              directory: "/test",
              version: "1.0.0",
              title: "Test",
              time: { created: Date.now(), updated: Date.now() },
            },
          },
        },
        sessionID,
      )

      logger.logChatMessage(
        { sessionID, model: { providerID: "anthropic", modelID: "claude-3-haiku-20240307" } },
        { parts: [{ type: "text", text: "Read the config" }] },
        sessionID,
      )

      // tool.execute.before
      logger.logToolBefore(
        { tool: "read", sessionID, callID, args: { filePath: "/config.ts" } },
        {},
        sessionID,
      )

      // tool.execute.after
      logger.logToolAfter(
        { tool: "read", sessionID, callID },
        { output: "export const debug = true", title: "/config.ts", metadata: {} },
        sessionID,
      )

      const now = Date.now()
      logger.logEvent(
        {
          type: "message.updated",
          properties: {
            info: {
              id: messageID,
              sessionID,
              role: "assistant",
              time: { created: now - 200, completed: now },
              parentID: "parent",
              modelID: "claude-3-haiku-20240307",
              providerID: "anthropic",
              mode: "build",
              path: { cwd: "/test", root: "/test" },
              cost: 0.001,
              tokens: { input: 20, output: 10, reasoning: 0, cache: { read: 0, write: 0 } },
            },
          },
        },
        sessionID,
      )

      logger.logEvent({ type: "session.idle", properties: { sessionID } }, sessionID)
    }, sessionID)

    expect(tree).not.toBeNull()
    const turn = tree?.children[0]
    expect(turn?.name).toBe("Turn 1")

    const toolSpan = turn?.children.find((c) => c.type === "tool")
    expect(toolSpan).toBeDefined()
    expect(toolSpan?.name).toBe("read: config.ts")
    expect(toolSpan?.input).toEqual({ filePath: "/config.ts" })
    expect(toolSpan?.output).toBe("export const debug = true")
    expect(toolSpan?.metadata?.tool_approval).toBe("approved")

    const llmSpan = turn?.children.find((c) => c.type === "llm")
    expect(llmSpan).toBeDefined()
    expect(llmSpan?.metrics?.prompt_tokens).toBe(20)
  })
})

describe("extractToolOutput", () => {
  it("passes through plain string output unchanged", () => {
    expect(extractToolOutput("hello")).toBe("hello")
  })

  it("passes through undefined as undefined", () => {
    expect(extractToolOutput(undefined)).toBeUndefined()
  })

  it("extracts text from MCP content array", () => {
    const mcpResult = {
      content: [
        { type: "text", text: "doc result 1" },
        { type: "text", text: "doc result 2" },
      ],
    }
    expect(extractToolOutput(mcpResult)).toBe("doc result 1\ndoc result 2")
  })

  it("returns content array as-is when no text parts", () => {
    const mcpResult = { content: [{ type: "image", data: "base64..." }] }
    expect(extractToolOutput(mcpResult)).toEqual([{ type: "image", data: "base64..." }])
  })

  it("passes through plain objects that aren't MCP results", () => {
    const obj = { foo: "bar" }
    expect(extractToolOutput(obj)).toEqual(obj)
  })
})

describe("MCP tool output fixture replay", () => {
  it("captures MCP tool input and output from mcp_tool_call.txt fixture", async () => {
    const fixturePath = path.join(__dirname, "testconvos", "mcp_tool_call.txt")

    // The fixture file contains two appended sessions. replayLogFile processes
    // all records in order; the second session (ses_36a045ec7ffe) is the one
    // we assert against — it is a clean standalone MCP doc search session.
    const spans = await replayLogFile(fixturePath, { projectName: "opencode" })
    const { spansToTree } = await import("./span-sink")

    // Find the root span for the second session
    const secondSessionRoot = spans.find(
      (s) =>
        s.metadata?.session_id === "ses_36a045ec7ffe1GCIk3kmSBiq9N" &&
        !s._is_merge &&
        !s.span_parents?.length,
    )
    expect(secondSessionRoot).toBeDefined()

    const tree = spansToTree(
      spans.filter((s) => s.root_span_id === secondSessionRoot!.root_span_id),
    )

    expect(tree).not.toBeNull()
    expect(tree?.name).toBe("OpenCode: opencode")

    const turn = tree?.children[0]
    expect(turn?.name).toBe("Turn 1")

    const toolSpan = turn?.children.find((c) => c.type === "tool")
    expect(toolSpan).toBeDefined()

    // Input: the query args passed to the MCP tool
    const input = toolSpan?.input as Record<string, unknown>
    expect(input).toBeDefined()
    expect(input.query).toBe("getting started")
    expect(input.top_k).toBe(3)

    // Output: extracted from MCP content[] array — must not be undefined
    expect(toolSpan?.output).toBeDefined()
    expect(typeof toolSpan?.output).toBe("string")
    expect(toolSpan?.output as string).toContain("Getting started")
  })
})
