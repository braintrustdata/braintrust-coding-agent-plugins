import { execFile } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { promisify } from "node:util"
import type { BraintrustConfig } from "../config"

const execFileAsync = promisify(execFile)

export interface ProjectInfo {
  id: string
  name: string
}

export interface ToolLogData {
  id: string
  span_id: string
  root_span_id: string
  input?: string
  output?: string
  expected?: string
  scores?: Record<string, number>
  metadata?: Record<string, unknown>
  tags?: string[]
  span_attributes: { name: string; type: "task" }
}

export type BtCliRunner = (args: string[]) => Promise<string>

async function defaultRunner(args: string[]): Promise<string> {
  const executable = process.env.BT_EXECUTABLE || "bt"
  try {
    const { stdout } = await execFileAsync(executable, args, {
      encoding: "utf8",
      timeout: 30_000,
      maxBuffer: 10 * 1024 * 1024,
    })
    return stdout
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`bt CLI command failed: ${message}`)
  }
}

/** Runs OpenCode data-access tools through bt so bt exclusively owns auth and API access. */
export class BtCliToolsClient {
  private readonly config: BraintrustConfig
  private readonly runner: BtCliRunner

  constructor(config: BraintrustConfig, runner: BtCliRunner = defaultRunner) {
    this.config = config
    this.runner = runner
  }

  private selection(includeProject = false): string[] {
    return [
      "--json",
      "--no-input",
      "--prefer-profile",
      ...(this.config.profile ? ["--profile", this.config.profile] : []),
      ...(this.config.orgName ? ["--org", this.config.orgName] : []),
      ...(includeProject ? ["--project", this.config.projectName] : []),
    ]
  }

  private async json<T>(args: string[]): Promise<T> {
    const stdout = await this.runner(args)
    try {
      return JSON.parse(stdout) as T
    } catch {
      throw new Error("bt CLI returned invalid JSON")
    }
  }

  async listProjects(): Promise<ProjectInfo[]> {
    return this.json<ProjectInfo[]>(["projects", "list", ...this.selection()])
  }

  private async project(): Promise<ProjectInfo> {
    const existing = (await this.listProjects()).find(
      (project) => project.name === this.config.projectName,
    )
    if (existing) return existing
    return this.json<ProjectInfo>([
      "projects",
      "create",
      this.config.projectName,
      ...this.selection(),
    ])
  }

  async queryLogs(sql: string): Promise<unknown> {
    const project = await this.project()
    const query = sql.replace(/\bFROM\s+logs\b/gi, `FROM project_logs('${project.id}')`)
    return this.json<unknown>(["sql", query, "--non-interactive", ...this.selection(true)])
  }

  async listExperiments(limit: number): Promise<unknown[]> {
    const result = await this.json<unknown[]>(["experiments", "list", ...this.selection(true)])
    return result.slice(0, limit)
  }

  async logData(data: ToolLogData): Promise<string> {
    await this.project()
    const directory = await mkdtemp(join(tmpdir(), "bt-opencode-tools-"))
    const input = join(directory, "event.jsonl")
    try {
      await writeFile(input, `${JSON.stringify(data)}\n`, { encoding: "utf8", mode: 0o600 })
      await this.json<unknown>([
        "sync",
        "push",
        `project_logs:${this.config.projectName}`,
        "--in",
        input,
        "--root",
        directory,
        "--fresh",
        ...this.selection(true),
      ])
      return data.id
    } finally {
      await rm(directory, { recursive: true, force: true })
    }
  }
}
