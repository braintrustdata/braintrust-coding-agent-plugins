import type { BraintrustConfig } from "../config"

interface LoginResponse {
  org_info: Array<{ name: string; api_url: string }>
}

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

/** API access used only by OpenCode's four explicit Braintrust tools. */
export class BraintrustToolsClient {
  private readonly config: BraintrustConfig
  private resolvedApiUrl?: string
  private projectId?: string
  private initPromise?: Promise<void>

  constructor(config: BraintrustConfig) {
    this.config = config
  }

  initialize(): Promise<void> {
    this.initPromise ??= this.doInitialize()
    return this.initPromise
  }

  private async doInitialize(): Promise<void> {
    this.resolvedApiUrl = await this.resolveApiUrl()
    this.projectId = await this.getOrCreateProject(this.config.projectName)
  }

  private async ready(): Promise<void> {
    if (!this.initPromise) throw new Error("Braintrust tools client not initialized")
    await this.initPromise
    if (!this.resolvedApiUrl || !this.projectId) {
      throw new Error("Braintrust tools client not initialized")
    }
  }

  private async resolveApiUrl(): Promise<string> {
    if (this.config.apiUrl) return this.config.apiUrl
    if (!this.config.apiKey) return "https://api.braintrust.dev"

    try {
      const response = await fetch(`${this.config.appUrl}/api/apikey/login`, {
        method: "POST",
        headers: { Authorization: `Bearer ${this.config.apiKey}` },
      })
      if (!response.ok) return "https://api.braintrust.dev"
      const data = (await response.json()) as LoginResponse
      const selected = this.config.orgName
        ? data.org_info.find((org) => org.name === this.config.orgName)
        : data.org_info[0]
      return selected?.api_url ?? "https://api.braintrust.dev"
    } catch {
      return "https://api.braintrust.dev"
    }
  }

  private async getOrCreateProject(name: string): Promise<string> {
    const headers = { Authorization: `Bearer ${this.config.apiKey}` }
    const encodedName = encodeURIComponent(name)
    try {
      const response = await fetch(
        `${this.resolvedApiUrl}/v1/project?project_name=${encodedName}`,
        { headers },
      )
      if (response.ok) {
        const project = (await response.json()) as ProjectInfo
        if (project.id) return project.id
      }
    } catch {
      // Try creation below.
    }

    const response = await fetch(`${this.resolvedApiUrl}/v1/project`, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    })
    if (!response.ok) throw new Error(`Failed to get or create project: ${name}`)
    const project = (await response.json()) as ProjectInfo
    if (!project.id) throw new Error(`Failed to get or create project: ${name}`)
    return project.id
  }

  async queryLogs(sql: string): Promise<unknown[]> {
    await this.ready()
    const query = sql.replace(/\bFROM\s+logs\b/gi, `FROM project_logs('${this.projectId}')`)
    const response = await fetch(`${this.resolvedApiUrl}/btql`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.config.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
    })
    if (!response.ok) throw new Error(`Query failed (${response.status}): ${await response.text()}`)
    return (await response.json()) as unknown[]
  }

  async listProjects(): Promise<ProjectInfo[]> {
    await this.ready()
    const response = await fetch(`${this.resolvedApiUrl}/v1/project`, {
      headers: { Authorization: `Bearer ${this.config.apiKey}` },
    })
    if (!response.ok) throw new Error(`Failed to list projects: ${response.status}`)
    return ((await response.json()) as { objects?: ProjectInfo[] }).objects ?? []
  }

  async logData(data: ToolLogData): Promise<string | undefined> {
    await this.ready()
    const response = await fetch(
      `${this.resolvedApiUrl}/v1/project_logs/${this.projectId}/insert`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ events: [data] }),
      },
    )
    if (!response.ok) return undefined
    return ((await response.json()) as { row_ids?: string[] }).row_ids?.[0]
  }

  getProjectId(): string | undefined {
    return this.projectId
  }
}
