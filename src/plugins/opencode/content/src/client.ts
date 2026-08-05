/**
 * Braintrust API client for the OpenCode plugin
 */

import { PLUGIN_VERSION } from "./version"

export interface BraintrustConfig {
  apiKey: string
  apiUrl?: string
  appUrl: string
  orgName?: string
  projectName: string
  tracingEnabled: boolean
  enableTools: boolean
  debug: boolean
  logToFile?: string // Path to write NDJSON log of all plugin I/O, or "auto" for default path
  additionalMetadata?: Record<string, unknown>
  queueSize?: number
}

/**
 * Plugin config from opencode.json `braintrust` section.
 * Uses snake_case to match environment variable naming.
 */
export interface PluginConfig {
  api_key?: string
  api_url?: string
  app_url?: string
  org_name?: string
  project?: string
  trace_to_braintrust?: boolean
  enable_tools?: boolean
  debug?: boolean
  log_to_file?: string // Path to write NDJSON log of all plugin I/O, or "true"/"auto" for default path
  additional_metadata?: Record<string, unknown>
  queue_size?: number
}

export interface SpanData {
  id: string
  span_id: string
  root_span_id: string
  span_parents?: string[]
  created?: string // ISO timestamp for ordering
  input?: unknown
  output?: unknown
  expected?: unknown
  error?: string
  scores?: Record<string, number>
  metadata?: Record<string, unknown>
  metrics?: {
    start?: number
    end?: number
    prompt_tokens?: number
    completion_tokens?: number
    tokens?: number
    prompt_cached_tokens?: number
    prompt_cache_creation_tokens?: number
    reasoning_tokens?: number
  }
  context?: {
    caller_functionname?: string
    caller_filename?: string
    caller_lineno?: number
    span_origin?: Record<string, unknown>
  }
  span_attributes?: {
    name?: string
    type?: "llm" | "task" | "tool" | "function" | "eval" | "score"
  }
  _is_merge?: boolean // When true, merge with existing span by id instead of creating new row
}

function detectEnvironment(): { type?: string; name?: string } | undefined {
  if (process.env.BRAINTRUST_ENVIRONMENT_TYPE || process.env.BRAINTRUST_ENVIRONMENT_NAME) {
    return {
      ...(process.env.BRAINTRUST_ENVIRONMENT_TYPE
        ? { type: process.env.BRAINTRUST_ENVIRONMENT_TYPE }
        : {}),
      ...(process.env.BRAINTRUST_ENVIRONMENT_NAME
        ? { name: process.env.BRAINTRUST_ENVIRONMENT_NAME }
        : {}),
    }
  }
  if (process.env.GITHUB_ACTIONS) return { type: "ci", name: "github_actions" }
  if (process.env.GITLAB_CI) return { type: "ci", name: "gitlab_ci" }
  if (process.env.CIRCLECI) return { type: "ci", name: "circleci" }
  if (process.env.BUILDKITE) return { type: "ci", name: "buildkite" }
  if (process.env.CI) return { type: "ci", name: "ci" }
  if (process.env.VERCEL) return { type: "server", name: "vercel" }
  if (process.env.NETLIFY) return { type: "server", name: "netlify" }
  if (
    process.env.ECS_CONTAINER_METADATA_URI ||
    process.env.ECS_CONTAINER_METADATA_URI_V4 ||
    process.env.AWS_EXECUTION_ENV?.startsWith("AWS_ECS_")
  ) {
    return { type: "server", name: "ecs" }
  }
  if (
    process.env.AWS_LAMBDA_FUNCTION_NAME ||
    process.env.AWS_EXECUTION_ENV?.startsWith("AWS_Lambda_")
  ) {
    return { type: "server", name: "aws_lambda" }
  }
  if (process.env.NODE_ENV === "production" || process.env.NODE_ENV === "staging") {
    return { type: "server", name: process.env.NODE_ENV }
  }
  if (process.env.NODE_ENV === "development" || process.env.NODE_ENV === "local") {
    return { type: "local", name: process.env.NODE_ENV }
  }
  return undefined
}

function withSpanOrigin(span: SpanData): SpanData {
  const environment = detectEnvironment()
  return {
    ...span,
    context: {
      ...(span.context ?? {}),
      span_origin: {
        name: "braintrust.plugin.opencode",
        version: PLUGIN_VERSION,
        instrumentation: { name: "opencode-tracing" },
        ...(environment ? { environment } : {}),
      },
    },
  }
}

interface LoginResponse {
  org_info: Array<{
    name: string
    api_url: string
  }>
}

interface ProjectResponse {
  id: string
  name: string
}

interface InsertResponse {
  row_ids: string[]
}

/**
 * Parse a boolean environment variable.
 * Accepts: "true", "TRUE", "1", "tRuE" (case-insensitive) as truthy.
 * All other values (including undefined, "false", "0", "no") are falsy.
 */
export function parseBooleanEnv(value: string | undefined): boolean {
  if (!value) return false
  const normalized = value.toLowerCase()
  return normalized === "true" || normalized === "1"
}

/**
 * Load Braintrust config with the following precedence (later overrides earlier):
 * 1. Default values
 * 2. opencode.json `braintrust` section (pluginConfig)
 * 3. Environment variables (highest priority)
 */
export function loadConfig(pluginConfig?: PluginConfig): BraintrustConfig {
  // Defaults
  const defaults: BraintrustConfig = {
    apiKey: "",
    apiUrl: "https://api.braintrust.dev",
    appUrl: "https://www.braintrust.dev",
    orgName: undefined,
    projectName: "opencode",
    tracingEnabled: false,
    enableTools: true,
    debug: false,
    logToFile: undefined,
  }

  // Layer 1: Apply opencode.json config (if provided)
  if (pluginConfig) {
    if (pluginConfig.api_key) defaults.apiKey = pluginConfig.api_key
    if (pluginConfig.api_url) defaults.apiUrl = pluginConfig.api_url
    if (pluginConfig.app_url) defaults.appUrl = pluginConfig.app_url
    if (pluginConfig.org_name) defaults.orgName = pluginConfig.org_name
    if (pluginConfig.project) defaults.projectName = pluginConfig.project
    if (pluginConfig.trace_to_braintrust !== undefined) {
      defaults.tracingEnabled = pluginConfig.trace_to_braintrust
    }
    if (pluginConfig.enable_tools !== undefined) {
      defaults.enableTools = pluginConfig.enable_tools
    }
    if (pluginConfig.debug !== undefined) {
      defaults.debug = pluginConfig.debug
    }
    if (pluginConfig.log_to_file !== undefined) {
      defaults.logToFile = pluginConfig.log_to_file
    }
    if (pluginConfig.additional_metadata) {
      defaults.additionalMetadata = pluginConfig.additional_metadata
    }
    if (pluginConfig.queue_size !== undefined) {
      defaults.queueSize = pluginConfig.queue_size
    }
  }

  // Layer 2: Apply environment variables (override opencode.json)
  let additionalMetadata = defaults.additionalMetadata
  if (process.env.BRAINTRUST_ADDITIONAL_METADATA) {
    try {
      additionalMetadata = JSON.parse(process.env.BRAINTRUST_ADDITIONAL_METADATA)
    } catch {
      // Invalid JSON in env var — ignore and keep config file value (if any)
    }
  }

  // Resolve logToFile: env var LOG_TO_FILE overrides config file
  // Values: a file path, "true"/"auto" (use default path), or unset/falsy (disabled)
  let logToFile = defaults.logToFile
  if (process.env.LOG_TO_FILE) {
    logToFile = process.env.LOG_TO_FILE
  }

  let queueSize = defaults.queueSize
  if (process.env.BRAINTRUST_QUEUE_SIZE) {
    const parsed = parseInt(process.env.BRAINTRUST_QUEUE_SIZE, 10)
    if (!Number.isNaN(parsed) && parsed > 0) {
      queueSize = parsed
    }
  }

  return {
    apiKey: process.env.BRAINTRUST_API_KEY || defaults.apiKey,
    apiUrl: process.env.BRAINTRUST_API_URL || defaults.apiUrl,
    appUrl: process.env.BRAINTRUST_APP_URL || defaults.appUrl,
    orgName: process.env.BRAINTRUST_ORG_NAME || defaults.orgName,
    projectName: process.env.BRAINTRUST_PROJECT || defaults.projectName,
    tracingEnabled: process.env.TRACE_TO_BRAINTRUST
      ? parseBooleanEnv(process.env.TRACE_TO_BRAINTRUST)
      : defaults.tracingEnabled,
    enableTools: process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS
      ? parseBooleanEnv(process.env.BRAINTRUST_OPENCODE_ENABLE_TOOLS)
      : defaults.enableTools,
    debug: process.env.BRAINTRUST_DEBUG
      ? parseBooleanEnv(process.env.BRAINTRUST_DEBUG)
      : defaults.debug,
    logToFile,
    additionalMetadata,
    queueSize,
  }
}

export class BraintrustClient {
  private config: BraintrustConfig
  private resolvedApiUrl?: string
  private projectId?: string
  private initPromise?: Promise<void>
  private initError?: Error

  constructor(config: BraintrustConfig) {
    this.config = config
  }

  async initialize(): Promise<void> {
    // Store the initialization promise so other methods can wait on it
    if (!this.initPromise) {
      this.initPromise = this._doInitialize()
    }
    return this.initPromise
  }

  private async _doInitialize(): Promise<void> {
    try {
      // Resolve API URL
      this.resolvedApiUrl = await this.resolveApiUrl()

      // Get or create project
      this.projectId = await this.getOrCreateProject(this.config.projectName)

      // Debug info stored for later retrieval if needed
    } catch (error) {
      this.initError = error instanceof Error ? error : new Error(String(error))
      throw this.initError
    }
  }

  /**
   * Wait for initialization to complete (used by methods that need the client ready)
   */
  async waitForInit(): Promise<boolean> {
    if (!this.initPromise) {
      return false
    }
    try {
      await this.initPromise
      return true
    } catch {
      return false
    }
  }

  private async resolveApiUrl(): Promise<string> {
    // Check for explicit override
    if (this.config.apiUrl) {
      return this.config.apiUrl
    }

    // Default if no API key
    if (!this.config.apiKey) {
      return "https://api.braintrust.dev"
    }

    try {
      // Login to discover API URL
      const response = await fetch(`${this.config.appUrl}/api/apikey/login`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
        },
      })

      if (!response.ok) {
        return "https://api.braintrust.dev"
      }

      const data = (await response.json()) as LoginResponse

      // Filter by org name if specified
      if (this.config.orgName) {
        const org = data.org_info.find((o) => o.name === this.config.orgName)
        if (org?.api_url) {
          return org.api_url
        }
      }

      // Use first org
      if (data.org_info?.[0]?.api_url) {
        return data.org_info[0].api_url
      }
    } catch {
      // Fall back to default API URL
    }

    return "https://api.braintrust.dev"
  }

  private async getOrCreateProject(name: string): Promise<string> {
    const encodedName = encodeURIComponent(name)

    // Try to get existing project
    try {
      const response = await fetch(
        `${this.resolvedApiUrl}/v1/project?project_name=${encodedName}`,
        {
          headers: {
            Authorization: `Bearer ${this.config.apiKey}`,
          },
        },
      )

      if (response.ok) {
        const data = (await response.json()) as ProjectResponse
        if (data.id) {
          return data.id
        }
      }
    } catch {
      // Continue to create
    }

    // Create project
    try {
      const response = await fetch(`${this.resolvedApiUrl}/v1/project`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ name }),
      })

      if (response.ok) {
        const data = (await response.json()) as ProjectResponse
        if (data.id) {
          return data.id
        }
      }
    } catch {
      // Fall through to throw
    }

    throw new Error(`Failed to get or create project: ${name}`)
  }

  /**
   * Insert a span into project logs
   */
  async insertSpan(
    span: SpanData,
    debugLog?: (msg: string, data?: unknown) => void,
  ): Promise<string | undefined> {
    // Wait for initialization to complete
    const ready = await this.waitForInit()
    if (!ready || !this.projectId) {
      debugLog?.("insertSpan: not ready", { ready, projectId: this.projectId })
      return undefined
    }

    try {
      const enrichedSpan = withSpanOrigin(span)
      const payload = { events: [enrichedSpan] }
      debugLog?.("insertSpan: sending", {
        spanId: span.span_id,
        isMerge: span._is_merge,
        hasInput: span.input !== undefined,
        hasOutput: span.output !== undefined,
        hasSpanParents: span.span_parents !== undefined,
        hasSpanAttributes: span.span_attributes !== undefined,
        payload: JSON.stringify(payload).substring(0, 500),
      })

      const response = await fetch(
        `${this.resolvedApiUrl}/v1/project_logs/${this.projectId}/insert`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.config.apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        },
      )

      if (!response.ok) {
        const text = await response.text()
        debugLog?.("insertSpan: request failed", { status: response.status, text })
        return undefined
      }

      const data = (await response.json()) as InsertResponse
      debugLog?.("insertSpan: success", { rowId: data.row_ids?.[0] })
      return data.row_ids?.[0]
    } catch (e) {
      debugLog?.("insertSpan: error", { error: String(e) })
      return undefined
    }
  }

  /**
   * Execute a BTQL query against project logs
   */
  async queryLogs(sql: string): Promise<unknown[]> {
    // Wait for initialization to complete
    const ready = await this.waitForInit()
    if (!ready || !this.projectId) {
      throw new Error("Braintrust client not initialized")
    }

    try {
      // Rewrite "FROM logs" to "FROM project_logs('project_id')"
      const rewrittenSql = sql.replace(
        /\bFROM\s+logs\b/gi,
        `FROM project_logs('${this.projectId}')`,
      )

      const response = await fetch(`${this.resolvedApiUrl}/btql`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query: rewrittenSql }),
      })

      if (!response.ok) {
        const text = await response.text()
        throw new Error(`Query failed (${response.status}): ${text}`)
      }

      const data = await response.json()
      return data as unknown[]
    } catch (error) {
      throw new Error(`Failed to execute query: ${error}`)
    }
  }

  /**
   * List projects in the organization
   */
  async listProjects(): Promise<ProjectResponse[]> {
    // Wait for initialization to complete
    const ready = await this.waitForInit()
    if (!ready) {
      throw new Error("Braintrust client not initialized")
    }

    try {
      const response = await fetch(`${this.resolvedApiUrl}/v1/project`, {
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
        },
      })

      if (!response.ok) {
        throw new Error(`Failed to list projects: ${response.status}`)
      }

      const data = await response.json()
      return (data as { objects: ProjectResponse[] }).objects || []
    } catch (error) {
      throw new Error(`Failed to list projects: ${error}`)
    }
  }

  /**
   * Get the current project ID
   */
  getProjectId(): string | undefined {
    return this.projectId
  }

  /**
   * Get the resolved API URL
   */
  getApiUrl(): string | undefined {
    return this.resolvedApiUrl
  }

  /**
   * Check if client is properly initialized
   */
  isInitialized(): boolean {
    return !!this.projectId && !!this.resolvedApiUrl
  }
}
