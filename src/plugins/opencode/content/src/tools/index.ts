/**
 * Braintrust tools for OpenCode
 *
 * Provides tools for:
 * - Querying logs
 * - Listing projects
 * - Logging data
 */

import type { ToolDefinition } from "@opencode-ai/plugin"
import { tool } from "@opencode-ai/plugin"
import type { BraintrustClient } from "../client"

/**
 * Create Braintrust tools
 */
export function createBraintrustTools(client: BraintrustClient): Record<string, ToolDefinition> {
  return {
    braintrust_query_logs: tool({
      description: `Query Braintrust logs using SQL.
Use "FROM logs" in your query - it will be automatically rewritten.

SQL dialect notes:
- Use hour(timestamp_column), day(timestamp_column) instead of date_trunc
- Use "interval 1 day" (singular unit, no quotes) for intervals
- Use dot notation for nested fields: metadata.key
- Common columns: id, input, output, expected, scores, metadata, created

Example queries:
- SELECT * FROM logs ORDER BY created DESC LIMIT 10
- SELECT * FROM logs WHERE scores.Factuality < 0.5
- SELECT * FROM logs WHERE created > now() - interval 1 hour`,
      args: {
        query: tool.schema.string().describe("SQL query to execute against Braintrust logs"),
      },
      async execute(args) {
        try {
          const results = await client.queryLogs(args.query)
          return JSON.stringify(results, null, 2)
        } catch (error) {
          return `Error executing query: ${error}`
        }
      },
    }),

    braintrust_list_projects: tool({
      description: "List all projects in your Braintrust organization",
      args: {},
      async execute() {
        try {
          const projects = await client.listProjects()
          if (projects.length === 0) {
            return "No projects found."
          }
          return projects.map((p) => `- ${p.name} (${p.id})`).join("\n")
        } catch (error) {
          return `Error listing projects: ${error}`
        }
      },
    }),

    braintrust_log_data: tool({
      description: `Log data to Braintrust for evaluation or tracking.
You can log input/output pairs, scores, and metadata.

This is useful for:
- Recording important decisions or outputs for review
- Creating evaluation datasets
- Tracking model performance over time`,
      args: {
        input: tool.schema.string().optional().describe("The input that was given (optional)"),
        output: tool.schema.string().optional().describe("The output that was produced (optional)"),
        expected: tool.schema.string().optional().describe("The expected/ideal output (optional)"),
        scores: tool.schema
          .string()
          .optional()
          .describe('JSON object of scores, e.g. {"accuracy": 0.95, "relevance": 0.8}'),
        metadata: tool.schema
          .string()
          .optional()
          .describe('JSON object of additional metadata, e.g. {"task_type": "code_review"}'),
        tags: tool.schema.string().optional().describe("Comma-separated list of tags"),
      },
      async execute(args) {
        try {
          const spanId = crypto.randomUUID()

          const span: {
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
          } = {
            id: crypto.randomUUID(),
            span_id: spanId,
            root_span_id: spanId,
            span_attributes: {
              name: "Manual Log",
              type: "task",
            },
          }

          if (args.input) span.input = args.input
          if (args.output) span.output = args.output
          if (args.expected) span.expected = args.expected

          if (args.scores) {
            try {
              span.scores = JSON.parse(args.scores)
            } catch {
              return "Error: scores must be valid JSON"
            }
          }

          if (args.metadata) {
            try {
              span.metadata = JSON.parse(args.metadata)
            } catch {
              return "Error: metadata must be valid JSON"
            }
          }

          if (args.tags) {
            span.tags = args.tags.split(",").map((t) => t.trim())
          }

          const rowId = await client.insertSpan(span)
          if (rowId) {
            return `Successfully logged data with ID: ${rowId}`
          }
          return "Failed to log data - check Braintrust connection"
        } catch (error) {
          return `Error logging data: ${error}`
        }
      },
    }),

    braintrust_get_experiments: tool({
      description: "List recent experiments for the current project",
      args: {
        limit: tool.schema
          .number()
          .optional()
          .describe("Maximum number of experiments to return (default: 10)"),
      },
      async execute(args) {
        const limit = args.limit || 10
        try {
          // Query experiments via BTQL
          const _query = `
            SELECT id, name, created, metadata
            FROM experiments
            ORDER BY created DESC
            LIMIT ${limit}
          `

          // Note: This would need the experiments endpoint
          // For now, return a helpful message
          return `To view experiments, visit https://www.braintrust.dev/app/projects/${client.getProjectId()}/experiments`
        } catch (error) {
          return `Error getting experiments: ${error}`
        }
      },
    }),
  }
}
