# Braintrust Trace Devin

Capture local Devin CLI sessions in Braintrust, including user prompts,
assistant replies, tool inputs and results, and permission requests.

## Try it locally

The experimental setup below targets macOS and Linux. You need a signed-in
Devin CLI and a `bt` build that includes this daemon's Devin translator. Authenticate `bt` with your usual Braintrust account.

From this repository, build and link the local plugin:

```bash
make validate-devin
devin plugins install --local ./dist/devin/plugins/braintrust-trace-devin
```

To opt a Git repository into tracing, set these fields in its root
`.braintrust.json` and substitute your Braintrust project ID:

```json
{
  "trace_to_braintrust": true,
  "route": {
    "destination": {
      "type": "project_logs",
      "project_id": "YOUR_PROJECT_ID"
    }
  }
}
```

If your Devin organization disables plugins, you can register the same hooks in
the repository. For a new hook configuration, copy the built plugin's
`hooks.json` to `.devin/hooks.v1.json`. If that file already exists, add the
Braintrust command to your existing event groups.

Start a new local `devin` session in that repository. Set
`"trace_to_braintrust": false` to disable capture for the repository, or run
`devin plugins remove braintrust-trace-devin --local` to unlink the plugin.

## Hook fidelity

A live macOS probe on Devin CLI `3000.15.14` captured matching
`tool_use_id` values across pre/post hooks for parallel shell calls and the final
assistant reply in `Stop.last_assistant_message`. Both fields are absent from the
published hook reference. The following gaps matter for full session traces:

- The observed hooks had no event timestamp. Span timestamps reflect when
  `bt` captured each hook. Native timestamps and tool execution durations would
  make the timeline precise. The payload also omitted the agent version and
  workspace path. Devin supplies the workspace separately in `DEVIN_PROJECT_DIR`.
- There is no documented model-request hook. The probe captured the user prompt
  and final reply, with no per-request model input, response, model identity, or
  token/cache counts. Paired model events with request and parent IDs would let
  us attach each tool call to its originating model call.
- A command exiting with code 1 reported `tool_response.success: true` and
  `error: null`. The exit code appeared in the output text. A typed exit code
  and tool outcome would distinguish tool transport success from command failure.
- Non-interactive permission rejection produced `PermissionRequest` with the
  original tool ID and no `PostToolUse` for that call. A terminal permission
  decision and reason would distinguish rejection from interruption or a
  missing event.
- A background Explore subagent's file read used the parent session and prompt
  IDs and carried no parent agent or spawning tool ID. The agent ID appeared in
  tool output and a context notification, and no `SubagentStop` was observed
  for that run. Structured subagent IDs on child activity and paired lifecycle
  events would make nested traces possible.

The probe used an isolated workspace with two independent file reads, a read of
a missing file, a background Explore subagent, and a separate headless turn with
a tool requiring approval. Only macOS was exercised.
[Devin lifecycle hooks](https://docs.devin.ai/cli/extensibility/hooks/lifecycle-hooks.md)
