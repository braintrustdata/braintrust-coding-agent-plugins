---
name: troubleshoot-braintrust-mcp
description: |
  This plugin auto-configures a "braintrust" MCP server. If you can't see it or reach it, activate this skill
version: 1.0.0
---

This Claude plugin automatically sets up a Braintrust MCP connection. The connection reads the `BRAINTRUST_API_KEY` environment variable to establish the MCP connection.

## Check the Claude surface first

This plugin MCP configuration supports Claude Code CLI and Claude Code mode in
the desktop app. It does not configure MCP inside the Cowork tab because Cowork
runs in a separate VM without the host environment variables or network
context expected by `.mcp.json`.

If the user is in Cowork, stop these plugin troubleshooting steps and direct
them to add or use the Braintrust connector through Claude. Do not ask them to
copy `BRAINTRUST_API_KEY` into the Cowork VM.

## Troubleshooting steps

### 1. Verify the environment variable is set

Run `echo $BRAINTRUST_API_KEY` to check if the variable is exported

API keys can be created at https://www.braintrust.dev/app/settings?subroute=api-keys

### 2. Verify the API key is valid

Test the key by calling the Braintrust API:

```bash
curl -s https://api.braintrust.dev/api/self/me -H "Authorization: Bearer $BRAINTRUST_API_KEY"
```

- If valid: returns JSON with user info (id, email, organizations, etc.)
- If invalid: returns an authentication error

NOTE: Even if you can curl the api via http, continue to attempt MCP setup. Http is just a troubleshooting tool, not a replacement for MCP

### 3. Check if the MCP server is reachable

If the key is valid but connection still fails, check if the MCP server is up:

```bash
curl -s -o /dev/null -w "%{http_code}" https://api.braintrust.dev/mcp
```

- Any HTTP response (even 401 or 405) means the server is reachable
- Connection timeout or "connection refused" means the server may be down

### 4. Contact support

If nothing else works, encourage the user to reach out:
- Discord: https://discord.com/invite/6G8s47F44X
- Email: support@braintrust.dev
