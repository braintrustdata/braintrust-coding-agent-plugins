# Braintrust coding-agent plugins (monorepo)

Single source of truth for all Braintrust coding-agent plugins. Skills and the
Braintrust MCP config are authored **once** under `src/`; per-agent trees are
**built** by `src/plugins/<agent>/build.sh` and force-pushed by CI into the
distribution repos that marketplaces consume:

| Agent  | Distribution repo (unchanged install URL)   |
|--------|---------------------------------------------|
| claude | braintrustdata/braintrust-claude-plugin     |
| codex  | braintrustdata/braintrust-codex-plugin      |

See AGENTS.md for the build/deploy model.
