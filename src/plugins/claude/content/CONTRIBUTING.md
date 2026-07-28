# Development of the plugins

## Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/) for the Braintrust skill
  evals.
- Rust for the shared tracing daemon.
- `jq` for plugin manifest validation and the optional fixture recorder.

## Local testing

Load a plugin directly without installing it from the marketplace:

```bash
claude --plugin-dir /path/to/repo/plugins/braintrust
claude --plugin-dir /path/to/repo/plugins/trace-claude-code
```

## Running evals

The `evals/` directory verifies that Claude can use Braintrust workflows:

```bash
cd evals
export BRAINTRUST_API_KEY="your-key"
uv run braintrust eval .
```

## Testing `trace-claude-code`

The plugin contains only a fail-open `bt` hook shim. All event translation and
Braintrust delivery live in the shared Rust daemon at `bt-daemon/`.

From the monorepo root:

```bash
cargo test --manifest-path bt-daemon/Cargo.toml --all-features
cargo clippy --manifest-path bt-daemon/Cargo.toml --all-targets --all-features -- -D warnings
make test
```

`bt-daemon/tests/claude_translator.rs` covers synthetic lifecycle cases and
replays the immutable captured sessions under
`plugins/trace-claude-code/test/fixtures/sessions/`. Add translator behavior and
assertions there, not as another hook script.

### Capturing a fixture

Set `BRAINTRUST_RECORD_DIR` to a new absolute directory before running Claude:

```bash
export BRAINTRUST_RECORD_DIR=/absolute/path/to/new-fixture
claude --plugin-dir /path/to/plugins/trace-claude-code
```

The shim appends `{ts, hook, payload}` records to `events.ndjson` and copies
referenced main/subagent transcripts under `transcripts/`. Move a reviewed,
credential-free capture under `test/fixtures/sessions/`, add its contract to
the Rust test, and run the full daemon suite. The daemon’s normal recovery
journal independently embeds transcript snapshots at lifecycle boundaries.

## Pre-commit hooks

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

# Releasing a plugin

Releases are manual and git-driven. There are no git tags or publish
automation: pushing to `main` is the release.

Claude Code resolves a plugin version from the first available source:

1. `version` in `plugins/<plugin>/.claude-plugin/plugin.json`
2. `version` in its marketplace entry
3. the source commit SHA

Each plugin’s `plugin.json` is authoritative. Do not add a per-plugin version
to `marketplace.json`; a stale duplicate can mask the real version.

Release steps:

1. Bump the plugin’s `.claude-plugin/plugin.json` version.
2. Optionally bump the marketplace manifest’s top-level bookkeeping version.
3. Commit and merge through a PR.
4. Users update with
   `claude plugin marketplace update braintrust-claude-plugin`.
