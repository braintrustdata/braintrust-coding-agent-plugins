# Native Cursor CLI captures

Captured October 2, 2026 with Cursor CLI `2026.10.01-14929f9` on macOS.
Both scenarios used an isolated temporary workspace containing only `sample.txt`
(`audit fixture hello\n`) and audit hooks. See
[the audit](../../../../../docs/cursor-tracing-audit.md) for commands and limits.

- `headless-tools-error`: local logging plugin, `--print --output-format
  stream-json --model composer-2.5 --trust --force`. The prompt requests Read,
  `printf`, and a one-sentence summary. Tool operations succeeded; the session
  failed after reconnects with `WritableIterable is closed`.
- `interactive-multi-turn-error`: project hooks, interactive Composer 2.5.
  First prompt requests `AUDIT_OK`; second requests Read and `AUDIT_TWO`.
  Both stops reported errors and native turn usage. The final transcript has
  only the last terminal marker because Cursor rewrites it between turns.
- `headless-tools-success`: project hooks with `afterAgentThought` removed,
  headless Auto model. Parallel Read/Shell and complete session succeeded.
  `stream-result.json` preserves the native CLI terminal result; its
  `usage.inputTokens` is normalized uncached input, unlike raw hook counts.
- `interactive-multi-turn-success`: the same reduced hook config, interactive
  Auto model, two successful user turns. Both `stop` observations precede their
  `afterAgentResponse` observations, which repeat the identical turn usage.
- `plugin-headless-resume-success`: the exact default development artifact,
  loaded with `--plugin-dir` and an absolute `BRAINTRUST_BT_BIN` recording shim. A headless
  Auto Read/Shell session and a separate `--resume` invocation both succeeded.
  This records only the plugin's forwarded payloads, without the shim arguments.
  The two stream-result files retain native terminal success and usage.
- `daemon-two-turn-success`: the exact plugin forwarded real interactive Auto
  hooks to a foreground standalone daemon and its offline debug sink. Two
  completed turns produced one session, two turns, three tools and four model
  spans with the expected parentage. Hooks are extracted from the actual event
  journal, retaining receipt time but removing daemon-private `_bt_` mirror
  metadata. The final native transcript is included.

`hooks.ndjson` contains capture wrappers with `received_at_ms` and the original
native `payload` shape. Receipt time is capture instrumentation, not native
Cursor timing. `transcript.jsonl` is Cursor's saved transcript shape.

Sanitization consistently replaces workspace paths, user email, random session,
turn, tool and request identifiers. Native suffixes and correlations are retained.
Unrelated model text is replaced with `[redacted non-audit model text]`; native
`[REDACTED]` strings were already present in Cursor's transcript. Numeric token
counts, durations, receipt order, nulls, nested field names and types are retained.
No credentials or pre-existing conversations were copied.

The `error` directories retain native failures even when answer text arrived.
The `success` directories retain independently observed successful execution.
Synthetic lifecycle/edge fixtures elsewhere must be identified separately and
must not be presented as captures.
