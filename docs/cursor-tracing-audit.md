# Cursor tracing evidence and limits

This audit inspected Cursor CLI `2026.10.01-14929f9` and desktop `3.4.16`
on macOS on October 2, 2026. The scope is PR 1 of
[issue 120](https://github.com/braintrustdata/braintrust-coding-agent-plugins/issues/120):
hook capture and Codex-style reconstruction from observable conversation data.
Exact model API requests are outside that target.

Sources are the [official hooks reference](https://cursor.com/docs/hooks),
[plugin documentation](https://cursor.com/docs/plugins), installed CLI chunks
`190.index.js`, `1962.index.js`, and `3778.index.js`, and the installed desktop
`out/vs/workbench/workbench.desktop.main.js`. Installed implementation details
are version-specific evidence, not supported public API guarantees.

## Native requirements

| Requirement | Status | Evidence and limits |
| --- | --- | --- |
| Millisecond timestamp on every event | No | Recorded native hook payloads omit timestamps. The fixture wrapper records hook receipt time; native tool `duration` and thought `duration_ms` are available. |
| Shared session, turn, and operation identities | Yes, with surface limits | `conversation_id` is stable across interactive turns. Interactive `beforeSubmitPrompt` and `stop` share a new `generation_id` for each user turn; tool pairs share `tool_use_id`. Headless tool `generation_id` equals the conversation ID. Thought IDs have additional, sometimes unrelated request suffixes. No model request pair ID is exposed. |
| Every operation identifies its user turn | No | Interactive Read uses the prompt generation ID, but headless tools use the conversation ID. Remote thought hooks can use a different request ID after reconnect. Subagent ancestry needs separate native parent fields. |
| Paired starts and stops for every operation | No | Generic tool hooks pair correctly. Thoughts/responses are completion observations; there is no documented model start hook. Compaction has only `preCompact`. |
| Full model request/response API bodies | No | Hooks expose thought/response text and selected model configuration, not API requests. Transcripts contain a reduced conversation view. |
| Tool/web/MCP starts and stops | Yes for observed tools; other tools unverified | Parallel Read/Shell starts and completions share tool IDs and carry durations. Generic hooks are documented for every tool. Web/MCP, permission denial, and cancelled tool execution were not demonstrated in the recorded sessions. |
| Turn start and stop | Yes for interactive project hooks | Successful and failed multi-turn captures use different prompt/stop generation IDs in one conversation. Plugin-only interactive local lifecycle gates and the headless path have important limitations below. |
| Recursive subagent hooks and parent turns | Unknown | Documented subagent fields and installed implementations exist. Child/nested successful sessions were not captured. A schema alone does not prove recursive emission or correct turn attribution. |
| Session end/shutdown | Yes | CLI `sessionEnd` reports `reason`, `duration_ms`, and `final_status`; resumed sessions remain the same logical conversation. Cloud has different lifecycle semantics in the official documentation. |
| Ordered blocking delivery | No as a universal claim | Permission/tool hook execution is awaited in the inspected adapter. Interactive local thought callbacks are launched without awaiting their completion, and the same thought can also arrive through remote hooks. Concurrent tool hooks overlap. Receipt order is evidence, not a total native operation order. |

## Recorded sessions

Sanitized native fixtures are in
[`bt-daemon/tests/fixtures/cursor/real-cli`](../bt-daemon/tests/fixtures/cursor/real-cli).
They preserve field names, value types, ID relationships, native durations,
usage, and record order. Workspace paths, user identity, and random identifiers
were replaced consistently. Model text unrelated to the controlled audit task
was redacted. The capture wrapper's `received_at_ms` is not a Cursor timestamp.
Each directory contains hook observations and the final saved native transcript.

| Scenario | Observed behavior | Outcome |
| --- | --- | --- |
| Headless local logging plugin, Composer 2.5 | Parallel Read and Shell succeeded; shell output was captured; transcript path appeared after startup. Tool generation IDs equal conversation ID; thought generation IDs vary. | Session ended with `WritableIterable is closed` after reconnect attempts. |
| Headless project hooks, Grok 4.5 low, no tools | Thought hooks arrived and a transcript was written. No local prompt/stop/response lifecycle was observed. | Same connection/stream failure. |
| Interactive project hooks, two turns, Composer 2.5 | Prompt and error stop pair per turn; second-turn Read shares that turn ID; stop carries native usage. Local and remote thought delivery duplicates content with different IDs. | Both turns produced requested answer text but terminated with the same stream error. |
| Headless project hooks, Auto, thought hook removed | Parallel Read/Shell succeeded; transcript records final answer and terminal status. | Native stream result and `sessionEnd` both report success. |
| Interactive project hooks, Auto, two turns, thought hook removed | Both turns completed; response and stop share each turn's generation ID and identical usage. Read/Shell first turn, Read second turn. | Both stops report completed; late response hooks provide final answers. |
| Exact development plugin, headless Auto, followed by `--resume` | Native plugin forwarded Read/Shell and completed session lifecycle through an absolute `BT_BIN` capture shim; resumed invocation reused the conversation ID and read the file again. | Both native CLI results report success; resumed result is `AUDIT_RESUMED`. |

Removing only `afterAgentThought` registration from the project config changed
the Auto tool scenario from repeated stream failure to success. A plain Auto run
without hooks also succeeded. The shipped plugin therefore excludes this hook
by default for current CLI compatibility; the translator can still accept it
when another surface supplies it. These runs demonstrate real successful CLI
capture and error handling. Real desktop validation remains outstanding.

## Correlation and usage

Interactive prompt generation IDs are the strongest observed user-turn boundary.
Use exact native IDs where they match, then correlate tool completions by
`tool_use_id`. Session-level IDs and remote thought request suffixes must not create
extra user turns. When an operation has no verified turn identity, retain the
uncertainty and use a deterministic observed-boundary fallback.

An interactive first-turn stop reported `input_tokens=12554`,
`output_tokens=5`, `cache_read_tokens=12544`, and `cache_write_tokens=0`.
The second turn reported `12823`, `30`, `12800`, and `0`, respectively.
Successful interactive turns additionally reported `27376/174/2816/0` and
`27866/117/27520/0` (input/output/cache read/cache write), respectively; each
response/stop pair repeats the same four counts. Installed interactive CLI code reads these from the most recent native
`turnEnded` update. Desktop resets its turn usage at submission, fills it from
that same update, and sends it on both `afterAgentResponse` and `stop`.
Treat these as native **turn totals**, deduplicating the two observations;
do not assign them to individual reconstructed model calls or add them twice.
In both successful turns, `stop` was received before `afterAgentResponse`.
The later response must enrich the same closed generation, without creating
another turn or adding its usage again.

Input counts include the cache subsets. The inspected CLI display subtracts
cache read/write counts to display uncached input. Preserve native inclusive
input tokens and cache read/write counts independently. Headless stream-JSON
`result.usage.inputTokens` is already normalized to uncached input by CLI code;
it is a different field from the raw hook `input_tokens`. Retry accounting
remains unverified, although successful response/stop duplication is captured.

Local and remote thoughts can deliver identical text with different generation
IDs. The interactive fixture shows a local exact turn ID and a remote ID with
a model-step suffix. Preserve native IDs in the durable journal while deduplicating the
same observed content within its supported boundary. Do not claim that every
thought corresponds to one model call: reconnects and completions can change IDs,
and no paired model start/end boundary establishes exact call counts.

## Tool completeness and transcripts

Read output is a JSON string containing `file_path` and `content_length`, not
file contents. The observed shell output contains `output` and `exitCode`.
Keep their native shape and report completeness separately; do not synthesize
unobserved read contents into model input. Specialized shell/file hooks can add
facts but must not create another span for a generic tool operation already seen.

The specialized `beforeReadFile` payload carries native `content` in these
CLI captures. Preserve it as a pre-read observation when it can be correlated
safely; it is not the generic delivered read result and does not establish
complete-output parity for other tools or surfaces.

CLI writes JSONL transcripts without a transcript option. Early hook paths were
null, and later hooks supplied a path after the file existed. The observed saved
transcript contains user/assistant text, tool names/inputs, and `turn_ended` status;
it omits tool results, call IDs, per-record timestamps, model usage, reasoning,
and system messages. A timestamp embedded in user text is not an event timestamp.

The native writer can rewrite the file. In the interactive two-turn capture, the
final file retained both prompts but only the latest `turn_ended` record. CLI
`3778.index.js` resets incremental writing after a terminal record, changed
summary archives, or changed prior message data. A private mirror must detect
rewrites/truncation as well as appends, retain previous captured terminal facts,
and replay observed content without duplicating it.

Append observations write only the new bytes. Detecting arbitrary same-length
rewrites requires a streamed comparison of the previously mirrored prefix.
A rewrite stores a new immutable snapshot, including its unchanged prefix;
storage is incremental for appends but can grow with repeated full rewrites.
Journal references remain bounded to each captured observation.

Desktop code also creates transcript writers during agent execution. The common
documented schema permits null paths when transcripts are disabled. Treat every
path as optional; desktop settings and cloud availability were not exercised.
Native historical import cannot recover the information that these saved
transcripts never recorded. Recovery from Braintrust's captured hook journal is
a different capability.

## Plugins and execution surfaces

The CLI supports local `--plugin-dir`; Cursor discovers
`.cursor-plugin/plugin.json` and `hooks/hooks.json`, and supplies
`CURSOR_PLUGIN_ROOT` to command hooks. Project hook command paths run from the
project root. User hook paths run from the user's `.cursor` directory.

The inspected interactive CLI checks only user/project configuration before
locally firing `beforeSubmitPrompt`, `afterAgentResponse`, `afterAgentThought`,
and `stop`. This check excludes plugin hooks even though the executor supports
them. Each event has its own gate: a project prompt hook alone does not activate
the response/thought/stop gates. A local development project config can use
valid no-op hooks for the desired supported names, allowing the loaded plugin to receive the
same events. The headless execution path lacks these local lifecycle calls;
adding project no-ops does not repair that path. Remote hook delivery can still
provide tools/thoughts, as demonstrated by the failed headless runs.

For this installed CLI, registering `afterAgentThought` repeatedly produced a
`WritableIterable is closed` failure, including a no-tool prompt. The same Auto
tool prompt succeeded with this one hook removed. The default plugin omits it;
reasoning availability is therefore limited on the verified CLI surface.
Desktop users may opt into it for further testing, but desktop compatibility
with this event was not demonstrated.

The exact default plugin was loaded using `--plugin-dir` after this compatibility
change, with a `BT_BIN` shim that recorded its arguments and stdin. Both an Auto
tool session and a separate headless `--resume` invocation succeeded and reused
the same conversation ID. Their sanitized native hooks, saved transcript, and
CLI results are in `plugin-headless-resume-success`. The shim establishes native
loading/forwarding compatibility; it does not by itself validate translated
Braintrust spans.

An additional real interactive Auto session used that exact plugin with
`src/plugins/cursor/test/bt-standalone-wrapper.sh`, the standalone daemon built
with `cargo build --manifest-path bt-daemon/Cargo.toml --all-features --locked`,
and the foreground debug sink in isolated private storage. Two successful turns
produced one session, two turns, three tools (Read/Shell, then Read), and four
reconstructed model spans. Every turn is a session child; each model/tool is a
direct child of its owning turn. Native usage is present only on turns. The
second turn's final output is `AUDIT_RESUMED`. The original hooks and final
transcript are sanitized in `daemon-two-turn-success`; private sink rows were
inspected after applying their merges. This verifies CLI-to-daemon translation
and sink delivery, without a remote Braintrust backend.

Desktop uses the broader configured-hook discovery before prompt/response/stop
execution; installed code includes plugin hooks in that discovery. This is
implementation evidence only; a real desktop session was not captured.
Cloud support is documented separately and must not be inferred from CLI tests.

Permission hooks require valid allow JSON even if tracing fails:
`preToolUse`, `subagentStart`, `beforeShellExecution`, `beforeMCPExecution`, and
`beforeReadFile` accept `{"permission":"allow"}`. Prompt submission accepts
`{"continue":true}`. Empty/invalid successful responses can block execution
despite `failClosed=false`; keep tracing command output off policy stdout.

## Subagents and compaction

Documented subagent hooks expose `subagent_id`, `parent_conversation_id`, a
spawning `tool_call_id`, lifecycle status/duration, task/summary data, and optional
child transcript paths. Installed desktop Task code uses the spawning tool ID
as the subagent ID and uses the parent conversation ID for the generation field.
That observation permits native operation correlation but does not establish
parent user-turn identity or recursive child-hook delivery. Unknown child
ancestry must remain unknown; child roots should be linked only when native
parent evidence is sufficient.

`preCompact` is an observation of a trigger and context counters. It supplies no
completion, generated summary, or replacement conversation. The inspected
transcript writer observes summary archive changes, but no controlled compaction
fixture established a recoverable replacement context. Emit only the observed
compaction boundary, mark history after it incomplete, and do not invent a
compaction model response or reuse old history as verified replacement context.

## Verdict and PR 1 parity

Under the strict requirements in
[`trace-coding-agents-with-hooks.md`](trace-coding-agents-with-hooks.md), Cursor
is **Insufficient**: full model requests, complete tool results, and model
start/stop identity are absent from both hooks and the observed transcript.
Codex-style reconstruction is feasible for observable live data with explicit
provenance and fidelity limits. Its release validation must distinguish fixture
coverage from successful real execution.

| Capability | PR 1 implementation | Native evidence and limits |
| --- | --- | --- |
| Session/turn hierarchy | Implemented; successful real CLI sink verified | Interactive multi-turn IDs, native stop usage, headless resume; overlapping turns remain unverified |
| Reconstructed model spans | Implemented from observable messages and tool boundaries | Exact API requests/times unavailable; exact call grouping unverified; default thought hook excluded on tested CLI |
| Tools | Implemented paired generic operations and specialized enrichment | Parallel Read/Shell verified; Read post-result is metadata, pre-read content available; write/edit, denial, cancellation, web/MCP lack real captures |
| Usage | Implemented deduplicated native turn totals | Successful response/stop duplicates and late order verified; retry accounting unverified |
| Transcript enrichment/recovery | Implemented private mirroring, observed boundaries and recovery | Delayed paths/full rewrites captured; recovery tested after original deletion; native historical results/usage/timing unavailable |
| Subagents | Implemented evidence-led lifecycle/parent correlation with synthetic tests | Public schema and installed code inspected; real child/nested/background activity unverified |
| Compaction | Implemented observed point and history invalidation with synthetic tests | Completion/summary/replacement context unavailable in captured evidence |
| Desktop/cloud | Native registration supported; no real parity claim | Installed desktop code and cloud docs inspected; neither exercised as real traced sessions |
| Setup/run/import/distribution | PR 2 scope | Not implemented or claimed as PR 1 parity |

## Verification

The explicit Unix PTY smoke is reproducible with an authenticated `agent` CLI:

```sh
cargo build --manifest-path bt-daemon/Cargo.toml --all-features --locked
python3 src/plugins/cursor/test/test_real_cursor.py --run
```

The real smoke passed on macOS: two completed turns, three tools, four
reconstructed model spans, correct parentage, and native usage confined to
turns. It uses the exact source plugin and the actual standalone debug sink,
not a fake inference endpoint. Its private temporary evidence directory is
retained for inspection. The harness is Unix-specific; desktop, Windows,
cloud, real subagents, and real compaction remain unverified surfaces.

Focused automated checks include 27 fixture translator tests, six transcript
mirror tests, hook policy/capture checks, and envelope-to-mirror-to-translator
pipeline recovery after deleting original transcripts. These distinguish real
capture fixtures from synthetic edge scenarios, including late responses,
duplicate usage, rewritten files, replay, permissions, and missing ancestry.
Run the Rust focused suites with:

```sh
cargo test --manifest-path bt-daemon/Cargo.toml --all-features --locked --test cursor_translator
cargo test --manifest-path bt-daemon/Cargo.toml --all-features --locked --test cursor_pipeline
cargo test --manifest-path bt-daemon/Cargo.toml --all-features --locked transcript_mirror::tests
make test
```

The full repository suite reproduced an existing doctor fixture failure on
unchanged `HEAD`; excluding that known test yielded 411 passes and four existing
ignored tests during implementation. This baseline failure is separate from
Cursor's native thought-hook issue above. `make test`, Rust formatting, and Clippy
with `--all-features --locked --all-targets -- -D warnings` passed. The baseline
failure is `trace_runtime::tests::doctor_reports_a_daemon_that_resolves_other_credentials`;
the full command used `-- --skip` with that exact test name.


| Delegated workstream | Status and reviewable evidence |
| --- | --- |
| `audit-agent-tracing-support` | Complete for PR 1: this audit, six sanitized native scenarios; native gaps and unverified surfaces listed above. |
| `add-coding-agent-translator` | Complete for PR 1: production Cursor registry/translator, 27 fixture and edge tests; reconstructed model grouping remains explicit. |
| `add-coding-agent-capture` | Complete: native local plugin, fail-open policy tests, exact plugin loaded by actual CLI. |
| `test-coding-agent-integration` | Complete for implemented surfaces: real CLI-to-debug-sink smoke, routed pipeline and deleted-source recovery; desktop/cloud/recursive execution remain unverified. |
| Setup, managed run, import, shipping | PR 2; no release or publication performed. |

## Instrumentation specification alignment

The [Braintrust instrumentation guide](https://github.com/braintrustdata/braintrust-spec/blob/main/skills/instrumentation-spec/references/instrumentation-guide.md)
and its [token and cost metrics](https://github.com/braintrustdata/braintrust-spec/blob/main/skills/instrumentation-spec/references/features/token-and-cost-metrics.md)
reference govern the exported span format. Session/turn spans are `task` spans;
model/tool spans are siblings under the owning turn. LLM inputs are ordered
OpenAI-style messages. Outputs are choice arrays with assistant messages,
`tool_calls`, and JSON-stringified function arguments. Observed thoughts are
preserved separately as reasoning summaries rather than merged into answer
text. Tool-result history uses matching `tool_call_id` values and string content.
Known tool-call boundaries use `finish_reason: tool_calls`; other provider finish
reasons remain null because Cursor does not report them.

Native turn usage is an aggregate parent metric, which the specification allows.
It is never attributed to a guessed individual provider request. `tokens` is
computed from known prompt and completion counts; cache counts remain subsets
of prompt counts. Partial snapshots are merged without summing duplicates or
fabricating missing zeros. Unknown per-call usage, resolved Auto models,
provider/billing identity, model-request bodies, and streaming token timing
remain unavailable. This is a hook-based reconstruction with documented gaps,
rather than a claim of full provider API instrumentation compliance.

Automatic Cursor metadata has an explicit allowlist:

- Session context: `source`, `session_id`, `username`, `os`, `workspace`,
  `trace_cursor_version`, `trace_plugin_version`.
- Git context: `git_origin_url`, `git_branch`, `git_commit_sha`.
- Model configuration and capture limits: `model`, `model_params`,
  `input_reconstructed`, `start_time_estimated`, `history_truncated`,
  `output_truncated`.
- Usage and outcome: `token_usage_scope`, `status`.
- Tool evidence and failures: `result_completeness`, `pre_read_content`,
  `failure_type`, `is_interrupt`, `tool_approval`.
- Subagent and compaction limits: `subagent_type`, `recursive_activity_verified`,
  `observation_only`, `completion_observed`, `replacement_context_available`.

Explicitly configured destination metadata remains a user customization on the
session root. Raw hook IDs and reducer bookkeeping remain in the durable journal.
