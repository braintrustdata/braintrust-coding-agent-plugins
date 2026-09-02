# Grok tracing conformance TODO

Audit date: 2026-09-03
Audited agent: `grok 1.0.13 (5e9a58528b76) [stable]`
Canonical source: `grok`

## Verdict

**Core trace prototype; not yet core-conformant or product-complete.**

The repository has a thin hook forwarder, transcript mirroring, a registered
production translator, root/turn/LLM/tool rows, aggregate usage, deterministic
fixture replay, package validation, and release plumbing. The largest gaps are:

1. The native contract has not been captured and versioned. Grok's documented
   hooks do not expose LLM calls and do not guarantee ordered blocking delivery
   for every lifecycle event. The translator depends on undocumented
   `updates.jsonl` as its authoritative data stream, uses `events.jsonl` only
   for optional tool enrichment, and snapshots `system_prompt.txt` for the
   first reconstructed LLM input.
2. Current LLM spans are reconstructed from output stream boundaries, not
   proven native LLM request/response boundaries. The system prompt is
   observable, but full request bodies and per-call usage remain unavailable.
3. Session-end spelling, resume behavior, source namespacing, cancellation, and
   bounded-state requirements are incomplete.
4. Permission, subagent, compaction, web/MCP, setup, doctor, managed run,
   import/attach, and Grok-specific end-to-end tests are absent.
5. The distributed artifact still contains a placeholder license and has no
   installed real-agent or real-ingest smoke test.

Do not describe the integration as full-fidelity or production-ready until the
P0 items and the release gates below are complete.

## Evidence inspected

- Official Grok hooks documentation:
  <https://docs.x.ai/build/features/hooks>
- `src/plugins/grok/content/hooks/hooks.json`
- `src/plugins/grok/content/hooks/forward.sh`
- `src/plugins/grok/content/README.md`
- `src/plugins/grok/test/test_hook.sh`
- `src/plugins/grok/{build,validate,publish}.sh`
- `src/plugins/grok/local-dev.sh`
- `bt-daemon/src/translate/grok.rs`
- `bt-daemon/src/{dispatch,ids,journal,server,setup,trace_command,trace_runtime}.rs`
- `bt-daemon/src/transcript_import/`
- `bt-daemon/tests/grok_translator.rs`
- `bt-daemon/tests/fixtures/grok/transcript/`
- `.github/workflows/{ci,_release,release,test-release}.yml`

The official hook documentation is not versioned to Grok 1.0.13. It documents
CamelCase lifecycle names and a common payload, but not timestamps, turn IDs,
shared operation IDs, cross-event ordering, LLM events, transcript paths, or the
`updates.jsonl`, `events.jsonl`, and `system_prompt.txt` formats. Repository
fixtures therefore prove observed behavior, not a stable compatibility
contract.

## Native support matrix

| # | Native requirement | Status | Evidence / gap |
|---|---|---|---|
| 1 | Millisecond timestamps | Partial | The daemon stamps hook capture time. Transcript fixtures contain `_meta.agentTimestampMs` and RFC3339 `ts`; the hook docs do not promise a native event timestamp. |
| 2 | Stable session, turn, and operation IDs | Partial | `sessionId`, `promptIndex`/`promptId`, and `toolCallId` are observed. No native LLM operation ID is present; the translator derives LLM identity from `streamStartMs`. |
| 3 | Turn attribution on every operation | Partial | Some transcript completions carry `promptId`; the fixture's tool start does not. The translator currently uses one implicit current turn. |
| 4 | Paired start/stop operations | Partial | Tool call/update records and user/turn-completed records exist. LLM spans are inferred from stream changes rather than paired native call events. |
| 5 | LLM call start/stop with full request/response | No | Grok documents no LLM hook. The first reconstructed LLM can include the native `system_prompt.txt` snapshot, but exact provider requests remain unavailable; output is agent stream content, not a proven full provider response. |
| 6 | Tool/web/MCP start/stop | Partial | Generic tool start/update data is observed. Web and MCP coverage/classification are unproven. |
| 7 | Turn start/stop | Partial | `user_message_chunk` and `turn_completed` are observed, but the stability and uniqueness of `promptIndex`/`promptId` across resume are not documented. |
| 8 | Recursive subagent events with ancestry | Unknown | `SubagentStart`/`SubagentStop` hooks are registered, but there is no captured payload, transcript fixture, recursive ancestry evidence, or translator support. |
| 9 | Session shutdown | Yes | `SessionEnd` is documented and registered. Translator/session flush semantics still need the fixes below. |
| 10 | Ordered blocking delivery | No evidence | Grok documents `PreToolUse` as the only blocking event. The adapter waits for daemon acknowledgement when invoked, but Grok does not document ordered blocking execution for all passive lifecycle hooks. |

**Fallback verdict: fallback-viable for a core root/turn/tool trace, with fidelity
loss.** The undocumented session files currently supply the system prompt,
assistant stream output, tool activity, turn completion, and aggregate usage.
They do not prove full LLM request/response boundaries, and schema/version
stability remains unknown.

## Section 11 conformance report

| Capability | Status | Evidence / blocker |
|---|---|---|
| Source registration | Partial | `grok` is used by the adapter, settings path, translator factory, registry, and fixtures. Setup, doctor, run, and import enums omit Grok. IDs, delivery keys, and journal paths are not source-namespaced. |
| Native event audit | Partial | All documented lifecycle hooks are registered and one transcript schema is fixture-tested for 1.0.13. No real raw-hook fixture set, schema provenance, compatibility window, ordering guarantee, or transcript stability evidence exists. |
| Fail-open capture | Partial | Adapter forwards stdin synchronously and its test covers forwarding failure and missing `bt`/`curl`. It tries `curl | bash` installation inside a 5-second hook and does not forward source/plugin versions or test terminal flush/order/reconnection. |
| Session root | Partial | Deterministic task root, external attachment, and native/documented session-end spellings exist. Source is absent from the ID namespace, and a closed translator cannot reopen/extend the root safely on resume. |
| Turn spans | Partial | User input, user-visible assistant output without reasoning, timing, stop metadata, failure/cancellation handling, and sequential multi-turn fixtures exist. Concurrent turns and resumed sessions remain unproven. |
| LLM spans/metrics | Partial | Native model names and reconstructed stream output/timing exist. The first LLM includes the native system prompt and first user message; each LLM output separates observed reasoning from assistant response. Exact later-call provider inputs and serialization remain unavailable. Aggregate usage remains on the turn and is also attributed once to the final reconstructed LLM with explicit turn-scope metadata; true per-call usage remains unavailable. Provider/finish/TTFT and proven request boundaries are unavailable. Raw `costUsdTicks` is preserved without plugin-side estimation. |
| Tool/web/MCP spans | Partial | Generic native-ID tool spans include input/output/timing/kind and basic errors. Web/MCP classification, structured failure fields, cancellation tags, nested ancestry, and concurrency coverage are missing. |
| Permission events | Not started | `PermissionDenied` is captured but ignored by the translator; request/allow/modify evidence is unknown. |
| Subagents | Not started | Start/stop hooks are captured but ignored; no hierarchy fixture or recursive evidence exists. |
| Compaction | Not started | Pre/post hooks are captured but ignored; no transcript or payload fixture exists. |
| Failures/cancellation | Partial | Tool failure becomes `error`; dangling work is marked incomplete. Failed/cancelled turns and session hooks are not translated into error/status metadata, and cancellation is not distinguished from failure. |
| Session/Git/custom metadata | Partial | Source/session/cwd/workspace/permission/transcript and optional versions are supported; route metadata precedence is implemented. Live version forwarding, shared Git enrichment, execution mode normalization, and Grok-specific precedence tests are missing. |
| Deterministic replay and bounded state | Partial | Incremental/full replay equivalence and bounded recent-ID caches exist. Open tools, assistant/LLM output, LLM ID vectors, and per-boundary record vectors are unbounded; duplicate records, malformed lines, truncation/regrowth, and resume are not covered. |
| Project/experiment/parent routing | Partial | Shared route and sink types support all destinations and the translator consumes attached IDs. Grok has no setup/run/import surface or route/attachment pipeline test. |
| Enable/disable/doctor | Not started | `SetupAgent` and `DoctorAgent` omit Grok. The README explicitly refers to a future setup command. |
| Managed run | Not started | `RunSource` omits Grok. `local-dev.sh` demonstrates a development-only isolated home/hook injection path, not the public route-isolated product contract. |
| Import/live attach | Blocked | `ImportSource` and `transcript_import` support only Codex and Claude. Implementation is absent, and historical roots, authoritative session lookup, stable `updates.jsonl` tail semantics, and independent optional `events.jsonl` enrichment are not established. |
| Security and data-handling review | Partial | Credentials stay out of the adapter and shared routes/journals redact auth; daemon storage is private. The hook auto-installs remote code, raw payloads are logged whenever Grok translator debug logging is enabled, and redaction limitations/retention are not fully documented. |
| Build/validation/publishing | Partial | Build, validation, version stamping, dry-run deployment, production deployment, tags, and releases include Grok. The artifact license is `TODO Apache 2.0 (placeholder)`; compatibility, rollback, supported platforms, and installed-artifact verification are missing. |
| Translator and pipeline tests | Partial | Translator tests cover first-LLM system/user input, reasoning/response output, turn content separation, incremental replay, malformed/partial records, replacement, terminal spellings, and asymmetric missing-file recovery. The packaged adapter-to-journal-to-translator-to-sink test covers independent mirrors, system prompt capture, first-LLM input, restart replay, route isolation, terminal flush, and late tool enrichment. |
| Installed real-agent smoke | Not started | `local-dev.sh` is manual and does not assert an installed distribution trace. CI installs/tests other agents only; release smoke runs only Codex or Antigravity. |

## P0: establish a trustworthy core trace

### Native evidence and compatibility

- [ ] **AUDIT-01 / AUDIT-02:** Capture provenance-labeled real hook payloads and
  complete `updates.jsonl`, `events.jsonl`, and `system_prompt.txt` session
  evidence from Grok 1.0.13 for a normal turn, tool
  success/failure/cancellation, permission denial, subagent, compaction,
  session exit, and resume.
- [ ] Record which fields are documented versus empirically observed, whether
  passive hooks are ordered and awaited, transcript locations/rotation rules,
  ID stability, timestamp units, and schema behavior across the minimum and
  current supported Grok versions.
- [x] Preserve `streamStartMs` groups as the observable LLM/tool/LLM sequence,
  while documenting that they are reconstructed stream boundaries rather than
  proven native requests. Attribute aggregate usage once to the final group
  with explicit turn-scope metadata; never assign it to every group.
- [ ] Treat full LLM request bodies, per-call native usage, web/MCP identity,
  subagent ancestry, and skill/background semantics as **Blocked**, not N/A,
  until the audit produces evidence or Grok documents their absence.

### Capture and terminal behavior

- [x] **CAP-01 / SEC-01:** Remove installation (`curl | bash`) from
  `hooks/forward.sh`. Installation belongs to `bt trace enable grok`; a missing
  CLI should produce a bounded fail-open diagnostic without changing the host.
- [ ] **CAP-02:** Forward Grok and plugin versions into the envelope. The plugin
  manifest contains version `0.1.0`, but `run_hook` currently writes
  `plugin_version: None` and the adapter supplies no `--source-version`.
- [x] Fix the daemon handshake to send `env.plugin_version` as
  `client.plugin_version`; `forward_envelope` currently mislabels
  `env.source_version` as the plugin version during both initialize attempts.
- [x] **CAP-03 / REL-05:** Define and test exact native event spelling. Official
  docs and `hooks.json` use `SessionEnd`/`Stop`; the adapter test uses
  snake_case, while `GrokTranslator::handle` checks only `session_end` and the
  shared flush path checks `SessionEnd`, `Stop`, and `SubagentStop`.
- [x] Make the terminal timeout coherent. Every Grok hook is capped at 5
  seconds, but `flush_session` defaults to 10 seconds before startup/IPC
  overhead, so Grok can terminate `SessionEnd` before the flush's own bound.
- [x] Capture independent high-water boundaries for `updates.jsonl`,
  `events.jsonl`, and `system_prompt.txt`. Each sibling is mirrored
  independently; a missing optional file omits only its reference and does not
  block the authoritative updates stream.
- [ ] Prove passive-event ordering and process-exit delivery. If Grok cannot
  await all relevant hooks, design transcript high-water/catch-up behavior that
  cannot lose the final records; do not claim ordered blocking capture.
- [ ] Test paths with spaces/non-ASCII, daemon startup timeout, repeated missing
  CLI diagnostics, forwarding timeout, and terminal flush failure. Preserve
  Grok's exit and permission behavior in every case.

### Identity, resume, and correlation

- [ ] **ID-01 / REL-02:** Namespace span IDs by canonical source as well as
  native session ID. `ids::span_id` currently hashes only `session_id + key`.
- [ ] Namespace daemon `DeliveryKey`, journal files, transcript mirrors, managed
  run records, and import processor keys by source where collisions are
  possible. Current delivery/journal identity uses session ID plus route but not
  source.
- [ ] This identity work changes common daemon contracts; obtain approval before
  changing the shared ID, journal, or route schema, and add migration/recovery
  coverage for existing journals.
- [ ] **TRACE-01 / REL-07:** Replace permanent `root_closed` behavior with tested
  resume semantics. A later process lifetime for the same native session must
  extend the same logical root without leaving its end time before new child
  work or duplicating the root.
- [ ] **TRACE-14:** Either prove Grok serializes turns and operations and lock the
  assumption with fixtures/tests, or replace `current_turn`/`open_llm` with
  native-ID keyed correlation. Ambiguous ancestry must fail safe.

### Translator correctness and bounds

- [x] **TRACE-03 / TRACE-11:** Keep aggregate usage on the turn and also copy it
  once to the final reconstructed LLM for LLM-view visibility. Mark the copy
  with `usage_scope: "turn"` and `usage_attribution: "last_llm"` so it is not
  represented as true per-call evidence, especially when `usage.modelCalls`
  exceeds one.
- [x] Preserve native `costUsdTicks` as `cost_usd_ticks`; do not calculate
  `estimated_cost` in the plugin.
- [x] **TRACE-08:** Map native failed, cancelled, interrupted, and incomplete
  turns/tools to explicit error and outcome metadata/tags. Never represent
  cancellation as generic failure or success.
- [x] Deduplicate a `user_message_chunk` before closing the current turn or
  incrementing `turn_seq`. The current order mutates state first, then rejects
  an already-emitted turn key, which can drop all later output for the active
  turn.
- [x] **REL-03:** Bound `open_tools`, assistant output, LLM output,
  `llm_span_ids`, and records processed per call. Use `drain_pending` for bounded
  catch-up instead of loading all new transcript records into a `Vec`.
- [ ] **REL-04:** Add explicit behavior for partial final writes, malformed
  complete lines, file replacement/truncation/regrowth, missing mirror files,
  additive fields, and version variants. One bad record must not silently stall
  every later record forever.
- [x] Make cursor advancement asymmetric. Updates read and cursor advancement
  remain transactional and authoritative. An unreadable or short events mirror
  leaves the events cursor unchanged, logs a debug diagnostic, and cannot
  discard update `SpanOp`s or delay terminal handling; a later hook retries it.
- [x] Close roots/turns at `max(hook timestamp, last observed transcript
  timestamp)` so capture-time skew cannot place an ancestor end before its
  child activity.
- [x] **OPS-02 / SEC-02:** Remove unconditional raw payload fields from normal
  debug logging or put them behind a separate explicit sensitive-diagnostics
  opt-in with private, bounded storage and documentation.

### Core verification

- [ ] **TEST-01:** Add Grok fixtures for multiple turns, exact `SessionEnd`,
  flush, shutdown, resume, duplicates inside a transcript, missing turn/tool/LLM
  boundaries, unknown records, additive fields, malformed/partial records,
  attachment, custom metadata precedence, version variants, cancellation, and
  state limits.
- [ ] Add assertions for root/turn/operation start and end times, remaining
  output variants, errors, cancellation tags, metric names/units, stable IDs,
  native system/user input provenance, reasoning/response separation, and no
  invented later-call provider history or per-call usage.
- [x] **TEST-02:** Add a Grok-specific pipeline test covering packaged adapter
  invocation, `HookArgs` mapping, envelope timestamps/versions/process capture,
  independent transcript and system prompt mirroring, redacted journal,
  production translator, route-isolated sink rows, restart replay, late
  optional events enrichment, and terminal flush.
- [ ] Add a source-collision regression using the same native session ID for
  Grok and another source on the same route.

## P1: native operation fidelity

- [ ] **TRACE-04:** Complete tool missing-start/missing-stop, duplicate,
  reordered, concurrent, structured failure, cancellation, and defensive-flush
  behavior using native `toolCallId`.
- [ ] **TRACE-09:** Classify observable web and MCP calls as tool spans while
  retaining native transport/server/method metadata. Keep Blocked if the audit
  cannot distinguish them.
- [ ] **TRACE-05:** Translate permission requests and decisions when observable.
  Current capture has only documented `PermissionDenied`; do not invent allow
  or modification decisions.
- [ ] **TRACE-10:** Represent explicit skill loads/invocations only if a native
  payload or vetted transcript proves them.
- [ ] **TRACE-03 / TRACE-12:** Add provider, native request/response, finish
  reason, TTFT, API duration, retry, fingerprint, and cost only where native
  evidence exists. Document unavailable fields.
- [ ] **TRACE-06:** Add recursive subagent task spans keyed and parented by
  native ancestry; cover sub-subagents and concurrent children.
- [ ] **TRACE-13:** Preserve observable background/asynchronous operation IDs
  until native completion/cancellation or defensive flush.
- [ ] **TRACE-07:** Add compaction/session-tree spans from `PreCompact` and
  `PostCompact` only after their payloads and correlation IDs are captured.

## P2: metadata, reliability, and routing

- [ ] **META-01 / META-02:** Pass live agent/plugin versions and use the shared
  `GitMetadataCache` for repository root, revision, branch, remote, and dirty
  state when a worktree is available.
- [ ] Normalize cwd/workspace/execution/permission/transcript provenance while
  preserving raw native fields needed for drift diagnosis.
- [ ] **META-03:** Test that route metadata reaches the root, `_bt_*` keys are
  hidden, and integration-owned source/session/version keys win.
- [ ] **META-04:** Add replay-safe native session summary metrics only when the
  transcript exposes authoritative totals without double counting resume.
- [x] **HIST-00 / REL-06:** Add Grok pipeline recovery tests proving the
  authoritative updates mirror, optional system prompt snapshot, and journal
  boundary survive daemon restart without reading future bytes or duplicating
  rows, while an optional events mirror may appear later and enrich the
  existing deterministic tool span.
- [ ] **ROUTE-01 / ROUTE-02 / ROUTE-03:** Exercise project ID/name, experiment,
  exported parent, profile, organization, metadata, and concurrent route
  isolation through Grok entry points.
- [ ] **CORR-01:** Prove daemon-captured process ancestry links nested Grok and
  cross-agent sessions only when unambiguous, including concurrent/restart
  cases; otherwise keep the session standalone.
- [ ] **OPS-01:** Include Grok in daemon/doctor delivery diagnostics with active
  sessions, pending delivery, last error, compatibility, activation/reload
  requirement, and permalink without payloads or credentials.

## P3: product surfaces

### Persistent setup

- [x] **SETUP-01:** Add Grok to `SetupAgent`, source/display mappings, public
  help, and runtime dispatch.
- [x] Implement marketplace/plugin install or update, enablement, repeated
  enable, route changes, and reversible disable/uninstall without touching
  unrelated Grok plugins, hooks, trust, or config.
- [x] Preserve Grok's trust boundary. Do not use global `--trust` as an
  unattended shortcut unless the product explicitly owns and documents that
  decision.
- [x] Persist only the shared non-secret route at `~/.grok/braintrust.json` with
  private atomic writes.
- [ ] **SETUP-02:** Add Grok doctor/status checks for executable/version,
  installed/enabled plugin, daemon/plugin compatibility, route/auth readiness,
  and Grok 1.0.13's required `/reload-plugins` activation workaround.

### Managed run

- [ ] **RUN-01:** Add Grok to `RunSource`, parsing, executable selection, source
  mapping, and help.
- [ ] Replace the development-only `local-dev.sh` mutation path with a safe
  invocation-local isolated home/plugin or hook injection that does not rewrite
  persistent setup and does not copy credentials into a less-safe location.
- [ ] Suppress inherited Braintrust capture only for the managed process tree,
  inject every required hook, preserve trust review, stdio, arguments, exit
  status, and interruption semantics, and flush only accepted run sessions.

### Import and attach

- [ ] **HIST-01:** Establish documented active/archive roots and deterministic
  session lookup for Grok's multi-file session directory before adding Grok to
  `ImportSource`.
- [ ] Implement one Grok parser that emits synthetic `grok` envelopes with
  bounded references to authoritative `updates.jsonl` evidence, the optional
  `system_prompt.txt` snapshot, and optional `events.jsonl` enrichment, matching
  live capture. Reuse `GrokTranslator`; do not build spans in the importer.
- [ ] **HIST-02:** Tail `updates.jsonl` incrementally with authoritative
  high-water marks and tail `events.jsonl` independently as optional
  enrichment; handle no-growth polls, partial writes, truncation, compaction,
  cancellation, resume, and finalization without duplicates.
- [ ] Test project/experiment/parent overrides without changing persistent Grok
  settings, plus invalid/ambiguous IDs and unsafe symlink traversal.

## P4: distribution and release evidence

- [ ] **SEC-02:** Document captured prompts, reasoning summaries, responses,
  file paths, tool payloads, transcript mirroring, retention, redaction limits,
  and the explicit sensitive-debug opt-in before release.
- [x] **DIST-01:** Replace `content/LICENSE` (`TODO Apache 2.0 (placeholder)`)
  with the real license and make validation reject placeholders.
- [x] Document supported Grok versions, operating systems, shell requirements,
  plugin activation/reload behavior, setup/run/import/disable commands, fidelity
  limitations, and troubleshooting. The current Bash adapter has no Windows
  compatibility statement or CI coverage.
- [ ] **DIST-02:** Add documented rollback and installed-version compatibility
  checks to the existing build/version/publish/tag/release flow.
- [ ] **TEST-03:** Add isolated product tests for first/repeated enable, route
  update, unrelated configuration preservation, disable, doctor, managed run,
  import, and attach.
- [ ] Add Grok minimum/current compatibility jobs and make CI install/report the
  Grok version rather than validating only JSON when Grok is absent.
- [ ] **TEST-04:** Add an installed-distribution real-Grok smoke that asserts
  root/turn/tool hierarchy, failure/cancellation, versions/provenance, and final
  delivery. A successful process exit alone is insufficient.
- [ ] **TEST-05:** Add release verification against a real Braintrust
  destination and query the resulting trace. Keep credentials confined to the
  host/release secret and skip cleanly when unavailable.

## Existing implementation worth preserving

- Raw stdin forwarding and fail-open exit behavior in
  `content/hooks/forward.sh`.
- Registration of all currently documented Grok lifecycle events in
  `content/hooks/hooks.json`.
- Daemon-owned incremental transcript mirrors and journaled high-water
  references in `dispatch.rs`/`transcript_mirror.rs`.
- Sink-neutral `SpanOp::Insert`/`Merge` translation and shared Braintrust sink.
- Native `toolCallId` correlation and aggregate usage remaining on the turn when
  multiple LLM rows are observed.
- Bounded recent completed-ID caches and deterministic incremental/full fixture
  replay.
- Shared route redaction, private daemon data directory, build validation,
  version stamping, dry-run publishing, and release tagging.

## Verification snapshot

- `make validate-grok`: **passed**; this covers package shape, hook
  registration, raw forwarding, and fail-open adapter behavior.
- `cargo +1.88.0 test --manifest-path bt-daemon/Cargo.toml --all-features
  --locked grok`: **passed** with 18 Grok tests across 16 test binaries. The
  explicit 1.88.0 toolchain is required because this workstation's default
  Rust 1.86.0 is older than the resolved `darling` dependency requires.
- Existing CI runs the complete Rust suite on stable Rust, but no CI job installs
  or exercises Grok. Repository coverage includes the Grok-specific packaged
  hook-to-journal-to-translator-to-sink pipeline test described above.
