# Coding-agent tracing plugin specification

Status: **Draft**

This specification defines how a coding-agent integration captures native
activity and turns it into Braintrust traces in this repository. It is both the
implementation contract for a new integration and the checklist used to review
one.

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to
be interpreted as normative requirements.

## 1. Objective

A conforming integration produces a faithful, recoverable Braintrust trace of a
coding-agent session without changing the agent's behavior or placing
Braintrust credentials in the plugin.

The minimum useful trace is:

```text
Coding agent session (task)
└── Turn (task)
```

When native evidence exists, the trace expands to:

```text
Coding agent session (task)
└── Turn (task)
    ├── LLM call (llm)
    ├── Tool call (tool)
    ├── Permission decision (task)
    └── Subagent (task)
        └── ...nested activity
```

An integration MUST represent only facts present in native events or a vetted
native transcript. It MUST NOT invent model calls, token counts, timing,
permission decisions, or nesting.

## 2. Architecture and ownership

There is no declarative plugin schema that automatically creates all tracing
features. Each agent has a native event model, so each integration supplies a
small capture adapter and an agent-specific Rust translator.

The data path is:

```text
agent hook/plugin
  → raw native payload
  → bt trace hook
  → daemon Envelope + journal
  → agent-specific Translator
  → sink-neutral SpanOp rows
  → Braintrust sink / Rust SDK
```

Responsibilities are intentionally separated:

| Component                           | Owns                                                                | MUST NOT own                                          |
|-------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------|
| Agent hook/plugin                   | Native event capture and synchronous forwarding                     | Credentials, span construction, retries to Braintrust |
| `bt trace hook` / daemon wire layer | Envelope fields, route resolution, journaling, per-session ordering | Agent-specific payload semantics                      |
| Agent translator                    | Correlation, deterministic IDs, trace hierarchy, span content       | Credentials, HTTP delivery, plugin installation       |
| Sink                                | Braintrust SDK objects, batching, merge delivery, flushing          | Agent-native event interpretation                     |
| Setup/run/import commands           | Installation and invocation UX, non-secret routing                  | A second trace-building implementation                |

A plugin therefore does **not** send arbitrary spans directly to the Rust SDK.
It forwards opaque native events. Its registered translator interprets those
events and emits the common `SpanOp`/`SpanRow` model; the sink is the only layer
that talks to Braintrust.

Relevant implementation boundaries:

```text
src/plugins/<agent>/              deployable capture adapter
bt-daemon/src/wire/envelope.rs    common event and route contract
bt-daemon/src/translate/<agent>.rs agent-specific state machine
bt-daemon/src/translate/mod.rs    translator and span interfaces
bt-daemon/src/sink/               delivery through Braintrust
bt-daemon/src/setup.rs            persistent setup surfaces
bt-daemon/src/trace_runtime.rs    managed-run surfaces
bt-daemon/src/transcript_import/  transcript import/attach surfaces
```

## 3. Conformance levels

A capability marked “when observable” is required only if the agent exposes
reliable native evidence through hooks, an in-process API, or a vetted
transcript. A conformance report MUST mark unavailable capabilities **N/A** and
record the evidence for that conclusion.

### 3.1 Core tracing

Required before an integration is described as tracing-capable:

- Stable source identity and registered translator.
- Thin, fail-open capture adapter.
- Session root span.
- Per-turn task spans with user input and final assistant output when available.
- Deterministic IDs, ordered handling, bounded state, and safe flush behavior.
- Journal-safe non-secret routing.
- Translator fixtures and an envelope-to-sink pipeline test.

### 3.2 Full-fidelity tracing

Required when the corresponding native evidence is observable:

- LLM spans and token/cache metrics.
- Tool, web, and MCP spans.
- Permission requests and decisions.
- Subagent hierarchy.
- Compaction events.
- Failure and cancellation details.
- Session, version, workspace, and Git metadata.

### 3.3 Product-complete integration

Required before production release:

- Persistent enable, disable, and doctor/status support.
- Project, experiment, and parent-span routing supported by the shared command
  surfaces where applicable.
- Managed-run support when capture can be injected safely for one process tree.
- Transcript import and live attach when a stable, sufficiently complete
  transcript exists.
- Reproducible packaging, validation, publishing, and rollback.
- An installed-artifact smoke test using the real agent.

## 4. Source identity and registration

Each integration MUST choose one lowercase canonical source identifier. The same
identifier MUST be used by:

- capture commands;
- `TranslatorFactory::source()`;
- setup, run, status, and import registries;
- fixtures and pipeline tests;
- settings paths and documentation.

Aliases MAY exist at user-facing command boundaries, but MUST resolve to the
canonical identifier before an envelope enters the daemon.

The production translator registry MUST include the source. Unknown sources MAY
use the debug translator for diagnostics, but debug fallback does not constitute
support for an integration.

## 5. Capture adapter

### 5.1 Native events

The adapter MUST register every native event needed by its translator. It SHOULD
register all documented lifecycle events even before all are translated, so
journals can reveal payload evolution and later features do not require a new
capture path.

The adapter MUST forward the original native payload without renaming or
normalizing agent fields. Normalization belongs in the translator.

Hook-provider requirements and preferred native fields are described in
[`trace-coding-agents-with-hooks.md`](./trace-coding-agents-with-hooks.md).

### 5.2 Forwarding behavior

A command-hook adapter MUST:

- read one native payload from standard input;
- invoke `bt trace hook` with the canonical source and mappings for session ID,
  event name, and transcript path where available;
- block until local daemon acceptance at ordering-sensitive boundaries;
- quote paths and payloads safely;
- never print or persist credentials;
- exit successfully if `bt`, daemon startup, or forwarding fails, unless running
  in an explicit diagnostic/test mode.

An in-process adapter MUST obey the same ownership and failure rules. It MAY
buffer events only if ordering and process-exit flushing are proven.

### 5.3 Envelope contract

Every accepted event becomes an `Envelope` containing:

| Field | Requirement |
|---|---|
| `source` | Canonical source identity |
| `session_id` | Stable native session identity |
| `event` | Unmodified native event name |
| `ts_ms` | Epoch milliseconds stamped at capture time |
| `payload` | Raw native JSON payload |
| `source_version` | Agent version when available |
| `plugin_version` | Capture package version when available |
| `route` | Optional immutable, non-secret routing intent |
| `managed_run_id` | Set only by invocation-local managed runs |
| `capture` | Daemon-safe process ancestry, without command lines or environment |

Credentials MUST NOT be serializable in an envelope or journal. They are
resolved by the embedding `bt` host at delivery time.

## 6. Translator contract

A translator is one stateful `AgentTranslator` instance per daemon session. It
MUST map native events into `SpanOp::Insert` and `SpanOp::Merge` values and MUST
remain independent of Braintrust network delivery.

### 6.1 General rules

A translator MUST:

- tolerate unknown events and additive payload fields;
- tolerate a missing session-start event by opening the root on first useful
  activity;
- use capture timestamps rather than translator wall-clock time;
- derive stable IDs from native stable identifiers and the daemon session;
- merge completion data into an existing span rather than creating a second
  logical operation;
- preserve externally attached parent/root IDs from `SessionConfig`;
- merge `additional_metadata` without allowing it to override
  integration-owned keys;
- close or defensibly finalize open spans during flush;
- bound deduplication, open-operation, and transcript state;
- return an error for malformed events only when continuing would corrupt trace
  semantics; schema drift SHOULD otherwise degrade gracefully.

A translator MUST NOT:

- perform network I/O to Braintrust;
- resolve credentials;
- parse plugin installation state;
- depend on events being delivered exactly once;
- infer facts that are not supported by native evidence.

### 6.2 Session root

A session root MUST be a `task` span. Its ID MUST be deterministic for the
canonical source/session pair. It SHOULD contain:

- native session ID and source;
- agent and plugin versions;
- cwd and workspace/repository context;
- execution or permission mode;
- transcript location when safe and useful;
- Git revision, branch, and dirty state when available;
- route-provided custom metadata.

A process shutdown/session-end event closes the current process lifetime. Since
agents can resume sessions, the translator MUST remain replay-safe if later
activity reuses the native session identifier.

### 6.3 Turns

Each user request and the agent work attributable to it MUST form one child
`task` span. A turn SHOULD include:

- the submitted user input;
- the final assistant output;
- native turn/prompt ID;
- start and end capture timestamps;
- completion, cancellation, or failure reason.

Concurrent or overlapping turns MUST use native identifiers rather than a single
“current turn” slot. If an agent guarantees one active turn, the translator MAY
use a single slot and MUST test that assumption.

### 6.4 Operations

For every observable LLM, tool, web, MCP, permission, or subagent operation:

- start and stop events MUST correlate by a native operation ID;
- the span MUST be parented to the native turn that spawned it;
- nested operations MUST preserve native ancestry;
- input MUST come from the start/request event;
- output, error, metrics, and end time MUST come from completion evidence;
- missing completion MUST be handled during turn/session close or flush without
  fabricating successful output.

Use `llm` for model calls, `tool` for tool/web/MCP execution, and `task` for
turns, permissions, compaction, subagents, and other orchestration.

### 6.5 LLM metrics

When observable, an LLM span SHOULD include model/provider identity, full native
request and response bodies subject to data policy, input/output/total tokens,
cache read/write tokens, time-to-first-token, and finish reason.

Metric names and units MUST match the common sink conventions already used by
existing translators. Estimated token counts MUST be explicitly labeled and
MUST NOT replace native counts silently.

### 6.6 Failures and cancellation

A native failed operation MUST set `error` and preserve structured failure
fields in metadata or output as appropriate. Cancellation MUST not be labeled as
success. Translators SHOULD distinguish cancellation from failure with tags or
metadata when the common span model has no dedicated status field.

## 7. Routing, flushing, and recovery

Routes are immutable per session and contain only auth selection, destination,
flush mode, and additional metadata.

Supported destination forms are:

- project logs by project ID and/or name;
- an experiment by ID;
- an exported parent span.

`fire_and_forget` is the normal interactive default. `flush_on_turn_end` SHOULD
be available for validation, short-lived processes, and workflows requiring
immediate visibility.

The daemon journals accepted envelopes before relying on remote delivery.
Replaying a journal MUST converge on the same logical spans. This requires
stable span IDs and idempotent insert/merge behavior. Recovery and explicit
transcript import are separate mechanisms and MUST share the production
translator rather than separate trace builders.

## 8. Product surfaces

### 8.1 Persistent setup

`bt trace enable <agent>` MUST install or update capture idempotently and store
only non-secret route settings. It MUST preserve unrelated user configuration.

`bt trace disable <agent>` MUST remove only Braintrust-managed state and MUST be
safe when repeated.

Doctor/status output SHOULD diagnose:

- whether the agent is installed;
- whether capture is installed and enabled;
- source/daemon/plugin version compatibility;
- route validity and credential availability without exposing credentials;
- daemon reachability;
- agent-specific activation requirements such as restart or plugin reload.

### 8.2 Managed run

`bt trace run <agent>` MUST isolate its route to one process tree, inject capture
without rewriting persistent setup, suppress inherited duplicate capture, and
preserve the child process's exit status and signal behavior.

If safe invocation-local injection is impossible, managed run MUST be marked N/A
with evidence rather than approximated by mutating global configuration.

### 8.3 Import and attach

Import is permitted only when a native transcript has enough stable information
to produce the same trace semantics as live capture. Import and live attach
MUST use one parser and the production translator. Attach differs only by
incremental waiting/finalization.

Session lookup MUST be deterministic and destination/parent overrides MUST not
rewrite persistent settings. Truncated records, resumed sessions, duplicate
records, and partial final writes MUST be tested.

## 9. Security and data handling

- Capture adapters MUST be credential-free.
- API keys and OAuth tokens MUST never appear in plugin settings, envelopes,
  journals, logs, command lines, or span metadata.
- Journals, debug logs, transcript mirrors, and temporary configs MUST use
  user-private permissions.
- Raw prompts, responses, tool payloads, and transcripts MUST be treated as
  sensitive.
- Full raw payload logging MUST be opt-in and documented.
- An integration MUST document what content it captures and any redaction
  limitations before release.
- Fail-open applies to tracing failures, not to bypassing the coding agent's own
  permission or trust model.

## 10. Verification requirements

### 10.1 Translator fixtures

Fixtures MUST cover:

- normal root/turn lifecycle;
- every translated operation type;
- failures and cancellation;
- duplicate events;
- missing start and missing stop boundaries;
- unknown events and additive schema drift;
- external parent/root attachment;
- custom metadata precedence;
- deterministic replay and flush;
- bounded-state behavior where practical;
- relevant agent-version variants.

### 10.2 Pipeline tests

Tests MUST exercise:

```text
native payload → adapter → Envelope → journal → translator → sink rows
```

They MUST verify route isolation, credential redaction, ordering, restart
recovery, and process-exit flush behavior. Debug-sink tests are necessary but do
not replace a Braintrust ingest test.

### 10.3 Product tests

A product-complete integration MUST test enable, repeated enable, route changes,
disable, doctor/status, and managed run independently. Import/attach MUST be
tested when supported.

Before release, CI or documented release verification MUST install the packaged
artifact into an isolated agent home, run the real agent, and confirm the
resulting Braintrust trace hierarchy and content.

Standard repository checks are:

```bash
make build
make test
cargo test --manifest-path bt-daemon/Cargo.toml --all-features
cargo fmt --manifest-path bt-daemon/Cargo.toml --all -- --check
cargo clippy --manifest-path bt-daemon/Cargo.toml --all-targets --all-features -- -D warnings
```

## 11. Conformance report template

Every integration SHOULD maintain this table in its plugin documentation or
tracking issue. Do not claim a row based only on code presence; link or name the
verification evidence.

| Capability | Status | Evidence / blocker |
|---|---|---|
| Source registration | Not started | |
| Native event audit | Not started | |
| Fail-open capture | Not started | |
| Session root | Not started | |
| Turn spans | Not started | |
| LLM spans/metrics | Not started | |
| Tool/web/MCP spans | Not started | |
| Permission events | Not started | |
| Subagents | Not started | |
| Compaction | Not started | |
| Failures/cancellation | Not started | |
| Session/Git/custom metadata | Not started | |
| Deterministic replay and bounded state | Not started | |
| Project/experiment/parent routing | Not started | |
| Enable/disable/doctor | Not started | |
| Managed run | Not started | |
| Import/live attach | Not started | |
| Security and data-handling review | Not started | |
| Build/validation/publishing | Not started | |
| Translator and pipeline tests | Not started | |
| Installed real-agent smoke | Not started | |

Allowed statuses are **Done**, **Partial**, **Not started**, **Blocked**, and
**N/A**. Partial, Blocked, and N/A entries MUST explain what remains or why the
capability cannot apply.

## 12. Boundaries for new integrations

Always:

- preserve one source identity and one production translator;
- capture raw native evidence before designing mappings;
- add fixtures from real payloads;
- keep adapters thin, synchronous where ordering matters, and fail-open;
- use shared daemon routing, journaling, IDs, metadata, and sink abstractions.

Ask first:

- before changing the common `Envelope`, `SpanRow`, or route schema;
- before adding a second capture path or transcript-derived data to live hooks;
- before adding a dependency, credential flow, or cross-agent correlation rule;
- before declaring an observable feature N/A.

Never:

- put Braintrust SDK calls or trace construction in an agent plugin/hook;
- implement a second translator for import or managed run;
- persist credentials;
- infer unavailable model/tool/token data;
- let tracing failure block or alter the coding agent.

## Appendix A: Coding-agent feature index (non-normative)

This index gives coding-agent integrations a stable vocabulary and a suggested
implementation sequence. It intentionally does not track which agents implement
each feature; integration status and evidence belong in the conformance report
from section 11. The stages are guidance rather than hard dependencies, and
features that require native evidence remain conditional under section 3.

| Stage                 | ID       | Feature                                     | Description / acceptance notes                                                                                                                                                         |
|-----------------------|----------|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1. Feasibility        | AUDIT-01 | Native event and transcript audit           | Pin the agent version; inventory hooks, plugin APIs, transcripts, IDs, timestamps, ordering, recursion, and schema stability before designing spans.                                   |
| 1. Feasibility        | AUDIT-02 | Capability and fidelity report              | Mark each observable feature supported, unavailable, or unknown, and record where fidelity is aggregate, reconstructed, delayed, or version-dependent.                                 |
| 1. Foundation         | ID-01    | Canonical source and production translator  | One lowercase source identity selects one agent-specific production translator at every live, managed-run, and import ingress.                                                         |
| 1. Foundation         | CAP-01   | Thin fail-open live capture                 | Native activity reaches the daemon without credentials or trace construction, and tracing failures never interrupt the agent.                                                          |
| 1. Foundation         | CAP-02   | Complete native event forwarding            | Capture registers every useful lifecycle event and forwards original payloads, stable IDs, timestamps, versions, transcript paths, and safe process context.                           |
| 1. Foundation         | CAP-03   | Ordered blocking delivery                   | Ordering-sensitive boundaries wait for local daemon acceptance; quoting, reconnection, timeout, and process-exit behavior are safe.                                                    |
| 1. Foundation         | SEC-01   | Credential-free capture and routes          | Plugins, envelopes, journals, temporary configuration, command lines, and span metadata contain no API keys or OAuth tokens.                                                           |
| 1. Foundation         | REL-01   | Durable event journal                       | Every accepted envelope is journaled before remote delivery using a route-safe representation suitable for deterministic recovery.                                                     |
| 1. Foundation         | SINK-01  | Idempotent span delivery                    | Sink-neutral insert and merge operations support batching, late completion updates, retries, and stable remote identity without agent-specific network code.                           |
| 1. Foundation         | ROUTE-01 | Project routing                             | A session can resolve and pin a Braintrust project destination without placing credentials in agent configuration.                                                                     |
| 2. Minimum trace      | TRACE-01 | Deterministic session root                  | A replay-safe task span represents the session, preserves external attachment, and can be extended after process shutdown or resume.                                                   |
| 2. Minimum trace      | TRACE-02 | Per-turn task spans                         | Each user request has a child task span containing native input, final output, timing, and completion status when observable.                                                          |
| 2. Minimum trace      | META-01  | Session, version, and workspace metadata    | The root includes available native session ID, source and plugin versions, cwd, workspace, execution mode, and transcript provenance.                                                  |
| 2. Minimum trace      | REL-02   | Deterministic IDs and duplicate handling    | Stable native identities and deterministic fallbacks make duplicate delivery and journal replay converge on the same logical spans.                                                    |
| 2. Minimum trace      | REL-03   | Bounded correlation state                   | Prompt, operation, transcript, and deduplication state have explicit bounds without dropping currently open work silently.                                                             |
| 2. Minimum trace      | TRACE-14 | Concurrent activity correlation             | Overlapping turns and operations correlate by native identity rather than a single implicit current slot; ambiguous ancestry fails safe.                                               |
| 2. Minimum trace      | TRACE-08 | Failure and cancellation closure            | Failed, cancelled, interrupted, and incomplete turns close defensibly and are never labeled as successful.                                                                             |
| 2. Verification       | TEST-01  | Translator fixture suite                    | Fixtures cover normal root/turn flow, missing lifecycle boundaries, duplicates, attachment, metadata precedence, flush, replay, and schema drift.                                      |
| 2. Verification       | TEST-02  | Envelope-to-sink pipeline test              | Tests exercise native payload capture, envelope creation, journaling, translation, sink rows, ordering, route isolation, and process-exit flush.                                       |
| 3. Operations         | TRACE-04 | Tool spans                                  | Native tool IDs correlate start and completion; spans preserve input, output, timing, kind, failure, and missing-boundary behavior.                                                    |
| 3. Operations         | TRACE-09 | Web and MCP classification                  | Web search, fetch, and MCP operations remain tool spans but retain their native transport, server, method, and operation classification when observable.                               |
| 3. Operations         | TRACE-05 | Permission requests and decisions           | Native permission requests and allow, deny, modification, or cancellation outcomes are represented without inferring unobserved decisions.                                             |
| 3. Operations         | TRACE-10 | Skill attribution                           | Explicit skill loads or invocations are represented as task spans or operation metadata and remain distinguishable from ordinary tools.                                                |
| 3. Model fidelity     | TRACE-03 | LLM call spans                              | Observable model-call boundaries become LLM spans with native model/provider identity and request/response content; unavailable inputs are explicit rather than reconstructed as fact. |
| 3. Model fidelity     | TRACE-11 | Token and cache metrics                     | Native prompt, completion, total, cache-read, cache-write, and reasoning token counts use common metric names; aggregate usage is never guessed across calls.                          |
| 3. Model fidelity     | TRACE-12 | Latency, finish, and cost metrics           | Time-to-first-token, API duration, finish reason, estimated/native cost, retries, and model fingerprints are recorded only when native evidence exists.                                |
| 3. Orchestration      | TRACE-06 | Subagent hierarchy                          | Child and recursively nested agent activity preserves native ancestry and nests beneath the turn or operation that spawned it.                                                         |
| 3. Orchestration      | TRACE-13 | Background and asynchronous work            | Background tools, tasks, workflows, or delayed completions retain identity and remain open until native completion, cancellation, or defensive flush.                                  |
| 3. Orchestration      | TRACE-07 | Compaction and session-tree spans           | Native compaction, branching, and session-tree activity becomes orchestration spans with before/after context only when observable.                                                    |
| 3. Metadata           | META-02  | Shared Git metadata                         | Repository root, revision, branch, remote, and dirty state are enriched through the shared Git metadata cache when a worktree is available.                                            |
| 3. Metadata           | META-03  | Route-provided custom metadata              | Custom route metadata reaches the root while integration-owned keys retain precedence and internal routing keys stay hidden.                                                           |
| 3. Metadata           | META-04  | Session summary metrics                     | Native totals such as turns, tool calls, tokens, duration, and cost can be merged into the root without double counting resumed activity.                                              |
| 4. Transcript support | HIST-00  | Durable transcript mirroring                | Mutable transcripts are copied incrementally into private daemon storage, and journaled high-water references bound live and replay observations.                                      |
| 4. Transcript support | REL-04   | Incremental and partial-record parsing      | Transcript readers tolerate append-only growth, partial final records, truncation, compaction, large catch-up batches, and additive schema changes.                                    |
| 4. Reliability        | REL-05   | Safe turn and session flushing              | Interactive delivery may batch, while turn boundaries, managed runs, idle retirement, shutdown, and explicit flush drain all accepted work.                                            |
| 4. Reliability        | REL-06   | Restart recovery                            | Daemon restart replays journal and transcript mirrors without duplicate spans, lost completions, route drift, or resurrection of closed operations.                                    |
| 4. Reliability        | REL-07   | Session resume semantics                    | A resumed native session extends the existing trace safely after a prior process lifetime closed, including late records and repeated shutdown events.                                 |
| 4. Routing            | ROUTE-02 | Experiment and exported-parent routing      | Sessions can target an experiment or attach beneath an exported span while preserving the externally supplied trace root.                                                              |
| 4. Routing            | ROUTE-03 | Profile, organization, and route isolation  | Concurrent sessions can use distinct profiles, organizations, destinations, and metadata without credentials or events crossing routes.                                                |
| 4. Correlation        | CORR-01  | Cross-agent parent-child linking            | Process ancestry and native evidence can link child coding-agent sessions beneath spawning tools, including concurrent, recursive, and restart cases, while ambiguity fails safe.      |
| 4. Operations         | OPS-01   | Daemon status and delivery diagnostics      | Operators can inspect daemon reachability, active sessions, pending delivery, last error, compatibility, and permalink without exposing credentials or payloads.                       |
| 4. Operations         | OPS-02   | Opt-in sensitive diagnostics                | Raw event and transcript diagnostics are private, bounded, clearly documented as sensitive, and disabled unless explicitly requested.                                                  |
| 5. Product            | SETUP-01 | Persistent enable and disable               | Setup installs or updates capture idempotently, stores only non-secret routing, preserves unrelated configuration, and removes only Braintrust-managed state.                          |
| 5. Product            | SETUP-02 | Agent-specific doctor/status                | Diagnostics cover agent installation, capture activation, plugin and daemon compatibility, route validity, credential availability, and restart or reload requirements.                |
| 5. Product            | RUN-01   | Invocation-local managed run                | One process tree receives an isolated route and temporary capture without mutating persistent configuration or duplicating inherited capture.                                          |
| 5. Product            | HIST-01  | Historical transcript import                | Stable native sessions can be located safely and replayed through the production translator with project, experiment, or parent overrides.                                             |
| 5. Product            | HIST-02  | Live transcript attach                      | The import parser can follow active sessions incrementally and finalize partial, cancelled, compacted, or resumed sessions without duplicates.                                         |
| 5. Verification       | TEST-03  | Product-surface tests                       | Enable, repeated enable, route changes, disable, doctor, managed run, import, and attach are tested independently in isolated agent homes.                                             |
| 6. Security           | SEC-02   | Sensitive-data policy and filesystem safety | Documentation states captured content and redaction limits; journals, transcript mirrors, logs, and temporary files use private permissions and safe path handling.                    |
| 6. Distribution       | DIST-01  | Reproducible package and validation         | A deterministic artifact contains every required manifest, hook, adapter, license, and document and validates independently of the source tree.                                        |
| 6. Distribution       | DIST-02  | Versioning, publishing, and rollback        | Agent and plugin compatibility, release versions, generated distribution repositories or registries, release automation, and rollback are defined and reproducible.                    |
| 6. Verification       | TEST-04  | Automated installed real-agent smoke        | Automation installs the packaged artifact, runs the real agent, and asserts trace hierarchy, content, failures, and terminal delivery.                                                 |
| 6. Verification       | TEST-05  | Real Braintrust ingest smoke                | Release verification confirms the packaged integration can deliver and query a trace through a real Braintrust destination, not only a debug or mock sink.                             |

The order favors a usable root-and-turn trace early, then adds operation fidelity,
reliability, product surfaces, and release evidence. Tests should be added with
each feature rather than deferred until the verification rows. When one row grows
to describe independently testable behavior, split it into stable feature IDs.
