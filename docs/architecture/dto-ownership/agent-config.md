# DTO inventory: agent schemas, Muse, setup and managed runs

Reviewed against `1b4a4e611f1a9bc73c796361c9d82cdbf52bb721` before this refactor. Paths, line numbers, and existing-method columns describe that baseline. See [the review summary](README.md) for changes made from these findings.

Scope: every production struct/enum/type declaration in `agents/**`, `muse/**`, `setup/**`, `managed_run/**`, `codex_hooks.rs`, `hook.rs`, `hook_executable.rs`, and `settings.rs`; macro-generated IDs and hook enums included. Test-only declarations are excluded below. DTO means native input, persisted state/config, authored output or transport shapes. Runtime services and capability marker types are separately classified, not silently omitted. Same-named agent schemas are not treated as shared contracts.

## Highest value behavior ownership

1. `CursorHooks`: `contains_registration`, `retain_commands`, `replace_registration`. Evidence: `setup/cursor/hooks.rs:124-129`, `186-199`, `209-250` repeatedly unwrap optional maps, distinguish known array/unknown event shapes, preserve unknown entries, and deduplicate commands. Configuration owns those mechanics; persistent marker/liveness/matcher policy stays setup. Preserve absent maps on disable, unknown arrays/entries, and existing version exactly.
2. `OpenCodeConfig`: `has_plugin`, `retain_plugins`, `replace_plugin`, `ensure_plugin` (or a minimal equivalent). `OpenCodePlugin::matches_package` recognizes the same package spec while deliberately excluding configured tuples, whose options must survive. Evidence: `setup/opencode.rs:32-81,105-114`, `managed_run/opencode.rs:72-89`. Setup owns bundled version and package policy; DTO owns optional list semantics and edits.
3. `OpenCodeTokens`: own `metrics`/prompt-inclusive-cache calculation, duplicated `translate/opencode.rs:1038-1044` and `translate/opencode_v2/span.rs:201-207`. `PiUsage` similarly own prompt/total and canonical metrics (`translate/pi.rs:769-771`). Native usage types already own parsing; consumers should not reconstruct cache accounting. Preserve signed native counters and existing optional total behavior; do not silently change overflow policy while extracting.
4. Antigravity `TranscriptRecord`: `normalized_kind`, `content`, `created_at_ms`. Current helpers `translate/antigravity.rs:669-715,815` depend only on record; importer uses kind/time/step too (`transcript_import/antigravity.rs:132-134,237-255`). Keep import-specific is_tool_result/is_denied policy in importer unless genuinely identical to reducer policy (not demonstrated).
5. `codex_hooks::Hook`: a shared `trust_identity()` for nonempty key/hash and `has_command_definition(expected_command)` would consolidate repeated structural validations in `managed_run_trust_state:255-309` and `trust_edits:327-389`. Keep full installed-file hash equality, sessionFlags/plugin source policies and error context in orchestrator; those trust policies are intentionally different.

## Module arrangement

Replace unrelated mega-file `agents/schema.rs` with `agents/schema/` and owner files (`hook_file`, `hook_group`, `command_hook`, `cursor_hooks`, `cursor_hook_entries`, `cursor_hook_entry`, `cursor_command_hook`, `opencode_config`, `opencode_plugin`, `plugin_manifest`, `repository`) with stable module reexports. Keep codec helper `deserialize_present` in module root or `json` rather than attach it to an unrelated DTO. `agents/usage/` can similarly separate `provider_usage`, `snake_case_usage`, `camel_case_usage`, `usage_metrics`, `token_metrics`, `opencode_tokens`, `pi_usage`; private leaf shapes stay with their owner until independently used. `muse/messages/` is worth splitting by message, content, image, and tool-call owners; single-use metadata projections can remain grouped by lifecycle domain. Do not create an impl or a public API merely to satisfy one-type-per-file. Generic `schema` modules should be compatibility facades, not the new dumping ground.

## Per-type review

Each row names an exact declaration. Caller descriptions below were traced to real owner modules; symbol-name grep alone is unreliable for generic names such as Hook/Session/Plugin/ContentBlock.

### `agents/schema.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `HookFile<Group>` | Codex hooks trust + managed commands; Cursor managed runs/setup parse packaged manifests; no methods. | Own event enumeration only if a caller needs normalization; current direct iteration is clear. Split owner file, retain generic shape. |
| `HookGroup` | Nested shared hook manifest group; Codex managed run rewrites its commands. | Data-only nested group; could own replace_commands delegation if multiple rewrite callers emerge. No invented builder. |
| `CommandHook` | Nested HookGroup command schema, rewritten in managed_run/codex.rs. | Own replace_command(unix,windows) if enforcing type==command; one current caller, moderate value. |
| `OpenCodeConfig` | Setup enables/updates/disables and managed run injects optional plugins list. | Expand list edit/lookup behavior; highest value #2. |
| `OpenCodePlugin` | Setup matching and managed run equality; package() already distinguishes configured options. | Expand package matching while preserving configured tuples; no conversion to string for configured variants. |
| `CursorHooks` | Setup persistent/temporary hooks and stale process cleanup; no methods. | Expand collection behavior; highest value #1. |
| `CursorHookEntries` | Native known array or unknown future payload; only setup traverses. | Keep enum data-only; collection mutation belongs CursorHooks, preserve Other unchanged. |
| `CursorHookEntry` | Known command vs unknown definition; command() already abstracts variant. | Existing behavior sufficient; equality/retention policy belongs owner. |
| `CursorCommandHook` | Constructed by setup and managed run, capture() centralizes defaults. | Existing capture constructor sufficient; registration comparisons belong CursorHooks. |
| `PluginManifest` | Hook version probe, setup common npm version, Cursor installation ownership. | Data-only manifest projection is appropriate; package identity policy differs by caller. |
| `Repository` | PluginManifest source string vs npm object; url() deliberately returns only supported string. | Existing method sufficient; do not normalize object repository into trusted plugin source inadvertently. |

### `agents/usage.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `ProviderUsage` | Antigravity record usage and metrics; custom boundary decode selects formats; metrics() exists. | Already owns behavior; retain. |
| `SnakeCaseUsage` | Codex/Claude counters; metrics() and is_empty() encode aliases. | Already owns behavior; split main owner file. |
| `TokenDetails` | SnakeCaseUsage nested cached/reasoning counters. | Data-only leaf; metric alias precedence belongs SnakeCaseUsage. |
| `CacheCreation` | SnakeCaseUsage nested TTL counters, roundtrip/Claude details. | Data-only leaf; keep with SnakeCaseUsage. |
| `CamelCaseUsage` | Grok translator + ProviderUsage; metrics() exists. | Existing metrics ownership sufficient; per-model aggregation remains translator policy. |
| `UsageMetrics` | Antigravity accumulated float metrics; is_empty/with_total/observe. | Already owns behavior, good model for others. |
| `TokenMetrics` | Muse and Cursor usage state; add() saturates optional counters. | Existing ownership good; split owner file. |
| `OpenCodeTokens` | OpenCode1 and2 native tokens. | Add canonical conversion/cache-inclusive prompt/total; highest value #3. |
| `OpenCodeCacheTokens` | Nested native cache read/write. | Data-only leaf with OpenCodeTokens, not independent service. |
| `PiUsage` | Pi normalized native counters, used response closure. | Add canonical metric/total methods; highest value #3. |
| `PiCost` | Nested PiUsage total price. | Data-only leaf; PiUsage decides emission. |

### `agents/request.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `RequestConfig` | Pi caches normalized provider request config; is_empty(). | Retain current behavior; moving native request parsing here may be useful but avoid provider redaction policy coupling. |
| `Stop` | Private string/list union. | Data-only wire variant. |
| `ToolDefinition` | Function/built-in; custom known-schema dispatch. | Serialization behavior already owned; no behavioral caller beyond codec. |
| `FunctionTool` | Private function tool envelope. | Data-only leaf. |
| `FunctionType` | Closed function tag. | Data-only discriminant. |
| `FunctionDefinition` | Function name/description/schema/strict/extensions. | Data-only leaf; JSON Schema intentionally arbitrary. |
| `ToolChoice` | Mode or named function. | Data-only wire union. |
| `ChoiceMode` | auto/none/required. | Data-only discriminant. |
| `NamedChoice` | Named function wrapper. | Data-only leaf. |
| `FunctionName` | Nested function name. | Data-only leaf. |
| `ResponseFormat` | Known/future union; custom dispatch. | Codec owns behavior already. |
| `KnownResponseFormat` | text/json_object/json_schema with unknown extension retention. | Data-only variant payloads. |
| `SchemaDefinition` | Named JSON Schema descriptor. | Data-only schema envelope; preserve Field nulls. |

### `agents/antigravity/schema.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `TranscriptRecord` | Live translator and import; fields read for kind/content/time/step. | Add record-local accessors noted #4; avoid moving importer state transitions onto DTO. |
| `ToolCalls` | One vs many native tool call; iter() shared translator/import. | Existing method sufficient. |
| `ToolCall` | Native name,args,output + opaque extensions, converted by translator/import. | Data-only: output conversion differs from invocation policy. |
| `optional_integer::Integer` | Private temporary string/number boundary codec. | Keep local, data-only, no standalone file. |

### `agents/cursor/schema.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `TranscriptRecord` | Cursor translator + transcript importer; role()/text() already encapsulate variants. | Already good behavior ownership. |
| `Role` | User/Assistant; as_str(). | Existing method sufficient. |
| `TranscriptMessage` | Nested content payload. | Data-only leaf. |
| `TurnEndedKind` | Closed turn marker. | Data-only discriminant. |
| `MessageContent` | Text/blocks; text() joins text blocks. | Already owns normalization. |
| `ContentBlock` | Text vs tool-use marker, text() owner skips tool blocks. | Data-only leaf; no unused API. |

### `muse/hooks.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `HookName (macro)` | Config event enumeration + decoder + translator; ALL/as_str/parse. | Already owns canonical aliases and names. |
| `MuseEvent (macro)` | Native decode, import synthesis, translation; name/context/payload/decode. | Already good aggregate behavior. |
| `Empty` | SessionStart empty body. | Data-only zero-size marker. |
| `Hook<B>` | Common context/body envelope, synthesized by import and decoded by MuseEvent. | Data-only generic container; no generic builder needed. |
| `HookContext` | Importer + translator correlation; turn()/request() filter empty. | Existing behavior sufficient. |
| `optional_id::Id` | Local tolerant native string/ignored codec. | Keep local, data-only. |
| `SessionMetadata` | Shared across native hook context and translator root; update() preserves missing/null. | Already owns update semantics. |
| `PromptSubmitted` | User prompt/image hook body; import constructs and translator reads. | Data-only; rendering belongs message/image layer. |
| `ModelDetails` | ModelStarted flattened counters/options. | Data-only; attribution/step identity belongs reducer. |
| `ModelStarted` | Import constructs; translator starts model span. | Could own borrowed input projection if duplicated; only one output mapping currently, no new method. |
| `ModelFinished` | Import constructs; translator closes model span. | Data-only outcome+usage envelope; completion lifecycle stays reducer. |
| `ToolStarted` | Shared tool-start payload, import and translator correlate. | Potential identifier/name helper only if repeated; current reducer chooses fallback IDs, keep policy there. |
| `ToolFinished` | Finish payload nested tool/outcome, import/tool lifecycle use. | Outcome classification belongs NativeOutcome; batch reducer behavior stays translator. |
| `ToolBatchFinished` | PostToolBatch Vec<ToolFinished>. | Data-only aggregate. |
| `PermissionObserved` | Tool metadata for permission hook; translator event branch reads. | Data-only. |
| `NotificationObserved` | Notification metadata hook; translator event branch reads. | Data-only. |
| `SubagentStarted` | Child session/subagent IDs used by import and translate events. | Candidate child_id() only if same precedence multiple callers; currently caller-specific identity, retain. |
| `SubagentFinished` | Child fields/output/outcome. | Data-only; reducer closes spans. |
| `Compaction` | Trigger/outcome. | Data-only; operation correlation belongs reducer. |
| `TurnFinished` | Assistant output/terminal/outcome. | Data-only; terminal lifecycle stays reducer. |
| `SessionFinished` | Reason/outcome. | Data-only; session shutdown behavior stays reducer. |

### `muse/messages.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `ToolInput` | Opaque tool-owned JSON wrapper used hook bodies and export synthesis. | Data-only newtype; do not add inspect-known-key methods. |
| `ToolOutput` | Opaque tool/provider output wrapper. | Data-only newtype. |
| `ModelOutput` | Text or typed messages, forwarded to output boundary. | Data-only native union; no duplicated rendering found. |
| `NativeMessage` | Messages in hook input, export output synthesis. | Could add assistant_tool_calls(calls) constructor for repeated synthesis; only one caller, not worth broad builder API. |
| `MessageRole` | Known/future role strings; From conversions. | Existing behavior sufficient. |
| `MessageContent` | Text or content blocks; retained losslessly. | Data-only; unlike Cursor, currently not repeatedly flattened to text. |
| `ContentBlock` | Known/future discriminated content; custom strict-known decoder. | Already owns codec behavior. |
| `KnownContentBlock` | Text/assistant/image payloads. | Data-only variant contents. |
| `MessagePhase` | Known/future assistant phase. | Data-only enum. |
| `ContentBlock::deserialize::Tag` | Temporary type discriminator. | Keep local, data-only. |
| `ImageSource` | Known base64/url/future; custom strict-known decoder. | Already owns codec behavior. |
| `Base64ImageSource` | Native encoded image descriptor. | Data-only leaf; no repeated extraction callers. |
| `Base64Type` | Closed base64 marker. | Data-only discriminant. |
| `UrlImageSource` | Native URL + extensions. | Data-only leaf. |
| `ImageUrl` | String or detailed URL descriptor. | Data-only union. |
| `ImageDetail` | auto/low/high/future. | Data-only discriminant. |
| `AssistantToolCall` | Import synthesizes function-call output records. | Possible constructor if repeated; currently one synthesis, keep data-only. |
| `FunctionCallType` | Closed function discriminator. | Data-only. |
| `FunctionCall` | Function name/arguments. | Data-only; args normalization belongs ToolArguments. |
| `ToolArguments` | Encoded JSON text or JSON; input() decodes/falls back. | Already owns behavior. |
| `ToolDescription` | Tool schema envelope for LLM input. | Data-only; parameter JSON Schema belongs native tool. |
| `InputSummary` | Bounded message count + extensions from hook/export. | Data-only. |

### `muse/provider_options.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `ProviderOptions` | Hook model details + final metadata, coexisting dotted options. | Data-only lossless schema; no repeated logic over options found. |
| `ReasoningEffort` | Known/future reasoning effort strings. | Data-only discriminant. |
| `ThinkingMode` | Known/future thinking mode strings. | Data-only discriminant. |

### `muse/outcome.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `ErrorDetail` | Hook/export errors; render(). | Already owns normalization. |
| `OperationStatus` | Known/future string statuses; From conversions. | Move failure membership into is_failure() used NativeOutcome (one caller but intrinsic status classification); useful if other consumers share exact classification. |
| `NativeOutcome` | Hook outcome across model/tool/subagent/turn/session; error_message() combines error/status/details. | Already owns common behavior. |
| `ToolOutcome` | Translator classified success/failure/unknown result. | Data-only semantic result, not raw DTO; no redundant methods. |

### `muse/usage.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `TokenUsage` | Hook and export counters, usage reducer; delta_since()/metrics(). | Already owns common behavior; good exemplar. |
| `UsageScope` | Turn-cumulative vs per-model marker. | Data-only enum; policy stays usage reducer. |

### `muse/ids.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `TurnId (macro)` | HookContext turn correlation; From/Deref/Display. | Already typed identity behavior, no extra methods. |
| `RequestId (macro)` | HookContext model request identity; From/Deref/Display. | Already sufficient. |
| `ToolCallId (macro)` | ToolStarted identity; From/Deref/Display. | Already sufficient. |

### `muse/export_v1.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `Header` | Import export header schema checks/session collection. | Data-only; import orchestrator validates supported version and sessions. |
| `Version` | String/object semver; into_string(). | Already owns wire normalization. |
| `Session` | Import session synthesis and finalization. | Data-only; lifecycle state belongs importer. |
| `SessionEnd` | Session exit reason. | Data-only leaf. |
| `UnixMicros` | Export timestamp; millis(). | Already owns units conversion. |
| `ExportRecord` | Import process() unwraps envelope, fallback recorded_at. | Candidate into_event() with timestamp precedence if reused; single importer, retain data-only. |
| `NativeEnvelope` | Record inner timestamp/payload. | Data-only leaf. |
| `NativePayload` | Kind-specific known parse/custom decoder. | Already owns codec dispatch. |
| `NativePayload::deserialize::Kind` | Temporary payload kind. | Keep local, data-only. |
| `NativePayload::deserialize::Payload` | Temporary raw fields for boundary dispatch. | Keep local, no standalone file. |
| `ExportMetadata` | Run-model metadata used importer current context. | Data-only; source precedence/update decisions belong importer. |
| `RunModelRecord` | Run stream+metadata envelope. | Data-only. |
| `RunStream` | Run ID wrapper object. | Data-only leaf. |
| `RunEvent` | Discriminated native stream event. | Data-only event variants; stream lifecycle stays importer. |
| `ExportToolCall` | Tool invocation list native call args. | Could own into_hook() only if it removes repeated import mapping; single site, retain. |
| `ExportToolResult` | Tool result event list. | Data-only. |
| `TaskEvent` | Task state event with ID/origin fallback read by process_task. | Candidate task_id() centralizes task_id/idempotency precedence only if shared; current one consumer. |
| `TaskEventKind` | Scheduled/start/complete/fail/cancel/reject. | Data-only; task terminal policy stays importer. |
| `TaskOrigin` | Originating tool-call ID. | Data-only leaf. |

### `muse/metadata.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `RootMetadata` | translate/muse.rs root metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ModelInput` | translate/muse.rs model input | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ModelStartMetadata` | translate/muse.rs model-start metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ModelEndMetadata` | translate/muse.rs model-end metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ToolStartMetadata` | translate/muse_events.rs tool-start metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ToolEndMetadata` | translate/muse_events.rs tool-end metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `ToolOutcomeLabel` | ToolEndMetadata success/error output label | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `SubagentMetadata` | translate/muse_events.rs child metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `CompactMetadata` | translate/muse_events.rs compaction metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `SessionEndMetadata` | translate/muse.rs session end metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `UnpairedMetadata` | translate/muse.rs unmatched model completion metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `PermissionMetadata` | translate/muse_events.rs permission metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `NotificationMetadata` | translate/muse_events.rs notification metadata | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |
| `CompletionFingerprint` | translate/muse.rs output/metrics/error/metadata dedup hash encoding | Retain data-only authored output projection; one construction site, serialization is its responsibility. Keep related lifecycle projections grouped, not independent builders. |

### `muse/config/hooks.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `Extensions` | Private unknown handler/group config map. | Alias stays data-only; restricted conservative extension scan needed. |
| `HookOverlay` | Setup manipulates user hooks; is_empty/contains_braintrust_hook/remove_owned/append_owned. | Already strong responsibility ownership; preserve exact foreign attributes. |
| `HookGroup` | Private handler group with matcher/extensions. | Move prune/remove_owned logic into private remove_owned() only if it meaningfully simplifies HookOverlay (moderate value). |
| `HookHandler` | Known user/enterprise or future handler; decoder/is_owned/contains_braintrust. | Already owns recognition and conservative inspection. |
| `CommandType` | Closed command tag shared strict ownership. | Data-only discriminant. |
| `UserCommand` | Owned user command with timeout/unknown attrs. | Data-only leaf; HookHandler owns variant recognition. |
| `EnterpriseCommand` | Foreign enterprise command. | Data-only leaf, never convert ownership. |
| `HookDocument` | Read-only root projection; contains_braintrust(). | Already owns inspection. |

### `muse/config/ownership.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `OwnedHookFile` | Exact current/previous/legacy file generation and recognition. | Already owns construction/equality; no new methods. |
| `OwnedHookGroup` | Strict private group shape. | Data-only leaf. |
| `OwnedCommandHook` | Strict private command shape. | Data-only leaf. |
| `HookFileInspection` | Missing/Owned/Foreign result; is_owned/contains_braintrust. | Already owns semantic result queries. |

### `muse/config/settings.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `MuseSettings` | Setup reads/writes; load/managed_path/hook_overlay/ensure_schema_version. | Already owns schema semantics and path resolution; writes require setup lock, keep transaction orchestration outside. |

### `settings.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `InvocationSettings` | Managed-run env protocol; enabled(route). | Existing constructor sufficient; conversion into AgentSettings could be From impl because duplicated load paths, verify before extracting. |
| `AgentSettings` | Hook/runtime/setup settings; load_for_hook/load/load_from/tracing_enabled. | Already owns load/enablement; move matching route replacement from migrate_persisted_route_at into method only if reused; file locks/atomic IO orchestration remain external. |

### `codex_hooks.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `ConfigRead` | activate app-server config/read response wrapper. | Data-only local protocol shape. |
| `Config` | features dictionary; activation checks hooks/codex_hooks. | Candidate hooks_enabled() if reused; only one check, retain. |
| `PluginList` | activate plugin/list response. | Candidate has_enabled(id) hides nested marketplaces traversal but single caller, low value. |
| `PluginMarketplace` | Nested plugin list. | Data-only leaf. |
| `Plugin` | Identity/installed/enabled native summary. | Data-only leaf; installation policy belongs activation. |
| `HooksList` | activate/run override native hooks/list. | Useful iter() flattening entries if reducing two loops; validation policies stay separate. |
| `HookEntry` | errors plus hook records; managed flow validates errors. | Data-only container; errors semantics differ setup/managed. |
| `Hook` | Repeated command/trust identity validation in trust_edits and managed_run_trust_state. | Add structural validation/identity methods only; keep separate source/full-file trust policies. See #5. |
| `TrustState` | Borrowed authored trusted_hash value. | Data-only output projection. |
| `request::Response` | Local RPC id/error/result wrapper in transport. | Data-only boundary codec; no reuse with unrelated wire Response. |
| `request::RpcError` | Local RPC error code/message. | Data-only transport projection. |

### `setup/claude.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `MarketplaceEntry` | Marketplace generic callbacks inspect name/source/repo. | Could own is_published_repo(repo) if multiple callers; one generic callback, data-only okay. |
| `InstalledPlugin` | enable/disable/update/stale find ID/version/enabled. | Data-only summary; caller owns plugin identity. |
| `InstalledPlugins alias` | Vec<InstalledPlugin>. | Data-only alias; no wrapper solely to add find_ours(). |
| `legacy_tracing_env_keys::Settings` | Read-only diagnostic env projection. | Keep local, data-only. |

### `setup/codex.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `MarketplaceList` | Generic marketplace find callback. | Data-only wrapper. |
| `MarketplaceEntry` | Native name/source fields. | Data-only; one generic callback. |
| `MarketplaceSource` | Native optional source string. | Data-only leaf. |
| `InstalledPlugins` | Native installed list wrapper read setup/stale. | Candidate find(id) only if broader list API needed; current private helper sufficient. |
| `InstalledPlugin` | Native plugin ID/version summary. | Data-only leaf. |

### `setup/grok.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `InstalledPlugin` | name/source used enable/update/disable publication checks. | Move is_published(repo) onto type if removing free helper; modest value, no generalized registry. |

### `setup/antigravity.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `update_at::PluginList` | Single plugin/list imports response. | Keep local data-only. |
| `update_at::Plugin` | Single native plugin name. | Keep local data-only. |

### `setup/mod.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `enable_tracing_at::LegacyFields` | Legacy route metadata/tags migration projection. | Keep local data-only; migration orchestration intentionally outside modern AgentSettings. |

### `setup/common.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `FileAccess` | Inherited/Private atomic edit permissions policy. | Data-only enum for IO helper, not native DTO. |
| `Marketplace<List,Entry>` | Generic runtime command plan with find/is_published callbacks; reconcile/refresh methods. | Runtime behavior object, not DTO; already owns operation. |

### `managed_run/mod.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `Terminal` | stdin/stdout terminal capability passed agent check. | Data-only value; no repeated common interpretation demonstrated. |
| `Injection` | Agent invocation args/env/lifetime guards consumed once by runner. | Runtime transfer object; no blanket builder. Methods append_env/args would mirror Vec without invariant. |
| `ManagedRunRuntime` | Host/data_dir runtime resource, start(). | Not DTO; runtime lifecycle object. |

### `managed_run/muse.rs`

| Type | Actual use and existing behavior | Judgment |
|---|---|---|
| `MuseManagedContext` | Serialized invocation-local route/socket/run ID/version; hook reads it. | Move load(path)/write_private(path) only if repeated secure-file lifecycle exists; currently split source/sink purposeful, data-only context file contract. |

## Exclusions and boundary accounting

Not DTOs: `agents::{Antigravity,Claude,Codex,Cursor,Grok,Muse,OpenCode,Pi}` are zero-size capability/registration implementations; `agents::{Agents<T>,Registrar}` are registries; `Identity` is an immutable registry value with `matches()` already; `muse::msp::{ListSessions,MuseMspClient}` are method marker/transport service; `setup::common::SystemCommandRunner` is a subprocess service; `setup::cursor::CursorManagedHooks` is an RAII cleanup guard with install/drop. No method-only refactor indicated for these. `muse/msp.rs` uses SDK DTOs (`SessionListParams`, `SessionListResult`, other imports): external generated contracts are not local DTO declarations and should not be forked.

Test-only declarations excluded: `hook::tests::HookCli`, `setup::common::FakeRunner`, `setup::cursor::tests::PublishedPlugin`, `setup::cursor::tests::Components`, `setup::antigravity::MissingAgyRunner`, `muse::msp::tests::TestTransport`. No DTO declarations exist in remaining hook executable helpers/setup agent files/managed-run agent files in this scope beyond types listed above.

Review discipline: methods should own a real schema invariant, conversion, validated lookup, lossless mutation or repeated projection. Avoid builders for single construction sites, aliases wrapping scalar getters, extracting runtime correlation onto wire types, guessing schema equivalence from same field names, and moving lock/process/environment policy onto data objects. Existing fixture/integration tests are the compatibility authority for unknown-field retention, null/missing, parent/child correlation, durable replay, and hook ownership.

## Implemented in this follow-up (agent-config ownership)

- Split `agents/schema.rs` into a directory with eleven owner modules (`hook_file`, `hook_group`, `command_hook`, `opencode_config`, `opencode_plugin`, `cursor_hooks`, `cursor_hook_entries`, `cursor_hook_entry`, `cursor_command_hook`, `plugin_manifest`, `repository`), stable reexports, and sibling tests. This reduces unrelated DTO navigation without changing schema names or serde layout.
- Added `CursorHooks::{contains_registration,retain_commands,replace_registration}` and used them for setup registration validation, stale managed-hook cleanup, persistent-hook removal and insertion. Marker recognition, process liveness, bundled matcher selection, filesystem paths and error context remain orchestration policy. Unknown event variants and unknown entries survive, disabling never creates an absent map/event, and complete registration equality includes timeout/matcher.
- Added `OpenCodeConfig::{find_plugin,retain_plugins,replace_plugin,ensure_plugin}` and `OpenCodePlugin::matches_package`; setup and managed runs now delegate list mechanics to their owner type. Configured tuples are deliberately not owned package entries. The managed-run method still matches exact specs and leaves differently configured specs alone.
- Added regression tests for missing maps, unknown event shapes/entries, command deduplication, untouched configured plugin options and version suffix vs similarly prefixed package names.
- Other per-type recommendations above are judgments/candidates, not claims of implementation. No blanket constructors, property getters, DTO networking, process liveness logic, trust-policy merging or reducer lifecycle migration were added. Token/Antigravity changes are coordinated with other reviewers; their final implementation should be reflected in the combined inventory.
