# DTO ownership inventory: translators, import, and transcript mirrors

Reviewed against `1b4a4e611f1a9bc73c796361c9d82cdbf52bb721` before this refactor. Paths, line numbers, and existing-method columns describe that baseline. See [the review summary](README.md) for changes made from these findings.


Scope: all production struct/enum/type declarations in `bt-daemon/src/translate/**`, `transcript_import/**`, and `transcript_mirror.rs`, including function-local DTOs. Reviewed against source callers, not merely derives. Test-only fixtures are excluded; runtime state declarations are explicitly accounted for below. Native schema types imported from agents/muse are owned by the other inventory scopes. This is the pre-refactor inventory; see [the review summary](README.md) for the implementation decisions.

## Decisions and module plan

1. Put interactions on the type that owns their policy, rather than adding mechanical getters. Highest-value changes: SkillLoad skill metadata + display name; Pi AssistantMessage response-model selection and normalization; Pi compaction input/summary projection; OpenCode PermissionProperties request normalization; OpenCode2 Context/native tool content conversion; TranscriptPayload reference selection. Existing Cursor Hook/Specialized and OpenCode2 span Message/ToolCall are good models.
2. DTOs used once solely to deserialize/serialize fields should remain data-only. A one-method wrapper around serde or a DTO getter that duplicates public field access adds no ownership. Keep provider-native models separate from span export models and actor continuation state.
3. Prefer one principal type per file with small private companions beside the owner: `pi/assistant_message.rs` + `pi/compaction.rs` + `pi/event.rs`; `opencode/permission.rs`; `opencode_v2/context.rs` for native context family and normalization; `tool/skill_load.rs`; `transcript_mirror/{reference,payload,observation,snapshot}.rs` with the I/O engine in mod.rs. Avoid 40 tiny files for OpenCode2 one-shot event payload wrappers.
4. Preserve unknown extensions, authored null/missing, native event ordering, save/restore wire JSON, capture absence vs noncaptured fallback, and replay high-water bounds. Existing fixtures and all-agent replay coverage are needed after moving code.

Rows below list declarations individually, local consumers/nearby functions found by symbol reference (up to four examples), existing impl methods, and a concrete decision. For nested DTOs the enclosing caller is listed. Consumer line numbers reflect pre-refactor source.


## bt-daemon/src/transcript_import/claude.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `Subagent` :348 | subagents@364 | none | KEEP parser result data-only: one builder subagents(), one event emission loop. Hierarchy traversal belongs importer; no independent methods justified. |

## bt-daemon/src/transcript_import/codex.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `SpawnCall` :327 | spawn_calls@336 | none | KEEP parser result data-only: one spawn_calls producer and child import consumer, no repeated policy. |

## bt-daemon/src/transcript_import/mod.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `TranscriptLayout` :32 | resolve_transcripts@107 | `discover`, `find`, `candidates`, `single`, `locations` | KEEP existing discover/find/candidates methods: good owner of transcript filename/root search. Move `transcript_layout.rs` if splitting import mod. |
| `Refresh` :248 | refresh_with_final_partial@260, poll@473 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |

## bt-daemon/src/transcript_import/muse.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `ReadMode` :19 | finish@543, for_each_envelope_for_session@652, for_each_snapshot_envelope_for_session@661 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |

## bt-daemon/src/transcript_import/pipeline.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `ImportTarget` :20 | run_import@33, import_transcript@145, import_transcripts@195, new@247 | none | KEEP data-only host/socket/route launch context. Do not hide process startup behind DTO methods; clients already own it. |

## bt-daemon/src/transcript_mirror.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `Reference` :25 | schema composition@34, read_path@50, is_empty@89 | `captured`, `read_path` | KEEP existing captured/read_path and ADD explicit complete-capture/path-bound selection only where replay semantics match. Split to transcript_mirror/reference.rs (parent owns). |
| `GrokReferences` :48 | read_path@57, is_empty@93 | `is_empty` | KEEP is_empty; no fake common iteration across heterogeneous slots, split with reference family. |
| `Observation` :64 | is_empty@95 | none | KEEP data-only; Antigravity path/bound precedence can be owned by TranscriptPayload::antigravity_source if shared, avoid filesystem I/O in DTO. |
| `Snapshot` :72 | is_empty@97 | none | KEEP data-only; content is immutable input to bounded reader. Separate snapshot.rs only if module makes lookup easier. |
| `TranscriptPayload` :80 | is_empty@112 | `decode` | ADD explicitly named Claude reference/through/native-path access methods used dispatch, server observer and readers. Preserve map-presence vs missing-entry semantics, captured absence, replay fallback. Split payload.rs (parent owns). |
| `ClaudeChildReference` :107 | is_empty@86 | none | KEEP data-only child path/offset pair; co-locate Reference family, no duplicated observer/runtime state. |

## bt-daemon/src/translate/antigravity.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `HookPayload` :27 | ensure_root@131, pre_invocation@270, post_invocation@321, pre_tool@346 | none | ADD Antigravity `tool_approval()` by moving tool_approval_from_hook only if splitting hook schema; one call so low priority. Keep other fields data-only. |
| `Message` :44 | create@97, transcript_message@678 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `TranscriptSource` :580 | transcript_source@587 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `ToolDetails` :733 | tool_details@740 | none | KEEP projection result data-only; tool_details interprets source TranscriptRecord and belongs that schema only if truly reused, not result DTO. |

## bt-daemon/src/translate/claude.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `HookContext` :30 | ensure_root@395, tail_main@458, observe_session_details@473, handle@1507 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `SubagentHook` :48 | parent_for@727, ensure_subagent@739, stop_subagent@968, handle@1514 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `TranscriptEnvelope` :60 | assistant_request_id@1764, parse_transcript@1793, is_real_user_record@2041 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `TimestampEnvelope` :73 | parse_timestamp_ms@2656 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `AgentOrigin` :177 | begin_compacted@288, ensure_subagent@750 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `ParsedTranscript` :1779 | emit_next_pending@1103, parse_transcript@1785 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `UsageRecord` :2162 | observe@2169 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `UsageMessage` :2166 | observe@2163 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |
| `TranscriptToolOutcome` :2254 | parse_transcript@1918, transcript_tool_outcome@2261 | none | KEEP data-only partial routing/parse result; reducer/parser owns correlation; put native envelopes in claude/hook.rs or transcript.rs if splitting, preserve isolated timestamp decode. |

## bt-daemon/src/translate/codex/tool_output.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `ToolOutcome` :13 | classify@19 | `command`, `observe`, `observe_text`, `observe_command_header` | KEEP observe/command internal methods; optionally make classify an associated constructor because all work builds this result, but not high-value compared with native DTO conversions. |

## bt-daemon/src/translate/codex.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `SessionStartHook` :54 | handle@290 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `CompactHook` :62 | handle@302, record_compaction_trigger@517 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `SubagentStartHook` :69 | handle@297, handle_subagent_start@562 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `RolloutRecord` :79 | catch_up_chunk@661, process_record@672 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `PayloadKind` :94 | process_record@733 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `WorkingDirectoryPayload` :100 | process_record@681 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `TurnContextPayload` :106 | process_record@689 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `TokenCount` :1292 | close_llm_with_tokens@1299 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `TokenCountInfo` :1296 | close_llm_with_tokens@1293 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |
| `ReadLines` :2161 | read_new_lines@2172 | none | KEEP data-only partial routing/parse result; one decode path handles it. Group hook fields in codex/hook.rs and rollout fields in codex/rollout.rs, no trivial accessors. |

## bt-daemon/src/translate/cursor/hooks.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `Hook` :20 | schema composition@28 | `decode`, `generation_id`, `model`, `model_params`, `cwd`, `tool_use_id` | KEEP decode/generation_id/model/model_params/cwd/tool_use_id; strong existing ownership. Split hook.rs plus hook_common.rs, preserve central event decode. |
| `HookCommon` :90 | schema composition@23 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `DaemonFields` :101 | decode@39 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `TranscriptMirror` :109 | schema composition@25, tool_use_id@103 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `EmptyObject` :120 | tool_use_id@115 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `CursorHook` :123 | schema composition@21, decode@31, cwd@66, tool_use_id@81 | `decode` | KEEP decode; enum owns native event discrimination. Avoid one file per zero-data event. |
| `PromptHook` :188 | tool_use_id@125 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ToolPhase` :193 | tool_use_id@126, decode@147 | `duration` | KEEP duration; add terminal/error interpretation only if reducers repeat exact branching. |
| `ToolHook` :202 | tool_use_id@126, decode@215 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `PostToolHook` :213 | decode@149 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ToolResult` :221 | decode@196 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ToolFailureHook` :228 | decode@153 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ToolFailure` :236 | decode@197 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ThoughtHook` :255 | tool_use_id@127 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ResponseHook` :260 | tool_use_id@128 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `StopHook` :267 | tool_use_id@129 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `TurnUsage` :276 | duration@263 | none | ADD metrics() only if not already covered agents usage convergence; Cursor reducer currently projects totals to standard token names. Good family-level helper, no field getters. |
| `SessionEndHook` :284 | tool_use_id@130 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `SubagentStartHook` :292 | tool_use_id@131 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `SubagentStopHook` :302 | tool_use_id@132 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `Specialized` :318 | tool_use_id@82, decode@164, duration@363 | `tool_use_id`, `accepts`, `matches_input`, `result` | KEEP tool_use_id/accepts/matches_input/result; excellent owner of native differences. Split specialized.rs and related leaf payloads. |
| `ShellHook` :326 | duration@319 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `McpHook` :333 | duration@320 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `FileEditHook` :343 | duration@321 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `FileEdit` :350 | decode@170, duration@321, tool_use_id@368, accepts@381 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ReadFileHook` :356 | duration@322 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ImportBoundary` :431 | tool_use_id@135 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |
| `ImportStopHook` :439 | tool_use_id@137 | none | KEEP data-only native leaf payload: Hook/CursorHook/ToolPhase/Specialized already own common interactions. Group with its principal owner. |

## bt-daemon/src/translate/cursor/mod.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `ModelContent` :121 | value@134, encoded_len@150, push@156, model_content@551 | `text`, `encoded_len` | KEEP as data-only restoration representation; ModelStep owns traversal/history behavior. |

## bt-daemon/src/translate/decode.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `DecodeError` :14 | schema composition@5, fmt@32, decode@43 | none | KEEP constructor/Display/Error behavior; already dedicated file. |

## bt-daemon/src/translate/git.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `FileStamp` :13 | schema composition@21, stamp@262 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `GitFingerprint` :20 | is_empty@44, fingerprint@246 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `GitMetadata` :29 | schema composition@34, is_empty@45, metadata@113, merge_metadata@183 | `is_empty` | KEEP `is_empty`; optionally add `merge_into(metadata)` to own git key serialization currently in GitMetadataCache::enrich. Keep file/process discovery in cache. |

## bt-daemon/src/translate/grok.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `NativeRecord` :34 | remember@286, process_update@490, process_event@927, record_ts_ms@1584 | none | ADD `timestamp_ms()` by moving record_ts_ms; encapsulates native metadata/RFC3339/epoch fallback. NativeParams/Meta remain data-only. |
| `NativeParams` :53 | schema composition@36 | none | KEEP data-only native nested payload or bounded-read result; parent NativeRecord/Update or transcript I/O owns interpretation. Group in grok/native.rs or reader.rs. |
| `NativeUpdate` :63 | schema composition@55, tool_failure_text@1641 | none | ADD `failure_text()` if touched; helper currently takes only this DTO and interprets its content/raw output. Raw tool data remains a boundary. |
| `NativeMeta` :109 | schema composition@57, turn_key@1559 | none | ADD `turn_key(sequence)` if touched, because prompt-id/prompt-index fallback belongs to identity-bearing DTO. No need to add getters for every field. |
| `PromptIndex` :125 | schema composition@113 | none | KEEP data-only native nested payload or bounded-read result; parent NativeRecord/Update or transcript I/O owns interpretation. Group in grok/native.rs or reader.rs. |
| `TurnCompletion` :226 | process_update@915, close_turn@1089, close_incomplete_turn@1143 | none | KEEP data-only native nested payload or bounded-read result; parent NativeRecord/Update or transcript I/O owns interpretation. Group in grok/native.rs or reader.rs. |
| `TranscriptRecord` :284 | remember@291, read_new_records@1502 | none | KEEP data-only native nested payload or bounded-read result; parent NativeRecord/Update or transcript I/O owns interpretation. Group in grok/native.rs or reader.rs. |
| `TranscriptBatch` :289 | read_new_records@1420 | none | KEEP data-only native nested payload or bounded-read result; parent NativeRecord/Update or transcript I/O owns interpretation. Group in grok/native.rs or reader.rs. |

## bt-daemon/src/translate/mod.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `SpanType` :57 | local_username@93 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `OriginSnapshot` :68 | local_username@132 | none | KEEP immutable data-only, version-stamping ownership already in origins/dispatcher. Group with span row if not independently useful file. |
| `LateMergeVersion` :77 | local_username@116 | none | KEEP data-only causal watermark; ordering decisions belong delivery ledger, not generic lexicographic derive. |
| `SpanRow` :85 | schema composition@6, local_username@141, row@147, row_mut@152 | none | KEEP publicly constructible data; consider metadata mutation helper only for structurally identical cross-cutting operations. Never move sink policy/redaction into row. `span_row.rs` appropriate. |
| `SpanOp` :140 | schema composition@2, local_username@145, handle@191, drain_pending@197 | `row`, `row_mut` | KEEP row/row_mut; add into_row/is_merge only if actual repeated destructuring callers justify it (not mechanically). `span_op.rs` appropriate. |
| `SessionCtx` :161 | handle@191, drain_pending@197, checkpoint@203, finalize@211 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |

## bt-daemon/src/translate/opencode.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `Saved` :50 | save_state@58, restore_state@65 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolInput` :121 | finalize@149 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolBeforeOutput` :131 | finalize@151 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolAfterResult` :137 | finalize@173 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolBeforePayload` :147 | finalize@193, from@194, handle@606 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolBefore` :161 | finalize@193, handle@607, tool_before@1132 | none | KEEP From<ToolBeforePayload>, ownership already exists; co-locate ToolBeforePayload/Input/Output in tool_event.rs. |
| `ToolAfterPayload` :169 | from@202, handle@610 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolAfter` :187 | from@202, handle@611, tool_after@1172 | none | KEEP From<ToolAfterPayload>, ownership already exists; co-locate ToolAfterPayload/Input/Result in tool_event.rs. |
| `PermissionToolRecord` :224 | from@242, record@298 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `PermissionTool` :240 | from@254, permission_record@1268 | `record`, `name` | KEEP record/name; companions remain data-only under PermissionProperties::request. Split permission.rs family. |
| `PermissionFields` :246 | from@273 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `PermissionPayload` :272 | from@280, permission_record@1261 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `PermissionProperties` :278 | permission_properties@313, permission_record@1259, permission_asked@1307, permission_replied@1315 | none | ADD `request()` by moving OpenCodeTranslator::permission_record static method; used by asked/replied and contains no translator state. Own source/info/direct/native aliases precedence in `opencode/permission.rs`. |
| `PartEvent` :320 | handle@600, part_updated@906 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `PartTime` :344 | permission_properties@328 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolPartState` :350 | permission_properties@338 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `ToolAttachment` :364 | permission_properties@358, tool_result_content@1477 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `MessageInfo` :374 | handle@603, message_updated@1019 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `MessageTime` :398 | permission_properties@386 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |
| `PermissionRequest` :574 | begin_compacted@587, permission_record@1259 | none | KEEP data-only native envelope/leaf; ToolBefore/After conversions and PermissionProperties own normalization. Group with event, permission, or message family. |

## bt-daemon/src/translate/opencode_v2/event.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `NativeEnvelope` :9 | field of enclosing DTO; consumed by parent decoder/reducer | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Location` :17 | schema composition@11 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `EventMetadata` :21 | schema composition@12 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `NativeEvent` :28 | schema composition@14 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Session` :80 | schema composition@68 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Created` :85 | schema composition@30 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Inbox` :93 | schema composition@34 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Enqueued` :100 | schema composition@32 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `InboxItem` :109 | schema composition@105 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Prompt` :116 | schema composition@111 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `PromptSkill` :125 | schema composition@122 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `PromptFile` :129 | schema composition@120 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Model` :134 | schema composition@147 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `StepStarted` :141 | schema composition@40 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `StepEnded` :150 | schema composition@42 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `TextEnded` :159 | schema composition@46 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolInputStarted` :168 | schema composition@50 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolCalled` :177 | schema composition@52 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolSuccess` :186 | schema composition@54 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolFailed` :195 | schema composition@56 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `NativeError` :204 | schema composition@156 | none | KEEP transport fields; if message formatting repeated, From/Display conversion is justified. Current reducer can use error.message directly without wrapper. |
| `ErrorResponse` :214 | schema composition@209 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolContent` :219 | schema composition@192 | none | ADD `into_span_part()` to own native image file to image_url conversion; reused live tool success and context tool-result normalization. Unknown native content must remain exact. |
| `KnownToolContent` :225 | schema composition@220 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `PermissionAsked` :237 | schema composition@58 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `PermissionSource` :245 | schema composition@242 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `PermissionReplied` :251 | schema composition@60 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `CompactionStarted` :259 | schema composition@62 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `CompactionEnded` :266 | schema composition@64 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `NativeCompaction` :277 | schema composition@272 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `NativeMessages` :282 | schema composition@278 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `NativeContextMessage` :288 | schema composition@283 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `CompactionFailed` :293 | schema composition@66 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ExecutionFailed` :300 | schema composition@70 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Interrupted` :306 | schema composition@72 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `Context` :313 | schema composition@38 | none | ADD `into_messages()` / From<Context> for Vec<span::Message> by moving normalize_context; owns system join and native content expansion. Keep session correlation in reducer. Split `context.rs` and private media children; related wrappers may stay grouped. |
| `ContextMessage` :322 | schema composition@319 | none | ADD `into_messages()` if needed by Context::into_messages; one native message may expand into tool result(s) plus assistant. Preserve order. Own normalized conversion, not actor state. |
| `MessageContent` :328 | schema composition@290 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ContextPart` :335 | schema composition@330, deserialize@353 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `KnownContextPart` :370 | schema composition@336 | none | ADD `image_url()`/media_parts only if it removes nested field fallback from Context conversion; keep no generic getters for every variant. |
| `Media` :398 | deserialize@374 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `MediaSource` :405 | deserialize@400 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |
| `ToolResult` :418 | deserialize@395 | none | KEEP data-only native event/leaf shape: one event-dispatch arm or parent DTO consumes fields; move with event family, no independent method justified. |

## bt-daemon/src/translate/opencode_v2/span.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `Content` :7 | new@92, tool@113, summary@119 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `ContentPart` :14 | schema composition@9, deserialize@46 | `text`, `image` | KEEP text/image constructors and exact Dynamic restoration; good ownership, split content.rs with TextPart/ImagePart/ImageUrl. |
| `TextPart` :31 | schema composition@15, text@48 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `ImagePart` :37 | schema composition@16, image@54 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `ImageUrl` :43 | deserialize@40, image@56 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `ToolCall` :61 | image@72, new@94 | `new` | KEEP new, owns JSON argument encoding and fixed kind. Split tool_call.rs with FunctionCall. |
| `FunctionCall` :68 | image@65, new@77 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `Reasoning` :85 | new@96 | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `Message` :90 | new@102 | `new`, `tool`, `summary` | KEEP new/tool/summary; good examples. Split message.rs with companion output content. |
| `RootMetadata` :125 | field of enclosing DTO; consumed by parent decoder/reducer | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `TurnMetadata` :137 | field of enclosing DTO; consumed by parent decoder/reducer | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `RootTotals` :143 | field of enclosing DTO; consumed by parent decoder/reducer | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `ToolMetadata` :148 | summary@167 | `new` | KEEP new; optional skill application method only if common interaction repeated. Group metadata siblings in metadata.rs. |
| `LlmMetadata` :183 | field of enclosing DTO; consumed by parent decoder/reducer | none | KEEP data-only serialization companion; constructor or conversion belongs principal Message/ContentPart/ToolCall/Metrics owner. Group with that type or metadata family. |
| `Metrics` :191 | new@199 | `from_tokens` | KEEP from_tokens; it owns SDK metric naming/arithmetic. Do not push sink representation into native token struct. |

## bt-daemon/src/translate/opencode_v2.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `CompactionResult` :103 | handle@682, compact@917 | none | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |

## bt-daemon/src/translate/pi.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `BeforeAgentStart` :59 | start_turn@646 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `PromptImage` :71 | create@65 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `SelectedModel` :79 | decode@313, handle@372, ensure_root@570, capture_context@701 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `ContextEvent` :85 | capture_context@700 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `MessageUpdate` :93 | streaming_update@730 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `MessageDelta` :101 | create@95 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `ThinkingLevel` :107 | handle@392 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `AssistantMessage` :115 | message_end@746, response_model@1111, normalize_assistant@1335 | none | ADD `response_model()` and `normalized()` by moving existing free functions; owns seven-field model precedence and content traversal. Keep model selection policy in reducer. Move with AssistantContent to `translate/pi/assistant_message.rs`. |
| `AssistantContent` :157 | create@139, normalize_assistant@1341 | none | Keep variant payload data-only; owned/traversed by AssistantMessage::normalized. Co-locate with owner. |
| `ToolExecutionStart` :184 | tool_start@858 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `ToolExecutionEnd` :199 | tool_end@896 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `AgentEnd` :215 | handle@407 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `SessionCompact` :223 | handle@420, compaction_message@1065 | none | ADD `summary_message()` by moving compaction_message. Owns optional-entry/summary projection, called from Pi compact handler. Co-locate in `pi/compaction.rs`. |
| `CompactionEntry` :231 | create@225 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `Preparation` :243 | create@283 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `BeforeCompact` :275 | compaction_input@1076 | none | ADD `input()` by moving compaction_input; owns counts and preparation projection. Keep reducer correlation in PiTranslator. Move to `pi/compaction.rs`. |
| `BeforeTree` :291 | handle@426, branch_summary_input@1095 | none | ADD `input()` by moving branch_summary_input; owns preparation projection. Move to `pi/compaction.rs`. |
| `SessionTree` :299 | handle@449 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `LegacyContinuation` :1028 | legacy_continuation@1036 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `ResumePayload` :1038 | legacy_continuation@1049 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |
| `Resume` :1042 | legacy_continuation@1039 | none | KEEP data-only partial native event or private resume wire companion: decoded in one Pi event handler; move event leaves to pi/event.rs, not getters for each field. |

## bt-daemon/src/translate/shared.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `CowSpanOp` :8 | from_arc@23, from@28, as_ref@33 | `new`, `make_mut`, `downgrade`, `from_arc` | KEEP new/make_mut/downgrade/from_arc/From/AsRef/Deref; already well-owned dedicated module. |

## bt-daemon/src/translate/state.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `SinkFields` :8 | schema composition@15, restore@36, from@42 | `of`, `restore` | KEEP of/restore: explicitly owns sink-only state serialization. Row remains private adapter, do not export. |
| `Row` :33 | schema composition@6, restore@38, from@46, serialize@55 | `restore` | KEEP From/restore private serialization adapter; already colocated with owning sink fields. |

## bt-daemon/src/translate/tool.rs

| Type | Callers / use | Existing methods | Decision |
|---|---|---|---|
| `ToolApproval` :15 | schema composition@20, add_tool_approval@30, tool_approval_metadata@36, with_tool_approval@182 | `as_str` | KEEP data-only; one composition/dispatch consumer; retain related-family locality and avoid invented methods. |
| `SkillLoad` :66 | insert_skill_metadata@75, skill_file_load@120, skill_md_paths@133 | none | ADD `insert_metadata(metadata, requested)` and `display_name()` to own name/path fallback and shared skill metadata. Existing free insert_skill_metadata is called from Claude, Codex, Cursor, Pi, OpenCode; regex discovery stays separate. Move type/behavior to `translate/tool/skill_load.rs`. |

## Runtime-state and implementation types explicitly excluded from DTO changes

These own active lifecycle, I/O, caches, visitor state, or durable actor continuation. Serialization alone does not make them DTOs. Keep methods with state machines; splitting may improve large files, but do not convert them into passive schema models.
| File | Declarations | Reason |
|---|---|---|
| `transcript_import/antigravity.rs` | `Tail` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/claude.rs` | `Tail` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/codex.rs` | `Tail` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/cursor.rs` | `Tail` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/mod.rs` | `IncrementalRecords`, `TranscriptTail` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/muse.rs` | `RunState`, `ModelState`, `PendingTool`, `Reader`, `EventsSeed`, `EventsVisitor`, `RecordsSeed`, `RecordsVisitor` | streaming reader / visitor / incremental filesystem state |
| `transcript_import/pipeline.rs` | `ImportProcessor` | streaming reader / visitor / incremental filesystem state |
| `translate/antigravity.rs` | `Turn`, `Invocation`, `PendingTool`, `AntigravityTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/claude.rs` | `Turn`, `PendingPrompt`, `TranscriptCursor`, `Subagent`, `PendingTool`, `PendingHistory`, `MessageHistory`, `HistoryMessage`, `PendingEmission`, `ClaudeTranslator`, `LlmCall`, `TranscriptTool` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/codex.rs` | `ScopeKind`, `OpenTurn`, `TurnInputSource`, `OpenLlm`, `OpenTool`, `Scope`, `DeferredHook`, `PendingWork`, `CodexTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/cursor/mod.rs` | `History`, `Turn`, `ModelStep`, `Tool`, `TranscriptCursor`, `CursorTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/debug.rs` | `DebugTranslatorFactory`, `DebugTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/git.rs` | `RepoEntry`, `NegativeEntry`, `CacheState`, `GitMetadataCache` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/grok.rs` | `BoundedOutput`, `OpenTurn`, `OpenLlm`, `OpenTool`, `CompletedTool`, `TranscriptCursor`, `PendingWork`, `GrokTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/mod.rs` | `Registry` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/muse.rs` | `OpenOp`, `Completed`, `MuseTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/muse_events.rs` | `Events` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/muse_usage.rs` | `UsageTracker` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/opencode.rs` | `OpenCodeVersionedTranslator`, `NativeSession`, `ClosedToolSpan`, `MessageHistory`, `HistoryEntry`, `CompactionBoundary`, `OpenCodeTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/opencode_v2.rs` | `SessionState`, `ToolState`, `Step`, `Permission`, `HistoryEntry`, `NativeWindow`, `Compaction`, `OpenCodeV2Translator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/origins.rs` | `WeakOrigins` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/pi.rs` | `PendingLlm`, `ToolStart`, `PiTranslator` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/recent.rs` | `RecentSet`, `RecentMap` | translator/session/span continuation, correlation state, bounded collection or process cache |
| `translate/turn_lineage.rs` | `TurnLineage` | translator/session/span continuation, correlation state, bounded collection or process cache |

Inventory totals: 188 DTO / transfer/result declarations reviewed individually, 88 runtime/implementation declarations reviewed and excluded. Imported/re-exported schema aliases and test-only fixtures are not double-counted. Associated `type Value`/`Target` and Result aliases are trait plumbing, not DTO definitions.
