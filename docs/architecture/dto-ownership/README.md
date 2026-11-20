# DTO ownership review

This review follows the concrete-schema work in PR #306. It inventories the production types at commit `1b4a4e611f1a9bc73c796361c9d82cdbf52bb721`, traces their consumers, and distinguishes shared interpretation from data transport and runtime orchestration. The detailed tables describe that baseline, including types subsequently moved by this refactor.

- [Agent configuration, Muse, setup, and managed runs](agent-config.md): 178 DTO and codec declarations, including nested and macro-generated types, with runtime and test exclusions.
- [Translators, transcript imports, and mirrors](translators.md): 188 DTO/result declarations and 88 explicitly excluded runtime declarations.
- [Core runtime and JavaScript/TypeScript](core-runtime.md): 235 Rust declarations, including explicit runtime exclusions, and 24 TypeScript declarations.

These are review counts, not a count of independent public contracts: nested codecs and compatibility variants are included, while imported SDK types and reexports are not counted as new definitions. Each table records consumers, existing behavior, and an individual recommendation. Broad names such as `Message` or `Hook` do not imply that different native contracts should share a type.

## Implemented decisions

| Owner | Responsibility moved onto the type | Why it belongs there |
|---|---|---|
| `CursorHooks` | Registration lookup, known-command retention, and registration replacement | Setup and managed-hook cleanup no longer traverse optional maps and unknown native variants themselves. Marker recognition and process liveness stay with setup. |
| `OpenCodeConfig`, `OpenCodePlugin` | Plugin lookup, preservation-aware replacement/removal, exact-spec insertion, package matching | Setup and managed runs share collection mechanics while keeping their different version/ownership policies. Configured tuples retain their provider options. |
| `TranscriptPayload`, `Reference` | Captured-prefix validation, matching snapshots/references, native path enumeration, and Codex path precedence | Capture, polling, translation, and import no longer repeat native format selection. A modern captured absence still prevents reading newer native bytes; legacy fallback behavior remains explicit. |
| Antigravity `TranscriptRecord` | Native kind normalization, content precedence, and timestamp decoding | Import and translation share the native record's interpretation. Explicit null content keeps precedence over alternate fields. |
| `OpenCodeTokens` | Cache-inclusive prompt and total accounting | Both OpenCode protocol versions use the native counter semantics without repeating the arithmetic. Output metrics keep their existing names and projection types. |
| `SkillLoad` | Display name and common span metadata | Translators share skill naming, identity metadata, and explicit-request classification. Native discovery remains in the integration helpers. |
| Pi `AssistantMessage` | Physical response-model selection and assistant span-message projection | The reducer no longer inspects native content variants or repeats model precedence. |
| Pi compaction/branch DTOs | Input, summary message, and summary-request interpretation | Schema-specific field combinations leave the reducer; turn and continuation lifecycle stays there. |
| OpenCode `PermissionProperties` | Native permission request and reply normalization | Asked/replied handling shares precedence across native permission variants. |
| OpenCode 2 `Context`, `ToolContent` | Native context and tool-output conversion | The native owner handles media/content variants and emits the existing span representation. |
| `RedactedEnvelope` | Conversion back to the replayable runtime envelope | Recovery and translation share reconstruction without reintroducing credentials. The former journal helper was internal and is removed; public wire imports remain unchanged. |
| `SessionConfig` | Destination project-name query | Delegates to the destination's existing semantics. |
| `SessionRoute` | Additional metadata and tag edits | Route mutation owns trimming, validation and stable deduplication; filesystem and credential resolution remain outside the DTO. |
| `AuthDiagnostic` | Ready/unconfirmed construction and status predicates | Runtime and output rendering share diagnostic vocabulary and state combinations. |

## Organization

Shared hook/config schemas, provider usage, wire envelope types, transcript references, and Antigravity records now have directories with one primary owner per file and stable module reexports. Pi assistant messages, compaction records, and lifecycle payloads are separated; OpenCode permission, context, and tool-content families have focused modules. Primary types live beside their interpretation methods. Small private leaf records and discriminants stay with their owner when they have no independent consumers.

This is deliberately not a rule to put every struct in its own file. A permission payload's private helper variants, or a group of single-use lifecycle records, are easier to understand together. The facade modules provide stable imports, rather than parallel schema definitions.

## Deliberately retained as data

Most RPC request/results, CLI argument projections, native lifecycle wrappers, export-v1 leaf records, and tool/image descriptors need no additional methods. Their jobs are validation and transport; their consumers do not share further interpretation. Existing behavior on typed IDs, token metrics, message content, outcomes, and configuration overlays is retained rather than wrapped again.

JavaScript/TypeScript interfaces remain structural data. Turning deserialized JSON into class instances would add lifecycle requirements without useful ownership. Existing adapter/client functions already own encoding and dispatch. A typed shared JavaScript destination union is a separate contract change; this refactor does not combine it with Rust method extraction.

Some baseline recommendations remain conditional rather than implemented requirements: a constructor at a single synthesis site, a helper replacing one field access, or a private group method that only renames a loop does not justify another API. Codex installed-file trust and invocation-local trust have different validation policies and stay separate. Likewise, sink destination resolution supplies fallbacks that correlation intentionally omits; those paths are not unified.

Runtime actors, caches, locks, subprocess services, filesystem discovery, credential resolution, and event-ordering state remain responsible for their own operations. DTO methods do not acquire those responsibilities simply because an operation consumes a DTO.

## Compatibility checks

The refactor preserves serialized field names, public wire reexports, replay reconstruction, unknown extensions, missing-versus-null values, source-specific metric conventions, and existing hook ownership rules. Focused tests exercise configuration preservation, captured absence and path precedence, skill metadata, and credential-free replay conversion. The full locked all-feature Rust suite and strict all-target Clippy cover the integrated result.
