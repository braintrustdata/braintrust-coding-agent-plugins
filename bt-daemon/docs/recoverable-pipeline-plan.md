# Recoverable pipeline implementation plan

Status: proposal. Supersedes the recoverable-stream pipeline design and implementation
plan and the event WAL recovery proposal. Start from current `main`, not PRs #130,
#132, or #171. This plan authorizes no implementation by itself.

## 1. Goal and scope

Build a concurrent pipeline of self-contained actors with typed input/output ports:

```text
capture -> event journal -> translation -> SpanOp journal
                                             |     |
                                          head A head B
                                             |     |
                                          route A route B
                                             |     |
                                          sink A  sink B
```

The requirements are failure reasons with retry criteria, persisted SpanOps, and
isolated stages composed through channels. Each route owns an independent resumable
head in the shared SpanOp journal. Downstream recovery starts at its earliest
unresolved SpanOp; it never replays translation.

Preserve trace shape, routing identity, attachment, imports, and existing ledger
semantics. Per-plugin durable checkpoints, translator snapshots, historical backfill,
prefix collection, and a new scheduler or workflow platform are outside scope.

## 2. Composition contract

Actors own mutable processing state. Stages accept typed inputs and return typed
outcome enums; assembly maps each variant to a compatible downstream input port.
Stages do not name successors, access another actor's state, or receive `Daemon`.
A small shared stage trait with associated `Input` and `Outcome` types and typed
input/output port traits is sufficient. Wiring must cover every outcome variant
and check connected types. Ordinary enum matches and adapters are sufficient.

For example, a plugin returns `Prepared`, `Filtered`, or `Failed`. Assembly routes
those outcomes to submission, terminal completion, or recovery. The actor runner
preserves the message envelope across that mapping. Adding a validation stage
changes wiring rather than the plugin implementation.

Both bounded Tokio channels and journal-backed channels expose send/receive ports
carrying the same envelope. Normal stages need not know which transport they use.
Storage adapters own durability, subscriptions, and completion; a successful volatile
send means queued, not consumed. A failed send retains or returns the message.

### Message ownership and completion

```rust
// Illustrative API; fields and completion plumbing are private.
struct Delivery<T> {
    value: T,
    completion: Completion,
}

impl<T> Delivery<T> {
    fn value(&self) -> &T;
    fn map<U>(self, f: impl FnOnce(T) -> U) -> Delivery<U>;
    async fn into_completed(self) -> Result<T, Unfinished<T>>;
}
```

`value()` borrows for processing. `map()` changes the value type while moving its
completion obligation. `into_completed()` claims the value has been handled,
confirms upstream completion, and returns ownership of the final value. The sink
calls it after checked acceptance; an intentional filter can also complete work.
If completion fails, `Unfinished<T>` retains the value and context for retry.

Volatile handoffs preserve the obligation. A no-op completion is valid only when
there is no upstream obligation. Dropping a message, cancelling a task, or releasing
an `Arc` does not complete it. An immutable payload may be shared with `Arc`, but
each route's completion capability is unique and non-cloneable.

A bounded owner mailbox and one-shot reply can implement completion. The reply
confirms the durable head write, not merely receipt of the request. Completion
requests are idempotent and cannot skip unresolved positions; late or duplicate
requests cannot advance a different subscription or overwrite newer progress.
Only one live consumer owns a given head. Restart reconstructs envelopes from
persisted positions, not from channels or reference counts.

For one input producing several outputs, keep a bounded batch under one obligation;
never clone the token onto each output. Terminal completion covers the whole batch.
Translation may produce no output: its empty commit still consumes the input.
General-purpose splitting/joining of obligations is unnecessary for this plan.

### Concurrency and ordering

The graph is shared; current stage and recovery state belong to pending executions,
not the pipeline globally. Use one ordered translation consumer per source/session
and one ordered routing consumer per route head. Different consumers run concurrently;
initially each processes one item or bounded batch at a time. Stateful translators
and route state belong to their consumers, not a global registered handler instance.

Use ordinary Tokio tasks and bounded queues; retain worker-local plugin runtimes.
Small synchronous stages may share an actor. A recovering consumer stops later data
on its head while still servicing configuration, status, and shutdown controls.
Journal-backed edges let upstream work continue persisting while consumers wait.
Notifications are wake hints; subscribe/recheck against storage to avoid lost wakes.

## 3. Durable channels

Keep the event journal. Use a concrete SQLite adapter for SpanOps, heads, and recovery
records, with blocking database work outside Tokio workers. Keep SQL and codecs
inside storage. Capture acknowledges only its durable event append, independently
of downstream progress. Storage read/corruption errors hold work and report failure;
they never silently skip retained records.

### Handoff and restart

The SpanOp adapter atomically commits ordered output and its consumed event position.
That position is the authoritative translation boundary. Stable commit identities
make retry idempotent, including empty output. Support bounded output chunks without
exposing partial commits to readers or collecting a whole transcript into memory.

At this durable handoff, the adapter resolves the preceding event obligation and
creates new obligations for downstream subscriptions. If output committed but
upstream confirmation was interrupted, reconcile the committed input position;
do not regenerate or append that output. Keep candidate output while retrying a
failed local commit before reading more input.

Existing translators rebuild by replaying the committed event prefix with output
suppressed, then process the uncommitted suffix. Retain that prefix and required
transcript references. This rebuild is for translation state, never routing retry.
Output-producing checkpoint/finalize controls need stable ordered event identities
and captured transcript boundaries so restart reproduces the same input.

Persist every field needed by routing, including creation provenance and operation
identity. Current `SpanRow` serialization skips fields such as `late_merge_key`;
use a storage codec that preserves them. Persist no credentials or auth leases.
Committed SpanOps are route-neutral; route preparation applies attachment, tags,
metadata, and plugin configuration to a private value. Version the storage codec
and hold unsupported records with a diagnostic rather than defaulting or skipping fields.

### Subscription split and completion

The producer sends one output batch without knowing route counts or heads. Assembly
registers subscriptions and pairs their receivers with routing actors. The durable
adapter supplies independent deliveries to each route; it need not eagerly push
copies or add a separate fan-out stage.

Persist the nonsecret subscription identity/settings needed to resume without a
new event. Preserve existing route identity rules: credential refresh keeps its
head, and configuration changes must not redirect an old head to another destination.

Each head advances monotonically over a contiguous consumed prefix. Reading and
forwarding never advance it. Sink enqueue is insufficient: require checked flush/
acceptance for every covered operation, including when SDK clients are shared.
Preserve terminal-span and late-merge ledger behavior without recording acceptance
prematurely.

Deferred roots and batching may read ahead while retaining the earliest unfinished
position. They must allow reading the child that releases a deferred root; later
success cannot skip the unresolved prefix. Keep pending memory bounded and preserve
existing deferral behavior.

After backend acceptance, retry a failed local head write without rerunning delivery
in process. If confirmation is lost, reload the durable head. A crash before durable
completion may require idempotent resubmission with existing stable span IDs; the
pipeline provides at-least-once delivery, not exactly-once external effects.

Retirement preserves subscriptions, heads, and pending work. Retain completed SpanOps
under the existing session retention policy so late routes can read history without
backfill. Delete a session only when retention permits, translation is caught up,
all heads are complete, and nothing is recovering. Protect event/transcript references
needed for reconstruction. Empty subscriber sets do not authorize accidental deletion.

## 4. Recovery as a pipeline state

A failure outcome carries a typed cause, local diagnostic, and retry condition.
Wiring maps it to `Recovering`, which retains the pending position, stage, boundary,
and condition evidence. It waits asynchronously, then returns an eligible outcome
mapped back into processing with updated conditions. No external supervisor restarts
stage clusters. Successful processing and completion, not eligibility alone, resolve
the failure.

| Cause | Retry criterion |
| --- | --- |
| Plugin | Failed file's bytes/existence or relevant configured chain changes |
| Translation | Applicable translator revision changes |
| Credentials | Credential notification or bounded fresh resolution |
| Transport/server | Capped retry delay |
| Rate limit | Retry-After or capped fallback delay |
| Permission/destination | Relevant configuration change or bounded recheck |
| Permanent rejection | Relevant processor/configuration change |
| Local persistence | Bounded retry of the failed storage operation |

Record exact plugin identity/digest and nonsecret configuration evidence. Unrelated
file edits do not qualify. Classify typed errors at their source rather than parsing
error strings. Restart re-evaluates persisted criteria; it is not itself permission
to retry unchanged plugins. Reload changed plugins or fresh credentials on retry.

Routing retries reload unresolved SpanOps and may repeat downstream transformations.
Translation failures use the event boundary. Persistence failures retry the local
commit/completion where possible. Persist recovery evidence and expose stage, pending
position, reason, and eligibility through doctor. A failure to persist diagnostics
must retain the original reason in memory and never release work.

## 5. Incremental PRs

Each PR has one reviewable responsibility, includes its checks, and leaves `main`
usable. Small preparatory extractions are acceptable; no line-count target applies.

1. **Contracts inside the current actor.** Extract stage handlers, typed outcomes,
   `Delivery<T>`, and queue port adapters without changing task ownership or replay.
   Establish checked sink acceptance and `into_completed()`. Test mapping, filtering,
   no completion on drop, and rejected delivery retaining its checkpoint.
2. **Actor/channel separation.** Split translation and routing ownership within each
   existing route, using bounded ports and the existing session handle as a facade.
   Carry event identity through completion. Test ordering, backpressure, cancellation,
   and controls during blocked sends. Preserve current journals/checkpoints.
3. **Route-neutral output.** Move attachment/tags/metadata into route preparation and
   carry provenance with SpanOps. Remove translator route-config reads and sink version
   setters. Test canonical output across routes and equivalent final trace behavior.
4. **Durable adapter.** Add SQLite commits, full SpanOp codec, heads, and subscription
   envelopes behind the port contract. Test empty/multiple output, chunked commit,
   interruption before/after commit, independent heads, and confirmed completion.
   Keep production wiring unchanged until these contracts pass.
5. **Activate the journal boundary.** Replace the volatile edge with the durable port;
   share translation per source/session and register a subscription per route. Include
   ordered controls and necessary lifecycle wiring. Migrate released checkpoints
   restartably; preserve data when a completion mapping cannot be proved. Test restart,
   late routes, zero translator calls during routing resume, and healthy sibling progress.
6. **Recovery transitions.** Persist reasons/criteria, wire waiting and retry outcomes,
   and restore pending work without new native events. Test exact plugin edits,
   credential refresh, transport retries, unchanged-condition holds, and pending retention.
   Plugin/auth/delivery retries must never enter translation.
7. **Lifecycle consolidation and cleanup.** Remove replaced per-route translation and
   active event delivery checkpoints; retain upgrade readers. Validate flush/finalize,
   managed runs, startup, retirement, shutdown, correlation, and import/live ledger
   compatibility. Existing import entry points remain unchanged.

Activation includes required lifecycle correctness; the last PR consolidates rather
than repairs an unshippable intermediate state. Flush targets ordered controls and
reports completion only when the covered route heads reach their output boundary.
Timeout or shutdown leaves unresolved work durable. Correlation progress follows
translation, not downstream delivery; adjust existing barriers narrowly to preserve
independence without redesigning correlation.

## 6. Validation

Test handlers separately from actor wiring. Fault-inject commit and head-write gaps,
including lost confirmation, completion cancellation, and actor failure between
volatile stages. Prove a blocked route does not block capture, translation, or its
sibling, and blocked translation leaves committed SpanOps deliverable.

Use the actual SDK path to establish rejection/acceptance semantics, alongside fake
sinks for pipeline tests. Run targeted checks for each PR and the locked all-feature
daemon suite, formatting, and CI lints before presenting it. Persistence changes
need completed Linux, macOS, and Windows CI. Report local and remote results separately.
