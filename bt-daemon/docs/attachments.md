# Span attachments

`SpanRow.attachments` accompanies both insert and merge operations. An attachment
contains immutable captured bytes, a SHA-256 digest, a filename and a content type.
The transport accepts arbitrary bytes, without an image-format allowlist.

Translation publication extracts base64 data URLs, Claude image/document source
blocks, Pi image blocks and OpenCode file blocks from input and output. Explicit
local file URLs are snapshotted; ordinary path strings and remote URLs are never
fetched. Cursor prompt file attachments are captured from their explicit hook
paths. OpenCode 2 prompt files are preserved regardless of MIME type.

Internal placeholders preserve attachment positions inside structured input and
output. The Braintrust sink uploads bytes before queueing a span and substitutes
Braintrust attachment references. References are also listed in
`metadata["braintrust.attachments"]` for attachments supplied directly alongside
an operation. Signed-upload requests receive only the headers returned by the
backend, never the Braintrust bearer token. Upload failures fail submission and
leave the route's delivery obligation pending.

## Storage and cleanup

The operation journal stores each distinct blob once in `attachment_blobs`.
`operation_attachments` links journal positions to blob digests. SQLite triggers
increment and decrement the `refs` column as links are inserted and deleted.
Both changes share the operation publication/acknowledgment transaction. The
unique operation/blob link and existing publication receipts prevent replay from
acquiring duplicate references.

The existing route heads determine when an operation has been delivered to all
subscribers. Deleting that operation releases its blob references immediately;
zero-reference blob rows are deleted in the same transaction. No collection
retention interval or separate garbage-collection timer is involved. SQLite
reuses freed pages; deletion does not shrink the database file on every
acknowledgment. Deduplication is scoped to one operation journal.

## Remaining work before SDK-834 is complete

This draft introduces the shared delivery mechanism; it does not establish full
native attachment coverage for every agent/version.

- Move local-file snapshotting to the native event capture boundary, so delayed
  translation cannot observe a changed or deleted native file.
- Ensure raw input journals and translator history/checkpoints do not retain
  additional byte copies beyond their actual recovery obligations.
- Capture and verify Grok and Antigravity native attachment schemas, including
  generated artifacts and media exposed through MCP tools.
- Extend Muse's native image-only asset recovery to other formats that its native
  session store actually exposes.
- Represent unavailable native assets explicitly, including opaque hosted file
  IDs, without inventing bytes or fetching arbitrary URLs.
- Verify filenames, content types, original-file versus model-transformed media,
  subagents, installed packages and import/attach paths against pinned real agent
  versions. Validate representative uploads against a real Braintrust backend.

The current implementation keeps attachment bytes in memory during extraction
and upload. Streaming capture/upload and large-file resource limits need review
before claiming unrestricted size support.
