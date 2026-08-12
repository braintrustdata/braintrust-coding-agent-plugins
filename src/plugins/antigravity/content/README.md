# Braintrust tracing for Google Antigravity

This Antigravity plugin forwards native lifecycle hooks to the Braintrust
daemon. The daemon combines exact model and tool boundaries from hooks with
the conversation's full JSONL transcript to construct a session, turn, model,
and tool span tree.

The hook adapter is synchronous, credential-free, and fail-open. Braintrust
authentication and destination routing remain owned by the `bt` CLI.

The initial implementation captures `PreInvocation`, `PostInvocation`,
`PostToolUse`, and `Stop`. It intentionally does not register `PreToolUse`:
Antigravity requires that hook to return a permission decision. Live testing
confirmed that an empty decision is handled as a denial, while `allow` would
bypass normal permission checks and `ask` could add prompts.

This package currently requires a `bt` CLI that exposes `bt trace hook` and a
Unix-compatible `sh`. Persistent setup, managed-run injection, transcript
import/attach, Windows support, and production distribution are not yet part of
this feasibility implementation.
