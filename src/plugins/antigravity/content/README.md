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

This package requires a `bt` CLI that exposes `bt trace hook` and a
Unix-compatible `sh`. Install or refresh the published plugin and configure its
Braintrust route with `bt trace enable antigravity` (`setup` remains an alias);
remove its managed registration with `bt trace disable antigravity`.

The published plugin can also be inspected or installed directly with:

```bash
agy plugin install https://github.com/braintrustdata/braintrust-antigravity-plugin
```

The `bt trace enable` command remains the recommended entrypoint because it also
persists the Braintrust destination. Existing conversations can be replayed
with `bt trace import antigravity <conversation-id>` or followed live with
`--attach`; both use Antigravity's durable `transcript_full.jsonl` and the same
daemon translator as hooks. Managed-run injection and Windows support are not
currently provided.
