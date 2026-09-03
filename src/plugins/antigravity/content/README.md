# Braintrust tracing for Google Antigravity

Capture your Google Antigravity sessions as Braintrust traces, including
prompts, model responses, and tool activity.

## Set up tracing

Use the Braintrust CLI to install the plugin and choose where traces are sent:

```bash
bt trace enable antigravity
```

You can disable it later with:

```bash
bt trace disable antigravity
```

The plugin requires the `bt` CLI and a Unix-compatible `sh`. You can also
install it directly with:

```bash
agy plugin install https://github.com/braintrustdata/braintrust-antigravity-plugin
```

However, `bt trace enable antigravity` is recommended because it also saves
your Braintrust destination.

## Import an existing conversation

Replay a previous Antigravity conversation by its conversation ID:

```bash
bt trace import antigravity <conversation-id>
```

To continue reporting a conversation while it is active, add `--attach`:

```bash
bt trace import antigravity <conversation-id> --attach
```

Managed `bt trace run antigravity` support and Windows support are not yet
available.
