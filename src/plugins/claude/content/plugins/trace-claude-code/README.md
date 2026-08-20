# Braintrust Claude Code tracing plugin

This plugin synchronously forwards Claude Code lifecycle payloads to:

```text
bt trace hook --source claude-code
```

Use `bt trace setup claude --project <project>` to install and configure it.
The hook first installs the `bt` CLI with the official installer when it is not
already available, then forwards the event. The plugin is credential-free and
fail-open; the `bt` CLI and shared daemon own authentication, event journaling,
trace construction, and delivery.

To add fields to each root trace span, pass a JSON object with
`bt trace setup claude --additional-metadata` for a persistent configuration,
or `bt trace run --additional-metadata '<JSON>' claude` for one invocation.
