# OpenCode package guidance

- Tracing code is a fail-open event adapter only. It forwards raw events to the
  shared daemon client and contains no span construction or Braintrust delivery.
- `bt-daemon` owns translation, correlation, journaling, recovery, and delivery.
- Direct Braintrust API access is allowed only beneath `src/tools/` for the four
  explicit data-access tools.
- Preserve independent `trace_to_braintrust` and `enable_tools` controls.
- Run `make validate-opencode` from the monorepo root after package changes.
