# Contributing

`@braintrust/pi-extension` is developed in the Braintrust coding-agent plugin
monorepo under `src/plugins/pi/content`.

The JavaScript package is intentionally a thin adapter: it reads non-secret
routing and UI preferences, forwards native pi events through the shared daemon
client, and displays daemon status. It must not construct spans, persist tracing
state, resolve credentials, or call the Braintrust API.

Run the package checks from this directory with:

```bash
pnpm install --frozen-lockfile
pnpm run check
pnpm test
pnpm run build
pnpm run smoke
```

From the monorepo root, `make validate-pi` performs the complete package
validation. The Rust translator and real-agent harness live under
`bt-daemon/` and are exercised by the root CI workflow.

Configuration precedence remains defaults, the global pi extension config,
project-local pi extension config, then environment variables. Configuration
may select a `bt` profile, organization, project, metadata, and UI behavior, but
must never contain credentials.
