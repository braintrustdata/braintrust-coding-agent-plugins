# Publishing

`@braintrust/trace-opencode` is built and released from the plugin monorepo.
Version 1 requires a compatible `bt` installation with the OpenCode daemon
translator. If the daemon is missing or incompatible, tracing warns once and
fails open without interrupting OpenCode.

From the monorepo root, validate locally with:

```bash
make validate-opencode
DRY_RUN=1 src/plugins/opencode/publish.sh
```

Validation builds the ESM package, bundles the canonical daemon client, runs
the tests and static checks, verifies npm's package file list, and performs a
dry-run publish. No persistent package tarball is part of the build output.

Real releases run only through `.github/workflows/release-opencode.yml`. The
workflow uses `braintrustdata/sdk-actions`, environment approval, npm trusted
publishing, and provenance. Local scripts deliberately refuse real publishes.
