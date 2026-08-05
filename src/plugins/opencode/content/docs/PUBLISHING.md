# Publishing

`@braintrust/trace-opencode` is built and released from the plugin monorepo.
The standalone repository workflow described by older versions of this file is
not used by this package.

From the monorepo root:

```bash
make validate-opencode
DRY_RUN=1 src/plugins/opencode/publish.sh dist/opencode
```

Validation builds the ESM package, bundles the shared daemon client, runs the
tests and static checks, verifies the tarball contents, and performs
`npm pack --dry-run`.

Publishing a real package is a separate release-only action. It requires an
already validated tarball and the explicit safeguards enforced by
`src/plugins/opencode/publish.sh`. Normal CI and integration work must use only
the dry-run path.
