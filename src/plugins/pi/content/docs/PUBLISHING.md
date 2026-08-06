# Publishing

This package is built and validated from the Braintrust coding-agent plugin
monorepo. The source manifest owns the committed package version, and
`src/plugins/pi/build.sh` creates the npm tarball from a clean staging tree.

Local release preflight from the monorepo root:

```bash
make validate-pi
DRY_RUN=1 NPM_TAG=next src/plugins/pi/publish.sh dist/pi/braintrust-pi-extension-*.tgz
```

The publisher accepts only a previously built tarball and rejects an existing
npm version. A real publish requires the release-only confirmation documented
by `publish.sh`. Normal development and CI use only the dry-run path; they do
not publish, tag, or create a GitHub release.
