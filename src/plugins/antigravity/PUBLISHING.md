# Publish the Braintrust Antigravity MCP plugin

The Antigravity distribution contains the existing root tracing plugin and
`plugins/braintrust/`, the independently installed hosted MCP plugin. All MCP
assets live in `content/plugins/braintrust/`. The root tracing identity and its
installation URL remain stable.

## Validate before release

```bash
make validate-antigravity test-antigravity-packaging antigravity-submission
agy plugin validate --strict ./dist/antigravity/plugins/braintrust
agy plugin install ./dist/antigravity/plugins/braintrust
agy plugin list --json
```

Confirm the installed `braintrust` plugin exposes one MCP server and no hooks.
Authenticate in Antigravity, then run all three starter prompts against an
account with representative projects and experiments. Verify missing data is
reported accurately. Uninstall the test copy with `agy plugin uninstall braintrust`.

## Distribution release

Use the existing `braintrustdata/braintrust-antigravity-plugin` distribution.
After merging the implementation, run **Release Plugin** on `main`, selecting
`antigravity` and the desired version. Review and merge its release PR. The
workflow stamps the MCP manifest, verifies its reviewed version and approval
record, deploys the distribution, and creates the source and distribution tags
and GitHub Releases. The release also includes an MCP-only ZIP with
`plugin.json` at its root for review or local installation after extraction.

The GitHub install URL selects the MCP subdirectory:

```bash
agy plugin install https://github.com/braintrustdata/braintrust-antigravity-plugin/tree/main/plugins/braintrust
```

Installing the repository root continues to install the tracing plugin.
Both packages are built and validated by `make validate-antigravity`.

To exercise deployment without production changes, use **Release plugin
(test)** with `antigravity`. It overwrites the shared test distribution.
For a local deployment preview against an existing repository:

```bash
DRY_RUN=1 PUBLISH_TARGETS="antigravity:braintrustdata/test-coding-agent-dist" make publish
```

## Marketplace submission

Follow Google's [Build plugins guide](https://antigravity.google/docs/build-plugins/#validating-and-publishing-plugins)
and submit the released MCP plugin through the
[Antigravity Marketplace partner intake form](https://forms.gle/2EX5RFYPoJe1UgxR9).
Use the bundled metadata, logo, README, and starter prompts. Provide a company
technical contact, public support address, plugin description, and documentation
URL as requested by the form. Reference the MCP subdirectory and release ZIP in
follow-up with Google. Distribution publication does not itself submit or
approve a Marketplace listing.

The package uses Braintrust's NA MCP endpoint and automatic OAuth discovery.
No pre-registered client ID or secret is bundled. The existing 200×200
Braintrust icon is copied verbatim from `src/plugins/cursor/content/icon.png`.
