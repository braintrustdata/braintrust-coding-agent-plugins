# Publishing

This package is published to npm from GitHub Actions using **npm trusted publishing** with **GitHub OIDC** and **provenance attestations**.

Workflow file:

- `.github/workflows/publish-package.yaml`

## Why this setup

This avoids storing a long-lived `NPM_TOKEN` in GitHub secrets.

Instead:

- GitHub Actions requests a short-lived OIDC identity token
- npm verifies that token against the configured trusted publisher
- `npm publish --provenance` attaches a provenance attestation for the published package

The workflow has these permissions:

- `contents: write` - to create and push the git tag
- `id-token: write` - required for npm trusted publishing via OIDC

## One-time npm setup

In npm, configure **trusted publishing** for this package:

- **Package:** `@braintrust/trace-opencode`
- **Repository:** `braintrustdata/braintrust-opencode-plugin`
- **Workflow:** `publish-package.yaml`

This workflow no longer requires a GitHub Actions environment.

## Release flow

Releases are started manually from GitHub Actions using the **Publish package** workflow.

### 1. Bump the version

Update the version in `package.json` to the version you want to publish.

### 2. Run the workflow

From the GitHub Actions UI:

- choose **Publish package**
- choose the branch to publish from (default: `main`)
- run the workflow

## What the workflow does

The workflow performs these steps:

1. Checks out the selected branch
2. Sets up Node.js and Bun
3. Reads `name` and `version` from `package.json`
4. Verifies that:
   - git tag `v<version>` does not already exist on `origin`
   - `@braintrust/trace-opencode@<version>` is not already published on npm
5. Installs dependencies with `bun install --frozen-lockfile`
6. Validates the package by running:
   - `bun run check`
   - `bun run typecheck`
   - `bun run test`
   - `bun run build`
   - `npm pack --dry-run`
7. Publishes to npm with:

   ```bash
   npm publish --provenance --access public
   ```

8. Creates and pushes the matching git tag:

   ```text
   v<version>
   ```

9. Creates a GitHub release for that tag

## Notes

- The package is public, so `package.json` includes:

  ```json
  {
    "publishConfig": {
      "access": "public"
    }
  }
  ```

- The publish step relies on npm trusted publishing. No `NPM_TOKEN` secret should be needed.
- If the publish succeeds, npm should show provenance information for the release.

## Failure modes

The workflow will fail early if:

- the git tag for that version already exists
- that exact version is already published on npm
- lint, typecheck, tests, build, or `npm pack --dry-run` fails
- npm trusted publishing is not configured correctly for the package/repo/workflow

## Troubleshooting

If npm rejects the publish, verify:

1. trusted publishing is enabled for `@braintrust/trace-opencode`
2. the repository matches exactly:
   - `braintrustdata/braintrust-opencode-plugin`
3. the workflow filename matches exactly:
   - `publish-package.yaml`
4. the GitHub Actions job has `id-token: write`

If needed, update the trusted publishing settings in npm and rerun the workflow.
