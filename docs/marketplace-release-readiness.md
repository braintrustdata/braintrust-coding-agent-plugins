# Claude and Codex marketplace release readiness

Claude Code and Codex remain marketplace-delivered from generated distribution repositories. The existing `src/plugins/<agent>/content`, `build.sh`, `validate.sh`, and `publish.sh` model is the correct delivery mechanism; neither plugin should become an npm package.

## Current behavior

- Production releases select `claude` or `codex`, update that plugin's committed manifest version, and deploy a freshly generated tree to its distribution repository.
- Test releases use the same reusable workflow and deploy to `braintrustdata/test-coding-agent-dist` without committing or tagging the monorepo.
- Codex has a post-deploy smoke workflow. Claude has no equivalent deployed-package smoke test.
- The checked-in Claude and Codex tracing packages still contain their legacy processing runtimes. They are not yet thin `bt trace hook --source ...` launchers.

## Release blockers

The marketplace packages build and validate locally, but the production workflow is not release-ready in its current ordering:

1. `_release.yml` creates and pushes a version commit, tag, and GitHub Release before the external distribution repository is deployed or installed successfully.
2. The workflow writes the version bump directly to `main` instead of releasing a committed version from an explicit full SHA already reachable from `main`.
3. The sandbox deployment is a separate manual workflow, not a required test of the exact production artifact.
4. The Codex smoke test invokes the obsolete `bt daemon hook` command; the current CLI surface is `bt trace`.
5. The Codex smoke test is skipped when `OPENAI_API_KEY` is absent, including during production preparation.
6. There is no deployed Claude smoke test.

## Follow-up acceptance order

A marketplace-focused follow-up should:

1. replace Claude and Codex's legacy tracing processors with thin fail-open `bt trace hook` launchers;
2. build and validate the selected plugin from an explicit full SHA on `main`;
3. deploy that exact artifact to the sandbox distribution repository and verify installation;
4. require smoke credentials for production preparation and run deployed smoke tests for both agents;
5. deploy the already-tested artifact to the production distribution repository;
6. verify production installation;
7. only then create the agent-qualified tag and GitHub Release.

Until that follow-up lands, `make build`, `make test`, and dry-run sandbox deployment are useful package checks, but a production marketplace release should not be initiated from the current workflow.
