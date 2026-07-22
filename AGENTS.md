# Repo model (for humans and agents)

- `src/plugins/<agent>/`   per-agent plugin: `content/` (deployable source) plus
                           `build.sh`, `validate.sh`, `publish.sh`
- `src/skills/*`           canonical skills (Agent Skills spec)
- `scripts/publish.sh`     reads the PUBLISH_TARGETS map and deploys each plugin
- `bt-daemon/`             shared Rust project (self-contained Cargo workspace;
                           see its README). Placeholder for now.

Build one agent locally:  `src/plugins/claude/build.sh /tmp/dist-claude`
Validate it:              `src/plugins/claude/validate.sh /tmp/dist-claude`
Build/validate all:       `make test`
Deploy (plugin:repo map): `PUBLISH_TARGETS="codex:<repo>,claude:<repo>" make publish`
