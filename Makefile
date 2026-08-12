# Braintrust coding-agent plugins monorepo.
#
# Each plugin under src/plugins/<agent>/ provides:
#   build.sh <target_dir>   assemble the agent's distribution tree
#   validate.sh <target>    sanity-check a built tree
#   publish.sh              deploy it (push to dist repo, npm/vsce publish, ...)
#
# Targets fan out over every plugin dir:
#   make build            build all plugins into dist/<agent>
#   make build-codex      build just one
#   make test             build then validate all
#   make publish          publish all (DRY_RUN=1 to test without pushing)
#   make publish-codex    publish just one

SHELL := /bin/bash

PLUGINS := $(notdir $(wildcard src/plugins/*))
DIST := dist

# Internal target lists. NOTE: deliberately NOT named PUBLISH_TARGETS — that is
# the user-facing env var (the plugin->repo map) and a makefile var of the same
# name would shadow it in recipe subshells.
BUILD_RULES := $(addprefix build-,$(PLUGINS))
PUBLISH_RULES := $(addprefix publish-,$(PLUGINS))
VALIDATE_RULES := $(addprefix validate-,$(PLUGINS))

.PHONY: build test publish clean $(BUILD_RULES) $(VALIDATE_RULES) $(PUBLISH_RULES)

build: $(BUILD_RULES)

# Static pattern rule (reliable across make versions for phony fan-out).
$(BUILD_RULES): build-%:
	@echo "==> build $*"
	@src/plugins/$*/build.sh "$(DIST)/$*"

test: build
	@for p in $(PLUGINS); do \
		echo "==> validate $$p"; \
		src/plugins/$$p/validate.sh "$(DIST)/$$p"; \
	done
	@bash scripts/test-hook-forwarders.sh "$(DIST)"

$(VALIDATE_RULES): validate-%: build-%
	@echo "==> validate $*"
	@src/plugins/$*/validate.sh "$(DIST)/$*"

# Deploy every plugin named in the PUBLISH_TARGETS env var map. Fails if unset.
publish:
	@scripts/publish.sh

# Publish a single plugin, e.g. DIST_REPO=owner/name make publish-codex
$(PUBLISH_RULES): publish-%:
	@echo "==> publish $*"
	@src/plugins/$*/publish.sh

clean:
	@rm -rf "$(DIST)"
