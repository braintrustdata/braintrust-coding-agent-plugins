.PHONY: test

# Runtime validation for the thin shared-daemon shim.
test:
	@sh -n bin/codex-hook.sh
	@grep -q "'daemon','hook','--source','codex'" bin/codex-hook.cmd
	@grep -Fq 'call "%BT_HOOK_BIN%" daemon hook --help' bin/codex-hook.cmd
	@jq empty hooks/hooks.json .codex-plugin/plugin.json
	@echo "trace-codex shim OK"
