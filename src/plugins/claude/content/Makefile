.PHONY: test test-trace-claude-code

test: test-trace-claude-code

test-trace-claude-code:
	@sh -n plugins/trace-claude-code/bin/claude-hook.sh
	@jq empty plugins/trace-claude-code/hooks/hooks.json
	@grep -q "'daemon','hook','--source','claude-code'" plugins/trace-claude-code/bin/claude-hook.cmd
	@echo "trace-claude-code shims OK"
