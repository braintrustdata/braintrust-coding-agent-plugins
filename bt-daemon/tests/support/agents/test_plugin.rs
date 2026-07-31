use crate::support::agent_process::AgentTestWorld;
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

const HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PermissionRequest",
    "PermissionDenied",
    "PostToolUse",
    "PostToolUseFailure",
    "PostToolBatch",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
    "StopFailure",
    "SessionEnd",
    "TaskCreated",
    "TaskCompleted",
];

pub fn codex_marketplace(world: &AgentTestWorld) -> PathBuf {
    let root = world.temp_path("codex-test-marketplace");
    write_json(
        &root.join(".agents/plugins/marketplace.json"),
        json!({
            "name": "braintrust-daemon-tests",
            "plugins": [{
                "name": "trace-codex-test",
                "source": {
                    "source": "local",
                    "path": "./plugins/trace-codex-test"
                }
            }]
        }),
    );

    let plugin = root.join("plugins/trace-codex-test");
    write_json(
        &plugin.join(".codex-plugin/plugin.json"),
        json!({
            "name": "trace-codex-test",
            "version": "0.0.0",
            "description": "Direct bt daemon hook fixture",
            "hooks": "./hooks/hooks.json"
        }),
    );
    write_json(
        &plugin.join("hooks/hooks.json"),
        hook_config("bt trace hook --source codex --source-version test"),
    );
    root
}

pub fn claude_plugin(world: &AgentTestWorld) -> PathBuf {
    let root = world.temp_path("claude-test-plugin");
    write_json(
        &root.join(".claude-plugin/plugin.json"),
        json!({
            "name": "trace-claude-test",
            "version": "0.0.0",
            "description": "Direct bt daemon hook fixture"
        }),
    );
    write_json(
        &root.join("hooks/hooks.json"),
        hook_config("bt trace hook --source claude-code --source-version test"),
    );
    root
}

fn hook_config(command: &str) -> Value {
    let hook = json!([{
        "hooks": [{
            "type": "command",
            "command": command,
            "async": false
        }]
    }]);
    let hooks = HOOK_EVENTS
        .iter()
        .map(|event| ((*event).to_string(), hook.clone()))
        .collect::<Map<_, _>>();
    json!({ "hooks": hooks })
}

fn write_json(path: &Path, value: Value) {
    std::fs::create_dir_all(path.parent().expect("test plugin file has parent"))
        .expect("create test plugin directory");
    std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap())
        .expect("write test plugin file");
}
