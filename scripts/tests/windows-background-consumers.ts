// Bundle the production implementations, not option mocks or test-only runners.
export { DaemonClient } from "../../src/runtime/js-daemon-client/src/index"
export { spawnBackground } from "../../src/runtime/js-daemon-client/src/background-process"
export { BtCliToolsClient } from "../../src/plugins/opencode/content/src/tools/bt-cli"
