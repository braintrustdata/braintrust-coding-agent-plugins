import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("./runtime/daemon-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./runtime/daemon-client")>()),
  DaemonClient: class {
    async close() {}
  },
}));

import plugin from "./server";

let directory: string;
beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "opencode-pause-"));
  vi.stubEnv("XDG_CONFIG_HOME", join(directory, "config"));
  vi.stubEnv("BRAINTRUST_OPENCODE_ENABLE_TOOLS", "false");
  vi.stubEnv("BT_TRACE_INVOCATION_SETTINGS", undefined);
  vi.stubEnv("BT_TRACE_MANAGED_RUN_ID", undefined);
});
afterEach(() => {
  vi.unstubAllEnvs();
  rmSync(directory, { recursive: true, force: true });
});

it("applies the global pause to OpenCode 2 and permits a managed run", async () => {
  const global = join(directory, "config", "opencode");
  const project = join(directory, ".opencode");
  mkdirSync(global, { recursive: true });
  mkdirSync(project, { recursive: true });
  writeFileSync(
    join(global, "braintrust.json"),
    JSON.stringify({ trace_to_braintrust: false, tracing_disabled: true }),
  );
  writeFileSync(join(project, "braintrust.json"), JSON.stringify({ trace_to_braintrust: true }));
  const hook = vi.fn(async () => {});
  const subscribe = vi.fn(async function* () {});
  const ctx = {
    app: { version: "2.2.5" },
    location: { directory },
    session: { hook, get: async () => ({}) },
    event: { subscribe },
    tool: { transform: async () => {} },
  };
  expect(await plugin.setup(ctx)).toBeUndefined();
  expect(hook).not.toHaveBeenCalled();
  expect(subscribe).not.toHaveBeenCalled();
  vi.stubEnv(
    "BT_TRACE_INVOCATION_SETTINGS",
    JSON.stringify({
      trace_to_braintrust: true,
      route: { destination: { type: "project_logs", project_name: "managed" } },
    }),
  );
  const cleanup = await plugin.setup(ctx);
  expect(hook).toHaveBeenCalled();
  expect(subscribe).toHaveBeenCalled();
  await cleanup?.();
});
