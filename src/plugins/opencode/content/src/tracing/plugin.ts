import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin";
import { loadConfig, type PluginConfig } from "../config";
import {
  claimManagedTracingInstance,
  mergeConfigObjects,
  mergeProjectRoute,
  projectSettingsPath,
} from "../runtime/daemon-client";
import { createDaemonTracingHooks } from "./daemon";

export async function readPluginConfig(input: {
  directory: string;
}): Promise<PluginConfig | undefined> {
  let pluginConfig: PluginConfig | undefined;
  try {
    const fs = await import("node:fs");
    const path = await import("node:path");
    const os = await import("node:os");
    const configHome = process.env.XDG_CONFIG_HOME || path.join(os.homedir(), ".config");
    const projectPath = projectSettingsPath(input.directory);
    const configPaths = [path.join(configHome, "opencode", "braintrust.json"), projectPath];
    for (const [index, configPath] of configPaths.entries()) {
      try {
        if (fs.existsSync(configPath)) {
          const parsed = JSON.parse(fs.readFileSync(configPath, "utf-8")) as PluginConfig;
          if (index === 0) {
            pluginConfig = parsed;
          } else {
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
              throw new TypeError("project settings must be an object");
            const { route: projectRoute, inherit_plugins: inheritPlugins, ...safe } = parsed;
            if (
              projectRoute !== undefined &&
              (!projectRoute || typeof projectRoute !== "object" || Array.isArray(projectRoute))
            )
              throw new TypeError("project route must be an object");
            if (inheritPlugins !== undefined && typeof inheritPlugins !== "boolean")
              throw new TypeError("inherit_plugins must be a boolean");
            const userRoute = mergeProjectRoute(
              pluginConfig?.route,
              projectRoute,
              configPath,
              inheritPlugins !== false,
            );
            if (safe.tags) {
              safe.tags = [
                ...new Set([...(userRoute?.tags ?? pluginConfig?.tags ?? []), ...safe.tags]),
              ];
            }
            if (safe.additional_metadata) {
              safe.additional_metadata = mergeConfigObjects(
                userRoute?.additional_metadata ?? pluginConfig?.additional_metadata ?? {},
                safe.additional_metadata,
              );
            } else if (projectRoute?.additional_metadata) {
              safe.additional_metadata = userRoute?.additional_metadata;
            }
            const auth = userRoute?.auth;
            const projectAuth =
              safe.profile || safe.org_name
                ? {
                    ...auth,
                    ...(safe.profile
                      ? {
                          source: "saved_profile" as const,
                          profile_id: undefined,
                          profile: safe.profile,
                        }
                      : {}),
                    ...(safe.org_name ? { org_name: safe.org_name } : {}),
                  }
                : auth;
            pluginConfig = {
              ...pluginConfig,
              ...safe,
              ...(userRoute
                ? {
                    route: {
                      ...userRoute,
                      ...(projectAuth ? { auth: projectAuth } : {}),
                      ...(safe.project
                        ? { destination: { type: "project_logs", project_name: safe.project } }
                        : {}),
                    },
                  }
                : {}),
            };
          }
        }
      } catch {
        // Do not bypass a project opt-out when its settings cannot be read.
        if (index === 1) pluginConfig = { ...pluginConfig, trace_to_braintrust: false };
      }
    }

    if (pluginConfig?.trace_to_braintrust !== true) return pluginConfig;

    const legacyDirectories = new Set([input.directory, path.dirname(projectPath)]);
    for (const directory of legacyDirectories) {
      const configPath = path.join(directory, ".opencode", "braintrust.json");
      try {
        if (!fs.existsSync(configPath)) continue;
        const legacy = JSON.parse(fs.readFileSync(configPath, "utf-8")) as Pick<
          PluginConfig,
          "trace_to_braintrust"
        > | null;
        if (legacy?.trace_to_braintrust === false) {
          return { ...pluginConfig, trace_to_braintrust: false };
        }
      } catch {
        // Preserve the legacy loader's handling of unreadable project files.
      }
    }
  } catch {
    // Config loading failed; invocation settings can still enable tracing.
  }
  return pluginConfig;
}

export function addTracingHooks(
  input: PluginInput,
  config: ReturnType<typeof loadConfig>,
  hooks: Hooks,
): void {
  if (!config.tracingEnabled || !claimManagedTracingInstance("opencode")) return;
  const tracingHooks = createDaemonTracingHooks(input, config, (message, extra) => {
    input.client.app
      .log({ body: { service: "braintrust-trace", level: "warn", message, extra } })
      .catch(() => {});
  });
  Object.assign(hooks, tracingHooks);
  input.client.app
    .log({
      body: {
        service: "braintrust",
        level: "info",
        message: `Tracing hooks registered: ${Object.keys(tracingHooks).join(", ")}`,
      },
    })
    .catch(() => {});
}

/** Trace-only entrypoint injected by `bt trace run opencode`. */
export const BraintrustTracingPlugin: Plugin = async (input: PluginInput) => {
  const config = loadConfig(await readPluginConfig(input));
  const hooks: Hooks = {};
  addTracingHooks(input, config, hooks);
  return hooks;
};
