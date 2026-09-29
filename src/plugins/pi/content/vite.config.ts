import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    ignorePatterns: [
      "AGENTS.md",
      "README.md",
      "dist/**",
      "src/runtime/daemon-client.ts",
      "src/runtime/background-process.ts",
    ],
  },
  lint: {
    ignorePatterns: ["dist/**"],
    overrides: [
      {
        files: ["src/**"],
        rules: {
          "no-restricted-imports": [
            "error",
            {
              paths: [
                {
                  name: "child_process",
                  message: "Use the shared src/runtime/background-process helper instead.",
                },
                {
                  name: "node:child_process",
                  message: "Use the shared src/runtime/background-process helper instead.",
                },
              ],
            },
          ],
        },
      },
      {
        files: ["src/runtime/background-process.ts", "**/*.test.ts"],
        rules: {
          "no-restricted-imports": "off",
        },
      },
    ],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
  pack: {
    entry: ["src/index.ts"],
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
});
