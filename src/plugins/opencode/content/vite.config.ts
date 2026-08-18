import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    ignorePatterns: ["AGENTS.md", "README.md", "dist/**", "src/runtime/daemon-client.ts"],
  },
  lint: {
    ignorePatterns: ["dist/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
  pack: {
    entry: ["src/index.ts", "src/tracing.ts"],
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
});
