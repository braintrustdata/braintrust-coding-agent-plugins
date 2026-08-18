/**
 * Trace-only OpenCode plugin entrypoint for invocation-local managed runs.
 * It intentionally omits the optional Braintrust data-access tools.
 */
export { BraintrustTracingPlugin as default } from "./tracing/plugin";
