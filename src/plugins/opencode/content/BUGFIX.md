# Bug Fix: "Cannot call a class constructor without |new|"

## The Problem

When loading the plugin, OpenCode would fail with:
```
TypeError: Cannot call a class constructor without |new|
    at BraintrustClient (/Users/ankur/.config/opencode/plugin/braintrust.js:12672:14)
```

## Root Cause

OpenCode's plugin loader (`packages/opencode/src/plugin/index.ts` line 67-72) iterates through **all exports** of a plugin module and attempts to call each one as a plugin function:

```typescript
for (const [_name, fn] of Object.entries<PluginInstance>(mod)) {
  if (seen.has(fn)) continue
  seen.add(fn)
  const init = await fn(input)  // Line 70 - calls every export as a function!
  hooks.push(init)
}
```

The original plugin was exporting:
- `BraintrustPlugin` (function) ✅
- `default` (same function) ✅
- `BraintrustClient` (class) ❌

When OpenCode tried to call `BraintrustClient(input)` instead of `new BraintrustClient()`, it failed because classes must be called with `new`.

## The Fix

Changed the export in `src/index.ts` from:

```typescript
// BAD - exports the class
export { BraintrustClient } from "./client"
```

To:

```typescript
// GOOD - only exports the type
export type { BraintrustClient } from "./client"
```

Now the plugin only exports:
- `BraintrustPlugin` (function)
- `default` (function)

Both can be safely called by OpenCode's loader.

## Lessons Learned

1. **OpenCode plugins should only export functions** that match the `Plugin` type signature
2. **Export types, not runtime values** for classes and utilities
3. Use `export type { ... }` for TypeScript types that shouldn't appear in the runtime exports
4. Test plugin loading behavior by checking `Object.keys()` of the imported module

## Verification

```bash
# Check exports (should only show functions)
node -e "import('./dist/index.js').then(m => console.log(Object.keys(m)))"
# Output: [ 'BraintrustPlugin', 'default' ]

# Test with OpenCode
BRAINTRUST_API_KEY=test-key opencode
# Should show "Login failed: 401" instead of class constructor error
```
