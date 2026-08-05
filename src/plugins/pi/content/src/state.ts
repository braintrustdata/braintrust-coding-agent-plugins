import { existsSync, readFileSync } from "node:fs";
import { rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { Logger, PersistedSessionState, StateStore } from "./types.ts";
import { ensureDir, isPlainObject } from "./utils.ts";

const STATE_VERSION = 1;
const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const DEFAULT_PERSIST_DEBOUNCE_MS = 50;

interface StoreData {
  version: number;
  sessions: Record<string, PersistedSessionState>;
}

interface StoreBackend {
  stateFile: string;
  state: StoreData;
  dirty: boolean;
  scheduledPersistTimer?: ReturnType<typeof setTimeout>;
  pendingPersist: Promise<void>;
  lastPersistedSnapshot?: string;
  logger?: Logger;
}

const storeBackends = new Map<string, StoreBackend>();

function normalizeSessions(value: unknown): Record<string, PersistedSessionState> {
  if (!isPlainObject(value)) return {};

  const sessions: Record<string, PersistedSessionState> = {};
  for (const [key, session] of Object.entries(value)) {
    if (
      isPlainObject(session) &&
      typeof session.rootSpanId === "string" &&
      typeof session.startedAt === "number"
    ) {
      sessions[key] = {
        rootSpanId: session.rootSpanId,
        rootSpanRecordId:
          typeof session.rootSpanRecordId === "string" ? session.rootSpanRecordId : undefined,
        traceRootSpanId:
          typeof session.traceRootSpanId === "string" ? session.traceRootSpanId : undefined,
        parentSpanId: typeof session.parentSpanId === "string" ? session.parentSpanId : undefined,
        traceUrl: typeof session.traceUrl === "string" ? session.traceUrl : undefined,
        startedAt: session.startedAt,
        totalTurns: typeof session.totalTurns === "number" ? session.totalTurns : undefined,
        totalToolCalls:
          typeof session.totalToolCalls === "number" ? session.totalToolCalls : undefined,
        lastSeenAt: typeof session.lastSeenAt === "number" ? session.lastSeenAt : undefined,
        sessionFile: typeof session.sessionFile === "string" ? session.sessionFile : undefined,
      };
    }
  }

  return sessions;
}

function serializedState(backend: StoreBackend): string {
  return `${JSON.stringify(backend.state, null, 2)}\n`;
}

function markDirty(backend: StoreBackend): void {
  backend.dirty = true;
}

function pruneExpired(backend: StoreBackend): boolean {
  const cutoff = Date.now() - RETENTION_MS;
  let changed = false;

  for (const [key, value] of Object.entries(backend.state.sessions)) {
    if ((value.lastSeenAt ?? value.startedAt ?? 0) < cutoff) {
      delete backend.state.sessions[key];
      changed = true;
    }
  }

  if (changed) markDirty(backend);
  return changed;
}

async function persistDirty(backend: StoreBackend): Promise<void> {
  if (!backend.dirty) {
    await backend.pendingPersist.catch(() => {});
    return;
  }

  const snapshot = serializedState(backend);
  backend.dirty = false;

  backend.pendingPersist = backend.pendingPersist
    .catch(() => {})
    .then(async () => {
      if (snapshot === backend.lastPersistedSnapshot) return;

      const tempFile = `${backend.stateFile}.tmp`;
      try {
        await writeFile(tempFile, snapshot, "utf8");
        await rename(tempFile, backend.stateFile);
        backend.lastPersistedSnapshot = snapshot;
      } catch (error) {
        markDirty(backend);
        backend.logger?.warn("failed to persist state store", { error: String(error) });
      }
    });

  await backend.pendingPersist.catch(() => {});
  if (backend.dirty) await persistDirty(backend);
}

function schedulePersist(backend: StoreBackend, delayMs = DEFAULT_PERSIST_DEBOUNCE_MS): void {
  if (backend.scheduledPersistTimer) clearTimeout(backend.scheduledPersistTimer);
  backend.scheduledPersistTimer = setTimeout(() => {
    backend.scheduledPersistTimer = undefined;
    void persistDirty(backend);
  }, delayMs);
  backend.scheduledPersistTimer.unref?.();
}

function createBackend(stateFile: string, logger?: Logger): StoreBackend {
  let state: StoreData = {
    version: STATE_VERSION,
    sessions: {},
  };

  if (existsSync(stateFile)) {
    try {
      const parsed = JSON.parse(readFileSync(stateFile, "utf8")) as unknown;
      if (isPlainObject(parsed)) {
        state = {
          version: STATE_VERSION,
          sessions: normalizeSessions(parsed.sessions),
        };
      }
    } catch (error) {
      logger?.warn("failed to load state store", { error: String(error) });
    }
  }

  const backend: StoreBackend = {
    stateFile,
    state,
    dirty: false,
    pendingPersist: Promise.resolve(),
    logger,
  };

  backend.lastPersistedSnapshot = serializedState(backend);
  if (pruneExpired(backend)) schedulePersist(backend, 0);
  return backend;
}

export function createStateStore(stateDir: string, logger?: Logger): StateStore {
  ensureDir(stateDir);
  const stateFile = join(stateDir, "sessions.json");

  let backend = storeBackends.get(stateFile);
  if (!backend) {
    backend = createBackend(stateFile, logger);
    storeBackends.set(stateFile, backend);
  } else if (!backend.logger) {
    backend.logger = logger;
  }

  return {
    get(sessionKey) {
      return backend.state.sessions[sessionKey];
    },
    set(sessionKey, value) {
      backend.state.sessions[sessionKey] = value;
      markDirty(backend);
      return backend.state.sessions[sessionKey];
    },
    patch(sessionKey, patch) {
      backend.state.sessions[sessionKey] = {
        ...backend.state.sessions[sessionKey],
        ...patch,
      } as PersistedSessionState;
      markDirty(backend);
      return backend.state.sessions[sessionKey];
    },
    delete(sessionKey) {
      delete backend.state.sessions[sessionKey];
      markDirty(backend);
    },
    schedulePersist(delayMs) {
      schedulePersist(backend, delayMs);
    },
    async flush() {
      if (backend.scheduledPersistTimer) {
        clearTimeout(backend.scheduledPersistTimer);
        backend.scheduledPersistTimer = undefined;
      }
      await persistDirty(backend);
    },
  };
}
