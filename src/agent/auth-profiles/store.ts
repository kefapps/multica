/**
 * Auth Profile Store
 *
 * Persistence layer for auth profile runtime state.
 * Stores usage stats, cooldowns, and last-good info in ~/.super-multica/auth-profiles.json.
 * Uses a custom file lock (exclusive-create based) for safe concurrent access.
 */

import {
  existsSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  openSync,
  closeSync,
  rmSync,
  statSync,
  constants as fsConstants,
} from "node:fs";
import { join, dirname } from "node:path";
import { DATA_DIR } from "../../shared/paths.js";
import { AUTH_STORE_VERSION, AUTH_PROFILE_STORE_FILENAME } from "./constants.js";
import type { AuthProfileStore } from "./types.js";

// ============================================================
// Custom file lock (synchronous, exclusive-create based)
// ============================================================

const LOCK_STALE_MS = 30_000;
const LOCK_RETRY_COUNT = 10;
const LOCK_RETRY_BASE_MS = 50;
const LOCK_RETRY_MAX_MS = 1_000;

type LockPayload = { pid: number; createdAt: string };

function isProcessAlive(pid: number): boolean {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function readLockPayloadSync(lockPath: string): LockPayload | null {
  try {
    const raw = readFileSync(lockPath, "utf8");
    const parsed = JSON.parse(raw) as Partial<LockPayload>;
    if (typeof parsed.pid !== "number" || typeof parsed.createdAt !== "string") return null;
    return { pid: parsed.pid, createdAt: parsed.createdAt };
  } catch {
    return null;
  }
}

function isLockStale(lockPath: string): boolean {
  const payload = readLockPayloadSync(lockPath);
  if (payload) {
    const age = Date.now() - Date.parse(payload.createdAt);
    if (!Number.isFinite(age) || age > LOCK_STALE_MS) return true;
    return !isProcessAlive(payload.pid);
  }
  // No payload readable — check file mtime
  try {
    const stat = statSync(lockPath);
    return Date.now() - stat.mtimeMs > LOCK_STALE_MS;
  } catch {
    return true; // Can't stat — treat as stale
  }
}

/**
 * Acquire a synchronous exclusive file lock.
 * Returns a release function. Throws if lock cannot be acquired after retries.
 */
function acquireLockSync(filePath: string): () => void {
  const lockPath = `${filePath}.lock`;
  const payload = JSON.stringify(
    { pid: process.pid, createdAt: new Date().toISOString() },
    null,
    2,
  );

  for (let attempt = 0; attempt < LOCK_RETRY_COUNT; attempt++) {
    try {
      // O_WRONLY | O_CREAT | O_EXCL — fails if file already exists
      const fd = openSync(lockPath, fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL);
      writeFileSync(fd, payload, "utf8");
      closeSync(fd);
      return () => {
        try { rmSync(lockPath, { force: true }); } catch { /* best effort */ }
      };
    } catch (err) {
      const code = (err as { code?: string }).code;
      if (code !== "EEXIST") throw err;

      // Lock file exists — check if stale
      if (isLockStale(lockPath)) {
        try { rmSync(lockPath, { force: true }); } catch { /* ignore */ }
        continue;
      }

      // Wait and retry (synchronous busy-wait via Atomics for minimal overhead)
      const delay = Math.min(LOCK_RETRY_MAX_MS, LOCK_RETRY_BASE_MS * (attempt + 1));
      const buf = new SharedArrayBuffer(4);
      Atomics.wait(new Int32Array(buf), 0, 0, delay);
    }
  }

  throw new Error(`Failed to acquire lock after ${LOCK_RETRY_COUNT} retries: ${filePath}`);
}

// ============================================================
// Paths
// ============================================================

/** Resolve the auth profile store file path */
export function resolveAuthStorePath(): string {
  return join(DATA_DIR, AUTH_PROFILE_STORE_FILENAME);
}

// ============================================================
// Load / Save
// ============================================================

function createEmptyStore(): AuthProfileStore {
  return { version: AUTH_STORE_VERSION };
}

/** Coerce raw JSON into a valid AuthProfileStore, defensive against malformed data */
export function coerceStore(raw: unknown): AuthProfileStore {
  if (!raw || typeof raw !== "object") return createEmptyStore();

  const obj = raw as Record<string, unknown>;
  const store: AuthProfileStore = {
    version: typeof obj.version === "number" ? obj.version : AUTH_STORE_VERSION,
  };

  if (obj.lastGood && typeof obj.lastGood === "object") {
    store.lastGood = obj.lastGood as Record<string, string>;
  }
  if (obj.usageStats && typeof obj.usageStats === "object") {
    store.usageStats = obj.usageStats as AuthProfileStore["usageStats"];
  }

  return store;
}

/** Ensure the store file exists on disk (creates it if missing) */
export function ensureAuthStoreFile(): string {
  const storePath = resolveAuthStorePath();
  const dir = dirname(storePath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  if (!existsSync(storePath)) {
    writeFileSync(storePath, JSON.stringify(createEmptyStore(), null, 2), "utf8");
  }
  return storePath;
}

/** Load auth profile store from disk. Returns empty store if file doesn't exist. */
export function loadAuthProfileStore(): AuthProfileStore {
  const storePath = resolveAuthStorePath();
  if (!existsSync(storePath)) return createEmptyStore();

  try {
    const raw = readFileSync(storePath, "utf8");
    return coerceStore(JSON.parse(raw));
  } catch {
    return createEmptyStore();
  }
}

/** Save auth profile store to disk */
export function saveAuthProfileStore(store: AuthProfileStore): void {
  const storePath = resolveAuthStorePath();
  const dir = dirname(storePath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(storePath, JSON.stringify(store, null, 2), "utf8");
}

/**
 * Atomic load-update-save cycle with file locking.
 * Acquires a lock on the store file, loads current state, runs the updater,
 * and saves. Falls back to unlocked update if the lock cannot be acquired.
 * Returns the updated store.
 */
export function updateAuthProfileStore(
  updater: (store: AuthProfileStore) => void,
): AuthProfileStore {
  const storePath = ensureAuthStoreFile();

  try {
    const release = acquireLockSync(storePath);
    try {
      const store = loadAuthProfileStore();
      updater(store);
      saveAuthProfileStore(store);
      return store;
    } finally {
      release();
    }
  } catch {
    // Fallback: unlocked update (better than losing the write entirely)
    const store = loadAuthProfileStore();
    updater(store);
    saveAuthProfileStore(store);
    return store;
  }
}
