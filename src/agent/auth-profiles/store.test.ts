import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { coerceStore, loadAuthProfileStore, saveAuthProfileStore, updateAuthProfileStore } from "./store.js";
import { AUTH_STORE_VERSION } from "./constants.js";
import type { AuthProfileStore } from "./types.js";

// Use a temp directory for tests to avoid touching real store
const TEST_DIR = join(import.meta.dirname ?? ".", "__test_store_tmp__");
const TEST_STORE_PATH = join(TEST_DIR, "auth-profiles.json");

// We need to mock resolveAuthStorePath to point to our test dir
import { vi } from "vitest";

vi.mock("../../shared/paths.js", () => ({
  DATA_DIR: join(import.meta.dirname ?? ".", "__test_store_tmp__"),
}));

beforeEach(() => {
  if (!existsSync(TEST_DIR)) {
    mkdirSync(TEST_DIR, { recursive: true });
  }
});

afterEach(() => {
  if (existsSync(TEST_DIR)) {
    rmSync(TEST_DIR, { recursive: true, force: true });
  }
});

// ============================================================
// coerceStore
// ============================================================

describe("coerceStore", () => {
  it("returns empty store for null", () => {
    const store = coerceStore(null);
    expect(store.version).toBe(AUTH_STORE_VERSION);
    expect(store.lastGood).toBeUndefined();
    expect(store.usageStats).toBeUndefined();
  });

  it("returns empty store for non-object", () => {
    expect(coerceStore("hello").version).toBe(AUTH_STORE_VERSION);
    expect(coerceStore(42).version).toBe(AUTH_STORE_VERSION);
    expect(coerceStore(undefined).version).toBe(AUTH_STORE_VERSION);
  });

  it("preserves valid store data", () => {
    const raw = {
      version: 1,
      lastGood: { anthropic: "anthropic:backup" },
      usageStats: {
        "anthropic": { lastUsed: 1000, errorCount: 0 },
      },
    };
    const store = coerceStore(raw);
    expect(store.version).toBe(1);
    expect(store.lastGood?.anthropic).toBe("anthropic:backup");
    expect(store.usageStats?.anthropic?.lastUsed).toBe(1000);
  });

  it("defaults version when missing", () => {
    const store = coerceStore({ lastGood: {} });
    expect(store.version).toBe(AUTH_STORE_VERSION);
  });
});

// ============================================================
// loadAuthProfileStore / saveAuthProfileStore
// ============================================================

describe("loadAuthProfileStore / saveAuthProfileStore", () => {
  it("returns empty store when file does not exist", () => {
    const store = loadAuthProfileStore();
    expect(store.version).toBe(AUTH_STORE_VERSION);
  });

  it("round-trips save and load", () => {
    const original: AuthProfileStore = {
      version: 1,
      lastGood: { anthropic: "anthropic:main" },
      usageStats: {
        "anthropic:main": { lastUsed: 5000, errorCount: 1 },
      },
    };
    saveAuthProfileStore(original);
    const loaded = loadAuthProfileStore();
    expect(loaded).toEqual(original);
  });

  it("handles corrupted JSON gracefully", () => {
    writeFileSync(TEST_STORE_PATH, "not valid json{{{", "utf8");
    const store = loadAuthProfileStore();
    expect(store.version).toBe(AUTH_STORE_VERSION);
  });
});

// ============================================================
// updateAuthProfileStore
// ============================================================

describe("updateAuthProfileStore", () => {
  it("creates file and applies update when file does not exist", () => {
    const result = updateAuthProfileStore((store) => {
      if (!store.lastGood) store.lastGood = {};
      store.lastGood.openai = "openai:primary";
    });
    expect(result.lastGood?.openai).toBe("openai:primary");

    // Verify persisted
    const loaded = loadAuthProfileStore();
    expect(loaded.lastGood?.openai).toBe("openai:primary");
  });

  it("preserves existing data across updates", () => {
    saveAuthProfileStore({
      version: 1,
      lastGood: { anthropic: "anthropic" },
    });

    updateAuthProfileStore((store) => {
      if (!store.usageStats) store.usageStats = {};
      store.usageStats["anthropic"] = { lastUsed: 9999 };
    });

    const loaded = loadAuthProfileStore();
    expect(loaded.lastGood?.anthropic).toBe("anthropic");
    expect(loaded.usageStats?.anthropic?.lastUsed).toBe(9999);
  });
});
