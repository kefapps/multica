import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { repairSessionFileIfNeeded } from "./session-file-repair.js";
import { acquireSessionWriteLock } from "./session-write-lock.js";

vi.mock("./session-write-lock.js", async () => {
  const actual = await vi.importActual<typeof import("./session-write-lock.js")>(
    "./session-write-lock.js",
  );
  return {
    ...actual,
    acquireSessionWriteLock: vi.fn(actual.acquireSessionWriteLock),
  };
});

describe("repairSessionFileIfNeeded", () => {
  beforeEach(() => {
    vi.mocked(acquireSessionWriteLock).mockClear();
  });

  it("rewrites session files that contain malformed lines", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "multica-session-repair-"));
    const file = path.join(dir, "session.jsonl");
    const meta = {
      type: "meta",
      meta: { provider: "kimi", model: "moonshot-v1-128k" },
      timestamp: Date.now(),
    };
    const message = {
      type: "message",
      message: { role: "user", content: "hello" },
      timestamp: Date.now(),
    };

    const content = `${JSON.stringify(meta)}\n${JSON.stringify(message)}\n{"type":"message"`;
    await fs.writeFile(file, content, "utf-8");

    const result = await repairSessionFileIfNeeded({ sessionFile: file });
    expect(result.repaired).toBe(true);
    expect(result.droppedLines).toBe(1);
    expect(result.backupPath).toBeTruthy();

    const repaired = await fs.readFile(file, "utf-8");
    expect(repaired.trim().split("\n")).toHaveLength(2);

    if (result.backupPath) {
      const backup = await fs.readFile(result.backupPath, "utf-8");
      expect(backup).toBe(content);
    }
  });

  it("does not drop CRLF-terminated JSONL lines", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "multica-session-repair-"));
    const file = path.join(dir, "session.jsonl");
    const meta = {
      type: "meta",
      meta: { provider: "kimi", model: "moonshot-v1-128k" },
      timestamp: Date.now(),
    };
    const message = {
      type: "message",
      message: { role: "user", content: "hello" },
      timestamp: Date.now(),
    };
    const content = `${JSON.stringify(meta)}\r\n${JSON.stringify(message)}\r\n`;
    await fs.writeFile(file, content, "utf-8");

    const result = await repairSessionFileIfNeeded({ sessionFile: file });
    expect(result.repaired).toBe(false);
    expect(result.droppedLines).toBe(0);
  });

  it("returns reason when file is empty after dropping all lines", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "multica-session-repair-"));
    const file = path.join(dir, "session.jsonl");
    await fs.writeFile(file, "{broken\n{also broken\n", "utf-8");

    const result = await repairSessionFileIfNeeded({ sessionFile: file });
    expect(result.repaired).toBe(false);
    expect(result.reason).toBe("empty session file");
  });

  it("returns a detailed reason when read errors are not ENOENT", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "multica-session-repair-"));
    const warn = vi.fn();

    const result = await repairSessionFileIfNeeded({ sessionFile: dir, warn });

    expect(result.repaired).toBe(false);
    expect(result.reason).toContain("failed to read session file");
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("acquires a write lock while repairing", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "multica-session-repair-"));
    const file = path.join(dir, "session.jsonl");
    await fs.writeFile(file, "{broken\n{also broken\n", "utf-8");

    await repairSessionFileIfNeeded({ sessionFile: file });

    expect(vi.mocked(acquireSessionWriteLock)).toHaveBeenCalledTimes(1);
  });
});
