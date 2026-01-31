import { type ChildProcess } from "child_process";
import { v7 as uuidv7 } from "uuid";

// Three-layer buffer size constants
export const PENDING_MAX_SIZE = 30 * 1024; // 30KB - recent unread output
export const AGGREGATED_MAX_SIZE = 200 * 1024; // 200KB - accumulated historical output
export const TAIL_SIZE = 1024; // 1KB - last N bytes for quick peek
export const TERMINATED_PROCESS_TTL = 30 * 60 * 1000; // 30 minutes TTL for terminated processes
const SWEEPER_INTERVAL = 5 * 60 * 1000; // 5 minutes

export type ProcessEntry = {
  id: string;
  command: string;
  cwd?: string | undefined;
  child: ChildProcess;
  exitCode: number | null;
  startedAt: number;
  terminatedAt?: number | undefined;
  source: "exec" | "process";

  // Three-layer buffer system
  pendingBuffer: string; // Recent output (30KB)
  aggregatedBuffer: string; // Historical accumulated output (200KB)
  tailBuffer: string; // Last N bytes for quick "tail" view (1KB)

  totalBytesReceived: number;
  truncated: boolean;
};

export const PROCESS_REGISTRY = new Map<string, ProcessEntry>();

// Sweeper state
let sweeperTimer: ReturnType<typeof setInterval> | null = null;

/**
 * Flush pending buffer to aggregated buffer.
 * Truncates from the head if aggregated exceeds max size.
 */
function flushPendingToAggregated(entry: ProcessEntry): void {
  if (!entry.pendingBuffer) return;

  entry.aggregatedBuffer += entry.pendingBuffer;
  entry.pendingBuffer = "";

  // Truncate from head if exceeds max
  if (entry.aggregatedBuffer.length > AGGREGATED_MAX_SIZE) {
    entry.truncated = true;
    entry.aggregatedBuffer = entry.aggregatedBuffer.slice(-AGGREGATED_MAX_SIZE);
  }
}

/**
 * Append output to process buffers with three-layer management.
 *
 * Flow:
 * 1. Append to pending buffer
 * 2. If pending exceeds max, flush to aggregated
 * 3. Always update tail buffer with last TAIL_SIZE bytes
 */
export function appendOutput(entry: ProcessEntry, chunk: string): void {
  entry.totalBytesReceived += chunk.length;

  // 1. Append to pending
  entry.pendingBuffer += chunk;

  // 2. Flush pending to aggregated if exceeds max
  if (entry.pendingBuffer.length > PENDING_MAX_SIZE) {
    flushPendingToAggregated(entry);
  }

  // 3. Update tail buffer (always keep last TAIL_SIZE bytes)
  const combined = entry.aggregatedBuffer + entry.pendingBuffer;
  entry.tailBuffer = combined.slice(-TAIL_SIZE);
}

/**
 * Get full output for a process.
 * Returns aggregated + pending + truncation info.
 */
export function getFullOutput(entry: ProcessEntry): {
  output: string;
  truncated: boolean;
} {
  // Flush pending first to get complete view
  flushPendingToAggregated(entry);

  return {
    output: entry.aggregatedBuffer,
    truncated: entry.truncated,
  };
}

/**
 * Get output snapshot for backgrounding.
 * Returns current aggregated + pending without flushing.
 */
export function getOutputSnapshot(entry: ProcessEntry): {
  output: string;
  truncated: boolean;
} {
  const output = entry.aggregatedBuffer + entry.pendingBuffer;
  return {
    output,
    truncated: entry.truncated || output.length > AGGREGATED_MAX_SIZE,
  };
}

/**
 * Start the sweeper if not already running.
 * Uses unref() so it doesn't prevent process exit.
 */
function ensureSweeperRunning(): void {
  if (sweeperTimer) return;

  sweeperTimer = setInterval(() => {
    cleanupTerminatedProcesses();

    // If registry is empty, stop the sweeper
    if (PROCESS_REGISTRY.size === 0) {
      stopSweeper();
    }
  }, SWEEPER_INTERVAL);

  // Allow process to exit even if sweeper is running
  sweeperTimer.unref();
}

/**
 * Stop the sweeper.
 */
function stopSweeper(): void {
  if (sweeperTimer) {
    clearInterval(sweeperTimer);
    sweeperTimer = null;
  }
}

/**
 * Register a process in the shared registry.
 * Sets up output collection and exit handling.
 */
export function registerProcess(
  child: ChildProcess,
  command: string,
  cwd: string | undefined,
  source: "exec" | "process",
  id?: string,
): string {
  const processId = id ?? uuidv7();

  const entry: ProcessEntry = {
    id: processId,
    command,
    cwd,
    child,
    exitCode: null,
    startedAt: Date.now(),
    source,
    // Three-layer buffer initialization
    pendingBuffer: "",
    aggregatedBuffer: "",
    tailBuffer: "",
    totalBytesReceived: 0,
    truncated: false,
  };

  PROCESS_REGISTRY.set(processId, entry);

  // Collect output using the appendOutput function
  const collectOutput = (data: Buffer) => {
    const text = data.toString("utf8");
    appendOutput(entry, text);
  };

  child.stdout?.on("data", collectOutput);
  child.stderr?.on("data", collectOutput);

  child.on("close", (code) => {
    entry.exitCode = code;
    entry.terminatedAt = Date.now();
    // Flush any remaining pending on close
    flushPendingToAggregated(entry);
  });

  // Start sweeper if not already running
  ensureSweeperRunning();

  return processId;
}

/**
 * Remove terminated processes older than TTL.
 * Returns the number of processes removed.
 */
export function cleanupTerminatedProcesses(): number {
  const now = Date.now();
  let removed = 0;
  for (const [id, entry] of PROCESS_REGISTRY) {
    if (entry.terminatedAt && now - entry.terminatedAt > TERMINATED_PROCESS_TTL) {
      PROCESS_REGISTRY.delete(id);
      removed++;
    }
  }
  return removed;
}
