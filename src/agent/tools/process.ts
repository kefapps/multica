import { spawn } from "child_process";
import { Type } from "@sinclair/typebox";
import type { AgentTool } from "@mariozechner/pi-agent-core";
import { v7 as uuidv7 } from "uuid";
import {
  PROCESS_REGISTRY,
  registerProcess,
  cleanupTerminatedProcesses,
  getFullOutput,
} from "./process-registry.js";

const ProcessSchema = Type.Object({
  action: Type.String({ description: "Action: start | status | stop | output | cleanup." }),
  id: Type.Optional(Type.String({ description: "Process id for status/stop/output." })),
  command: Type.Optional(Type.String({ description: "Command to run for start." })),
  cwd: Type.Optional(Type.String({ description: "Working directory." })),
});

export type ProcessResult = {
  id?: string | undefined;
  running?: boolean | undefined;
  exitCode?: number | null | undefined;
  message?: string | undefined;
  output?: string | undefined;
};

export function createProcessTool(defaultCwd?: string): AgentTool<typeof ProcessSchema, ProcessResult> {
  return {
    name: "process",
    label: "Process",
    description: "Manage long-running background processes like servers, watchers, or daemons. Actions: 'start' to launch (returns immediately with process id), 'status' to check if running, 'output' to read stdout/stderr, 'stop' to terminate, 'cleanup' to remove terminated processes from memory. Use this for servers (e.g., python server.py, npm run dev) instead of 'exec'.",
    parameters: ProcessSchema,
    execute: async (_toolCallId, params, signal) => {
      // Auto-cleanup old terminated processes on each invocation
      cleanupTerminatedProcesses();

      const action = String(params.action ?? "").toLowerCase();
      if (!action) {
        throw new Error("Missing action");
      }

      if (action === "start") {
        const command = String(params.command ?? "");
        if (!command) throw new Error("Missing command");
        const id = params.id ? String(params.id) : uuidv7();
        if (PROCESS_REGISTRY.has(id)) {
          throw new Error(`Process already exists: ${id}`);
        }

        const cwd = params.cwd || defaultCwd;

        // 使用 Promise 等待进程启动或失败
        const result = await new Promise<{ success: boolean; error?: string }>((resolve) => {
          const child = spawn(command, {
            shell: true,
            cwd,
            stdio: ["ignore", "pipe", "pipe"],
            detached: true,
          });

          let resolved = false;

          // 处理 spawn 错误（如 shell 不存在）
          child.on("error", (err) => {
            if (!resolved) {
              resolved = true;
              resolve({ success: false, error: err.message });
            }
          });

          // 进程启动成功后注册到 registry
          child.on("spawn", () => {
            if (!resolved) {
              resolved = true;
              registerProcess(child, command, cwd, "process", id);

              if (signal) {
                signal.addEventListener("abort", () => {
                  child.kill("SIGTERM");
                });
              }

              resolve({ success: true });
            }
          });
        });

        if (!result.success) {
          return {
            content: [{ type: "text", text: `Failed to start process: ${result.error}` }],
            details: { id, running: false, message: result.error },
          };
        }

        return {
          content: [{ type: "text", text: `Started process ${id}` }],
          details: { id, running: true },
        };
      }

      if (action === "status") {
        const id = String(params.id ?? "");
        const entry = PROCESS_REGISTRY.get(id);
        if (!entry) {
          return {
            content: [{ type: "text", text: `Process not found: ${id}` }],
            details: { id, running: false },
          };
        }
        const running = entry.exitCode === null;
        return {
          content: [{ type: "text", text: running ? `Process running: ${id}` : `Process exited: ${id}` }],
          details: { id, running, exitCode: entry.exitCode },
        };
      }

      if (action === "stop") {
        const id = String(params.id ?? "");
        const entry = PROCESS_REGISTRY.get(id);
        if (!entry) {
          return {
            content: [{ type: "text", text: `Process not found: ${id}` }],
            details: { id, running: false },
          };
        }
        entry.child.kill("SIGTERM");
        return {
          content: [{ type: "text", text: `Stopped process ${id}` }],
          details: { id, running: false },
        };
      }

      if (action === "output") {
        const id = String(params.id ?? "");
        const entry = PROCESS_REGISTRY.get(id);
        if (!entry) {
          return {
            content: [{ type: "text", text: `Process not found: ${id}` }],
            details: { id, running: false },
          };
        }
        const { output, truncated } = getFullOutput(entry);
        const running = entry.exitCode === null;
        return {
          content: [{ type: "text", text: (output || "(no output)") + (truncated ? "\n[output truncated]" : "") }],
          details: { id, running, exitCode: entry.exitCode, output },
        };
      }

      if (action === "cleanup") {
        // Remove specific terminated process, or all terminated processes if no id
        const id = params.id ? String(params.id) : undefined;
        if (id) {
          const entry = PROCESS_REGISTRY.get(id);
          if (!entry) {
            return {
              content: [{ type: "text", text: `Process not found: ${id}` }],
              details: { id, running: false },
            };
          }
          if (entry.exitCode === null) {
            return {
              content: [{ type: "text", text: `Process still running: ${id}` }],
              details: { id, running: true },
            };
          }
          PROCESS_REGISTRY.delete(id);
          return {
            content: [{ type: "text", text: `Removed process: ${id}` }],
            details: { id, running: false, message: "cleaned up" },
          };
        }
        // Remove all terminated processes
        let removed = 0;
        for (const [entryId, entry] of PROCESS_REGISTRY) {
          if (entry.exitCode !== null) {
            PROCESS_REGISTRY.delete(entryId);
            removed++;
          }
        }
        return {
          content: [{ type: "text", text: `Removed ${removed} terminated process(es)` }],
          details: { message: `cleaned up ${removed} processes` },
        };
      }

      throw new Error(`Unknown action: ${action}`);
    },
  };
}
