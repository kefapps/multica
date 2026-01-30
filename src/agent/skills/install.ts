/**
 * Skills Install Module
 *
 * Handles installation of skill dependencies (brew, npm, uv, go, download)
 */

import { spawn } from "node:child_process";
import { createWriteStream, existsSync } from "node:fs";
import { mkdir, stat, unlink } from "node:fs/promises";
import { join, basename, dirname } from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";

import { DATA_DIR } from "../../shared/index.js";
import type { Skill, SkillInstallSpec, SkillsInstallConfig } from "./types.js";
import { getSkillKey } from "./types.js";
import { binaryExists } from "./eligibility.js";
import { serialize, SerializeKeys } from "./serialize.js";

// ============================================================================
// Types
// ============================================================================

export interface SkillInstallRequest {
  /** Skill to install dependencies for */
  skill: Skill;
  /** Specific install spec ID (if skill has multiple) */
  installId?: string | undefined;
  /** Timeout in milliseconds (default: 300000 = 5 min) */
  timeoutMs?: number | undefined;
  /** Install preferences */
  prefs?: SkillsInstallConfig | undefined;
}

export interface SkillInstallResult {
  /** Whether installation succeeded */
  ok: boolean;
  /** Human-readable message */
  message: string;
  /** Command stdout */
  stdout: string;
  /** Command stderr */
  stderr: string;
  /** Exit code (null if not applicable) */
  code: number | null;
}

// ============================================================================
// Constants
// ============================================================================

/** Default timeout for install commands (5 minutes) */
const DEFAULT_TIMEOUT_MS = 300_000;

/** Maximum timeout (15 minutes) */
const MAX_TIMEOUT_MS = 900_000;

/** Tools directory: ~/.super-multica/tools */
const TOOLS_DIR = join(DATA_DIR, "tools");

// ============================================================================
// Command Building
// ============================================================================

/**
 * Build the install command for a given spec
 */
function buildInstallCommand(
  spec: SkillInstallSpec,
  prefs: SkillsInstallConfig = {},
): { argv: string[] | null; error?: string } {
  switch (spec.kind) {
    case "brew": {
      if (!spec.formula) {
        return { argv: null, error: "Missing brew formula" };
      }
      return { argv: ["brew", "install", spec.formula] };
    }

    case "node": {
      if (!spec.package) {
        return { argv: null, error: "Missing node package" };
      }
      const pkg = spec.package;
      switch (prefs.nodeManager) {
        case "pnpm":
          return { argv: ["pnpm", "add", "-g", pkg] };
        case "yarn":
          return { argv: ["yarn", "global", "add", pkg] };
        case "bun":
          return { argv: ["bun", "add", "-g", pkg] };
        default:
          return { argv: ["npm", "install", "-g", pkg] };
      }
    }

    case "uv": {
      if (!spec.package) {
        return { argv: null, error: "Missing uv package" };
      }
      return { argv: ["uv", "tool", "install", spec.package] };
    }

    case "go": {
      if (!spec.module) {
        return { argv: null, error: "Missing go module" };
      }
      return { argv: ["go", "install", spec.module] };
    }

    case "download": {
      // Download is handled separately
      return { argv: null, error: "download_handled_separately" };
    }

    default:
      return { argv: null, error: `Unsupported install kind: ${spec.kind}` };
  }
}

/**
 * Select the preferred install spec from a list
 *
 * Priority:
 * 1. If preferBrew and brew spec exists → brew
 * 2. uv (fast, isolated)
 * 3. node
 * 4. brew (if not preferred but available)
 * 5. go
 * 6. download (last resort)
 */
export function selectPreferredInstallSpec(
  specs: SkillInstallSpec[],
  prefs: SkillsInstallConfig = {},
): SkillInstallSpec | undefined {
  if (specs.length === 0) return undefined;
  if (specs.length === 1) return specs[0];

  const platform = process.platform;

  // Filter by platform
  const eligible = specs.filter((s) => {
    if (!s.os || s.os.length === 0) return true;
    return s.os.includes(platform);
  });

  if (eligible.length === 0) return undefined;
  if (eligible.length === 1) return eligible[0];

  // Priority ordering
  const byKind = (kind: SkillInstallSpec["kind"]) =>
    eligible.find((s) => s.kind === kind);

  if (prefs.preferBrew) {
    const brew = byKind("brew");
    if (brew) return brew;
  }

  return (
    byKind("uv") ??
    byKind("node") ??
    byKind("brew") ??
    byKind("go") ??
    byKind("download") ??
    eligible[0]
  );
}

/**
 * Find install spec by ID
 */
function findInstallSpec(
  specs: SkillInstallSpec[],
  installId: string,
): SkillInstallSpec | undefined {
  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i]!;
    const id = spec.id ?? `${spec.kind}-${i}`;
    if (id === installId) return spec;
  }
  return undefined;
}

// ============================================================================
// Command Execution
// ============================================================================

/**
 * Run a command with timeout
 */
async function runCommand(
  argv: string[],
  options: { timeoutMs: number; env?: NodeJS.ProcessEnv | undefined },
): Promise<{ stdout: string; stderr: string; code: number | null }> {
  const [cmd, ...args] = argv;
  if (!cmd) {
    return { stdout: "", stderr: "Empty command", code: null };
  }

  return new Promise((resolve) => {
    const proc = spawn(cmd, args, {
      env: { ...process.env, ...options.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let killed = false;

    const timeout = setTimeout(() => {
      killed = true;
      proc.kill("SIGTERM");
    }, options.timeoutMs);

    proc.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });

    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code: number | null) => {
      clearTimeout(timeout);
      if (killed) {
        resolve({
          stdout,
          stderr: stderr + "\n[Timed out]",
          code: null,
        });
      } else {
        resolve({ stdout, stderr, code });
      }
    });

    proc.on("error", (err: Error) => {
      clearTimeout(timeout);
      resolve({
        stdout,
        stderr: stderr + "\n" + err.message,
        code: null,
      });
    });
  });
}

// ============================================================================
// Download Support
// ============================================================================

/**
 * Resolve the target directory for downloads
 */
function resolveDownloadTargetDir(skill: Skill, spec: SkillInstallSpec): string {
  if (spec.targetDir?.trim()) {
    // Expand ~ to home directory
    const dir = spec.targetDir.replace(/^~/, process.env.HOME ?? "");
    return dir;
  }
  const key = getSkillKey(skill);
  return join(TOOLS_DIR, key);
}

/**
 * Detect archive type from filename
 */
function detectArchiveType(
  spec: SkillInstallSpec,
  filename: string,
): string | undefined {
  if (spec.archive) return spec.archive;

  const lower = filename.toLowerCase();
  if (lower.endsWith(".tar.gz") || lower.endsWith(".tgz")) return "tar.gz";
  if (lower.endsWith(".tar.bz2") || lower.endsWith(".tbz2")) return "tar.bz2";
  if (lower.endsWith(".zip")) return "zip";
  return undefined;
}

/**
 * Download a file
 */
async function downloadFile(
  url: string,
  destPath: string,
  timeoutMs: number,
): Promise<{ bytes: number }> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok || !response.body) {
      throw new Error(`Download failed: ${response.status} ${response.statusText}`);
    }

    await mkdir(dirname(destPath), { recursive: true });

    const file = createWriteStream(destPath);
    const readable = Readable.fromWeb(response.body as Parameters<typeof Readable.fromWeb>[0]);
    await pipeline(readable, file);

    const stats = await stat(destPath);
    return { bytes: stats.size };
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Extract an archive
 */
async function extractArchive(params: {
  archivePath: string;
  archiveType: string;
  targetDir: string;
  stripComponents?: number | undefined;
  timeoutMs: number;
}): Promise<{ stdout: string; stderr: string; code: number | null }> {
  const { archivePath, archiveType, targetDir, stripComponents, timeoutMs } = params;

  await mkdir(targetDir, { recursive: true });

  if (archiveType === "zip") {
    if (!binaryExists("unzip")) {
      return { stdout: "", stderr: "unzip not found in PATH", code: null };
    }
    return runCommand(["unzip", "-q", "-o", archivePath, "-d", targetDir], {
      timeoutMs,
    });
  }

  // tar.gz or tar.bz2
  if (!binaryExists("tar")) {
    return { stdout: "", stderr: "tar not found in PATH", code: null };
  }

  const argv = ["tar", "xf", archivePath, "-C", targetDir];
  if (typeof stripComponents === "number" && stripComponents > 0) {
    argv.push("--strip-components", String(Math.floor(stripComponents)));
  }

  return runCommand(argv, { timeoutMs });
}

/**
 * Install via download
 */
async function installDownload(
  skill: Skill,
  spec: SkillInstallSpec,
  timeoutMs: number,
): Promise<SkillInstallResult> {
  const url = spec.url?.trim();
  if (!url) {
    return {
      ok: false,
      message: "Missing download URL",
      stdout: "",
      stderr: "",
      code: null,
    };
  }

  // Extract filename from URL
  let filename: string;
  try {
    const parsed = new URL(url);
    filename = basename(parsed.pathname) || "download";
  } catch {
    filename = basename(url) || "download";
  }

  const targetDir = resolveDownloadTargetDir(skill, spec);
  const archivePath = join(targetDir, filename);

  // Download
  let bytes: number;
  try {
    const result = await downloadFile(url, archivePath, timeoutMs);
    bytes = result.bytes;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: false,
      message: `Download failed: ${message}`,
      stdout: "",
      stderr: message,
      code: null,
    };
  }

  // Check if we should extract
  const archiveType = detectArchiveType(spec, filename);
  const shouldExtract = spec.extract ?? Boolean(archiveType);

  if (!shouldExtract) {
    return {
      ok: true,
      message: `Downloaded to ${archivePath} (${bytes} bytes)`,
      stdout: `downloaded=${bytes}`,
      stderr: "",
      code: 0,
    };
  }

  if (!archiveType) {
    return {
      ok: false,
      message: "Extract requested but archive type could not be detected",
      stdout: "",
      stderr: "",
      code: null,
    };
  }

  // Extract
  const extractResult = await extractArchive({
    archivePath,
    archiveType,
    targetDir,
    stripComponents: spec.stripComponents,
    timeoutMs,
  });

  // Clean up archive after extraction
  try {
    await unlink(archivePath);
  } catch {
    // Ignore cleanup errors
  }

  const success = extractResult.code === 0;
  return {
    ok: success,
    message: success
      ? `Downloaded and extracted to ${targetDir}`
      : `Extraction failed: ${extractResult.stderr.trim() || "unknown error"}`,
    stdout: extractResult.stdout.trim(),
    stderr: extractResult.stderr.trim(),
    code: extractResult.code,
  };
}

// ============================================================================
// Main Install Function
// ============================================================================

/**
 * Check if required tool is available for install kind
 */
function checkInstallPrerequisites(
  spec: SkillInstallSpec,
): { ok: true } | { ok: false; message: string } {
  switch (spec.kind) {
    case "brew":
      if (!binaryExists("brew")) {
        return { ok: false, message: "brew not installed. Install Homebrew first." };
      }
      break;
    case "uv":
      if (!binaryExists("uv")) {
        return { ok: false, message: "uv not installed. Run: brew install uv" };
      }
      break;
    case "go":
      if (!binaryExists("go")) {
        return { ok: false, message: "go not installed. Run: brew install go" };
      }
      break;
    case "node": {
      const manager = spec.package ? "npm" : "npm";
      if (!binaryExists(manager)) {
        return { ok: false, message: `${manager} not found in PATH` };
      }
      break;
    }
  }
  return { ok: true };
}

/**
 * Install skill dependencies
 *
 * Operations are serialized to prevent concurrent installations
 * of the same skill from interfering with each other.
 *
 * @param request - Install request
 * @returns Install result
 */
export async function installSkill(
  request: SkillInstallRequest,
): Promise<SkillInstallResult> {
  // Serialize operations for the same skill
  return serialize(SerializeKeys.skillInstall(request.skill.id), () =>
    installSkillInternal(request),
  );
}

/**
 * Internal implementation of installSkill (serialized)
 */
async function installSkillInternal(
  request: SkillInstallRequest,
): Promise<SkillInstallResult> {
  const { skill, installId, prefs } = request;
  const timeoutMs = Math.min(
    Math.max(request.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1000),
    MAX_TIMEOUT_MS,
  );

  // Get install specs from skill metadata
  const specs = skill.frontmatter.metadata?.install ?? [];
  if (specs.length === 0) {
    return {
      ok: false,
      message: `Skill '${skill.id}' has no install specifications`,
      stdout: "",
      stderr: "",
      code: null,
    };
  }

  // Find the spec to use
  let spec: SkillInstallSpec | undefined;
  if (installId) {
    spec = findInstallSpec(specs, installId);
    if (!spec) {
      return {
        ok: false,
        message: `Install spec '${installId}' not found for skill '${skill.id}'`,
        stdout: "",
        stderr: "",
        code: null,
      };
    }
  } else {
    spec = selectPreferredInstallSpec(specs, prefs);
    if (!spec) {
      return {
        ok: false,
        message: `No compatible install spec found for skill '${skill.id}' on ${process.platform}`,
        stdout: "",
        stderr: "",
        code: null,
      };
    }
  }

  // Handle download separately
  if (spec.kind === "download") {
    return installDownload(skill, spec, timeoutMs);
  }

  // Check prerequisites
  const prereq = checkInstallPrerequisites(spec);
  if (!prereq.ok) {
    return {
      ok: false,
      message: (prereq as { ok: false; message: string }).message,
      stdout: "",
      stderr: "",
      code: null,
    };
  }

  // Build command
  const command = buildInstallCommand(spec, prefs);
  if (!command.argv) {
    return {
      ok: false,
      message: command.error ?? "Failed to build install command",
      stdout: "",
      stderr: "",
      code: null,
    };
  }

  // Run command
  const result = await runCommand(command.argv, { timeoutMs });
  const success = result.code === 0;

  return {
    ok: success,
    message: success
      ? `Successfully installed via ${spec.kind}`
      : `Install failed (exit ${result.code}): ${summarizeOutput(result.stderr) || summarizeOutput(result.stdout) || "unknown error"}`,
    stdout: result.stdout.trim(),
    stderr: result.stderr.trim(),
    code: result.code,
  };
}

/**
 * Summarize output for error messages
 */
function summarizeOutput(text: string): string {
  const lines = text.trim().split("\n").filter(Boolean);
  if (lines.length === 0) return "";

  // Look for error lines
  const errorLine =
    lines.find((l) => /^error\b/i.test(l)) ??
    lines.find((l) => /\b(err!|error:|failed)\b/i.test(l)) ??
    lines[lines.length - 1];

  if (!errorLine) return "";

  const normalized = errorLine.replace(/\s+/g, " ").trim();
  const maxLen = 150;
  return normalized.length > maxLen
    ? `${normalized.slice(0, maxLen - 1)}…`
    : normalized;
}

/**
 * Get available install options for a skill
 */
export function getInstallOptions(skill: Skill): Array<{
  id: string;
  kind: SkillInstallSpec["kind"];
  label: string;
  available: boolean;
  reason?: string;
}> {
  const specs = skill.frontmatter.metadata?.install ?? [];
  const platform = process.platform;

  return specs.map((spec, index) => {
    const id = spec.id ?? `${spec.kind}-${index}`;
    const label = spec.label ?? `Install via ${spec.kind}`;

    // Check platform compatibility
    if (spec.os && spec.os.length > 0 && !spec.os.includes(platform)) {
      return {
        id,
        kind: spec.kind,
        label,
        available: false,
        reason: `Not available on ${platform}`,
      };
    }

    // Check prerequisites
    const prereq = checkInstallPrerequisites(spec);
    if (!prereq.ok) {
      return {
        id,
        kind: spec.kind,
        label,
        available: false,
        reason: (prereq as { ok: false; message: string }).message,
      };
    }

    return {
      id,
      kind: spec.kind,
      label,
      available: true,
    };
  });
}
