/**
 * Skills Add Module
 *
 * Add skills from GitHub repositories
 *
 * Supports formats:
 *   - owner/repo           → Clone entire repo to ~/.super-multica/skills/<repo>
 *   - owner/repo/skill     → Download single skill directory
 *   - https://github.com/owner/repo
 */

import { mkdir, rm, readdir, stat, rename } from "node:fs/promises";
import { join, basename } from "node:path";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";

import { DATA_DIR } from "../../shared/index.js";
import { binaryExists } from "./eligibility.js";
import { bumpSkillsVersion } from "./watcher.js";
import { serialize, SerializeKeys } from "./serialize.js";

// ============================================================================
// Types
// ============================================================================

export interface SkillAddRequest {
  /** Source identifier (owner/repo, owner/repo/skill, or full URL) */
  source: string;
  /** Custom name for the skill (defaults to repo or skill name) */
  name?: string | undefined;
  /** Force overwrite if exists */
  force?: boolean | undefined;
  /** Timeout in milliseconds (default: 60000) */
  timeoutMs?: number | undefined;
}

export interface SkillAddResult {
  /** Whether addition succeeded */
  ok: boolean;
  /** Human-readable message */
  message: string;
  /** Path where skill was installed */
  path?: string | undefined;
  /** Skills found (for multi-skill repos) */
  skills?: string[] | undefined;
}

interface ParsedSource {
  /** GitHub owner */
  owner: string;
  /** Repository name */
  repo: string;
  /** Specific skill path within repo (optional) */
  skillPath?: string | undefined;
  /** Branch/tag reference (optional) */
  ref?: string | undefined;
}

// ============================================================================
// Constants
// ============================================================================

/** Default timeout for git operations (60 seconds) */
const DEFAULT_TIMEOUT_MS = 60_000;

/** Skills directory: ~/.super-multica/skills */
const SKILLS_DIR = join(DATA_DIR, "skills");

// ============================================================================
// Source Parsing
// ============================================================================

/**
 * Parse a source identifier into components
 *
 * Supported formats:
 *   - owner/repo
 *   - owner/repo/skill-name
 *   - owner/repo@ref
 *   - owner/repo/skill-name@ref
 *   - https://github.com/owner/repo
 *   - https://github.com/owner/repo/tree/main/skill-name
 */
export function parseSource(source: string): ParsedSource | null {
  const trimmed = source.trim();

  // Handle full GitHub URLs
  if (trimmed.startsWith("https://") || trimmed.startsWith("http://")) {
    return parseGitHubUrl(trimmed);
  }

  // Handle owner/repo format
  return parseShorthand(trimmed);
}

function parseGitHubUrl(url: string): ParsedSource | null {
  try {
    const parsed = new URL(url);

    // Only support github.com
    if (!parsed.hostname.includes("github.com")) {
      return null;
    }

    // Parse path: /owner/repo or /owner/repo/tree/branch/path
    const parts = parsed.pathname.split("/").filter(Boolean);

    if (parts.length < 2) {
      return null;
    }

    const owner = parts[0]!;
    // Remove .git suffix if present
    const repo = parts[1]!.replace(/\.git$/, "");

    // Simple case: /owner/repo
    if (parts.length === 2) {
      return { owner, repo };
    }

    // /owner/repo/tree/branch/path case
    if (parts[2] === "tree" && parts.length >= 4) {
      const ref = parts[3];
      const skillPath = parts.length > 4 ? parts.slice(4).join("/") : undefined;
      return { owner, repo, ref, skillPath };
    }

    // /owner/repo/blob/... - not supported
    if (parts[2] === "blob") {
      return null;
    }

    return { owner, repo };
  } catch {
    return null;
  }
}

function parseShorthand(source: string): ParsedSource | null {
  // Split off @ref if present
  const [pathPart, ref] = source.split("@") as [string, string | undefined];

  const parts = pathPart.split("/").filter(Boolean);

  if (parts.length < 2) {
    return null;
  }

  const owner = parts[0]!;
  const repo = parts[1]!;
  const skillPath = parts.length > 2 ? parts.slice(2).join("/") : undefined;

  return { owner, repo, skillPath, ref };
}

// ============================================================================
// Git Operations
// ============================================================================

/**
 * Run a git command with timeout
 */
async function runGit(
  args: string[],
  options: {
    cwd?: string | undefined;
    timeoutMs: number;
  },
): Promise<{ ok: boolean; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    const proc = spawn("git", args, {
      cwd: options.cwd,
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
        resolve({ ok: false, stdout, stderr: stderr + "\n[Timed out]" });
      } else {
        resolve({ ok: code === 0, stdout, stderr });
      }
    });

    proc.on("error", (err: Error) => {
      clearTimeout(timeout);
      resolve({ ok: false, stdout, stderr: stderr + "\n" + err.message });
    });
  });
}

/**
 * Clone a repository with sparse checkout for a specific path
 */
async function sparseClone(params: {
  repoUrl: string;
  targetDir: string;
  sparsePath: string;
  ref?: string | undefined;
  timeoutMs: number;
}): Promise<{ ok: boolean; message: string }> {
  const { repoUrl, targetDir, sparsePath, ref, timeoutMs } = params;

  // Initialize empty repo
  let result = await runGit(["init", targetDir], { timeoutMs });
  if (!result.ok) {
    return { ok: false, message: `git init failed: ${result.stderr}` };
  }

  // Add remote
  result = await runGit(["remote", "add", "origin", repoUrl], {
    cwd: targetDir,
    timeoutMs,
  });
  if (!result.ok) {
    return { ok: false, message: `git remote add failed: ${result.stderr}` };
  }

  // Enable sparse checkout
  result = await runGit(["config", "core.sparseCheckout", "true"], {
    cwd: targetDir,
    timeoutMs,
  });
  if (!result.ok) {
    return { ok: false, message: `git config failed: ${result.stderr}` };
  }

  // Set sparse checkout path
  result = await runGit(
    ["sparse-checkout", "set", "--no-cone", sparsePath],
    { cwd: targetDir, timeoutMs },
  );
  if (!result.ok) {
    return { ok: false, message: `git sparse-checkout failed: ${result.stderr}` };
  }

  // Fetch and checkout
  const fetchRef = ref ?? "HEAD";
  result = await runGit(["fetch", "--depth=1", "origin", fetchRef], {
    cwd: targetDir,
    timeoutMs,
  });
  if (!result.ok) {
    return { ok: false, message: `git fetch failed: ${result.stderr}` };
  }

  result = await runGit(["checkout", "FETCH_HEAD"], {
    cwd: targetDir,
    timeoutMs,
  });
  if (!result.ok) {
    return { ok: false, message: `git checkout failed: ${result.stderr}` };
  }

  return { ok: true, message: "Sparse clone completed" };
}

/**
 * Shallow clone an entire repository
 */
async function shallowClone(params: {
  repoUrl: string;
  targetDir: string;
  ref?: string | undefined;
  timeoutMs: number;
}): Promise<{ ok: boolean; message: string }> {
  const { repoUrl, targetDir, ref, timeoutMs } = params;

  const args = ["clone", "--depth=1"];

  if (ref) {
    args.push("--branch", ref);
  }

  args.push(repoUrl, targetDir);

  const result = await runGit(args, { timeoutMs });

  if (!result.ok) {
    return { ok: false, message: `git clone failed: ${result.stderr}` };
  }

  return { ok: true, message: "Clone completed" };
}

// ============================================================================
// Skill Detection
// ============================================================================

/**
 * Find SKILL.md files in a directory (recursively, max 2 levels)
 */
async function findSkillFiles(
  dir: string,
  maxDepth: number = 2,
  currentDepth: number = 0,
): Promise<string[]> {
  const results: string[] = [];

  if (currentDepth > maxDepth) {
    return results;
  }

  try {
    const entries = await readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = join(dir, entry.name);

      if (entry.isFile() && entry.name.toUpperCase() === "SKILL.MD") {
        results.push(fullPath);
      } else if (entry.isDirectory() && !entry.name.startsWith(".")) {
        const nested = await findSkillFiles(fullPath, maxDepth, currentDepth + 1);
        results.push(...nested);
      }
    }
  } catch {
    // Ignore read errors
  }

  return results;
}

/**
 * Check if a directory is a valid skill (has SKILL.md)
 */
async function isSkillDirectory(dir: string): Promise<boolean> {
  const skillFile = join(dir, "SKILL.md");
  try {
    const stats = await stat(skillFile);
    return stats.isFile();
  } catch {
    return false;
  }
}

// ============================================================================
// Main Add Function
// ============================================================================

/**
 * Add a skill from a GitHub repository
 *
 * Operations are serialized to prevent concurrent modifications
 * to the same skill directory.
 */
export async function addSkill(request: SkillAddRequest): Promise<SkillAddResult> {
  // Parse source to determine the target name for serialization key
  const parsed = parseSource(request.source);
  const targetName = request.name ?? (parsed?.skillPath ? basename(parsed.skillPath) : parsed?.repo ?? "default");

  // Serialize operations for the same target
  return serialize(SerializeKeys.skillAdd(targetName), () => addSkillInternal(request));
}

/**
 * Internal implementation of addSkill (serialized)
 */
async function addSkillInternal(request: SkillAddRequest): Promise<SkillAddResult> {
  const timeoutMs = request.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  // Check git is available
  if (!binaryExists("git")) {
    return {
      ok: false,
      message: "git is not installed. Please install git first.",
    };
  }

  // Parse source
  const parsed = parseSource(request.source);
  if (!parsed) {
    return {
      ok: false,
      message: `Invalid source format: ${request.source}. Use owner/repo or owner/repo/skill-name`,
    };
  }

  const { owner, repo, skillPath, ref } = parsed;
  const repoUrl = `https://github.com/${owner}/${repo}.git`;

  // Determine target name
  const targetName = request.name ?? (skillPath ? basename(skillPath) : repo);
  const targetDir = join(SKILLS_DIR, targetName);

  // Check if exists
  if (existsSync(targetDir) && !request.force) {
    return {
      ok: false,
      message: `Skill '${targetName}' already exists at ${targetDir}. Use --force to overwrite.`,
    };
  }

  // Ensure skills directory exists
  await mkdir(SKILLS_DIR, { recursive: true });

  // Remove existing if force
  if (existsSync(targetDir)) {
    await rm(targetDir, { recursive: true, force: true });
  }

  // Clone
  let cloneResult: { ok: boolean; message: string };

  if (skillPath) {
    // Sparse checkout for specific skill path
    cloneResult = await sparseClone({
      repoUrl,
      targetDir,
      sparsePath: skillPath,
      ref,
      timeoutMs,
    });

    if (cloneResult.ok) {
      // Move skill contents up from nested path
      const nestedPath = join(targetDir, skillPath);
      if (existsSync(nestedPath)) {
        // Create temp dir, move contents, swap
        const tempDir = `${targetDir}_temp_${Date.now()}`;
        await rename(nestedPath, tempDir);
        await rm(targetDir, { recursive: true, force: true });
        await rename(tempDir, targetDir);
      }
    }
  } else {
    // Full shallow clone
    cloneResult = await shallowClone({
      repoUrl,
      targetDir,
      ref,
      timeoutMs,
    });
  }

  if (!cloneResult.ok) {
    // Clean up on failure
    if (existsSync(targetDir)) {
      await rm(targetDir, { recursive: true, force: true });
    }
    return {
      ok: false,
      message: cloneResult.message,
    };
  }

  // Remove .git directory to save space
  const gitDir = join(targetDir, ".git");
  if (existsSync(gitDir)) {
    await rm(gitDir, { recursive: true, force: true });
  }

  // Find skills in the downloaded content
  const skillFiles = await findSkillFiles(targetDir);

  if (skillFiles.length === 0) {
    // Check if this is a multi-skill repo
    const isSkill = await isSkillDirectory(targetDir);
    if (!isSkill) {
      // Clean up - no valid skill found
      await rm(targetDir, { recursive: true, force: true });
      return {
        ok: false,
        message: `No SKILL.md found in ${request.source}. Is this a valid skill repository?`,
      };
    }
  }

  // Bump version to trigger reload
  bumpSkillsVersion("manual", targetDir);

  // Determine skill names found
  const skillNames = skillFiles.map((f) => {
    const dir = f.replace(/\/SKILL\.md$/i, "");
    return dir === targetDir ? targetName : basename(dir);
  });

  return {
    ok: true,
    message:
      skillNames.length === 1
        ? `Added skill '${targetName}' to ${targetDir}`
        : `Added ${skillNames.length} skills from ${owner}/${repo}`,
    path: targetDir,
    skills: skillNames.length > 0 ? skillNames : [targetName],
  };
}

/**
 * Remove an installed skill
 *
 * Operations are serialized to prevent concurrent modifications.
 */
export async function removeSkill(name: string): Promise<SkillAddResult> {
  return serialize(SerializeKeys.skillRemove(name), () => removeSkillInternal(name));
}

/**
 * Internal implementation of removeSkill (serialized)
 */
async function removeSkillInternal(name: string): Promise<SkillAddResult> {
  const targetDir = join(SKILLS_DIR, name);

  if (!existsSync(targetDir)) {
    return {
      ok: false,
      message: `Skill '${name}' not found at ${targetDir}`,
    };
  }

  try {
    await rm(targetDir, { recursive: true, force: true });
    bumpSkillsVersion("manual", targetDir);

    return {
      ok: true,
      message: `Removed skill '${name}'`,
      path: targetDir,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: false,
      message: `Failed to remove skill: ${message}`,
    };
  }
}

/**
 * List installed skills (in managed directory)
 */
export async function listInstalledSkills(): Promise<string[]> {
  if (!existsSync(SKILLS_DIR)) {
    return [];
  }

  try {
    const entries = await readdir(SKILLS_DIR, { withFileTypes: true });
    const skills: string[] = [];

    for (const entry of entries) {
      if (entry.isDirectory() && !entry.name.startsWith(".")) {
        const hasSkill = await isSkillDirectory(join(SKILLS_DIR, entry.name));
        if (hasSkill) {
          skills.push(entry.name);
        }
      }
    }

    return skills;
  } catch {
    return [];
  }
}
