/**
 * Auth Profile Ordering
 *
 * Determines the order in which auth profiles are tried for a given provider.
 * Supports explicit ordering (from credentials.json5) and automatic round-robin
 * with two-level sort: credential type priority (OAuth > API key), then lastUsed.
 * Profiles in cooldown are pushed to the end.
 */

import { credentialManager } from "../credentials.js";
import { isOAuthProvider } from "../providers/registry.js";
import { resolveApiKeyForProfile } from "../providers/resolver.js";
import type { AuthProfileStore } from "./types.js";
import { isProfileInCooldown, resolveProfileUnusableUntil } from "./usage.js";

// ============================================================
// Profile discovery
// ============================================================

/**
 * List all profile IDs from credentials.json5 that belong to a given provider.
 * A profile matches if its key equals the provider exactly or starts with "provider:".
 */
export function listProfilesForProvider(provider: string): string[] {
  return credentialManager.listProfileIdsForProvider(provider);
}

// ============================================================
// Type priority
// ============================================================

/**
 * Get the type-based priority for a profile.
 * OAuth providers (e.g. claude-code, openai-codex) get priority 0 (preferred),
 * API-key providers get priority 1.
 * Lower number = higher priority.
 */
function getProfileTypePriority(profileId: string): number {
  // Extract the provider portion from profileId (before ":" if present)
  const provider = profileId.includes(":") ? profileId.split(":")[0]! : profileId;
  return isOAuthProvider(provider) ? 0 : 1;
}

// ============================================================
// Ordering
// ============================================================

export interface AuthProfileOrderOptions {
  /** Preferred profile to put first (used when user or agent selects a profile) */
  preferredProfile?: string | undefined;
}

/**
 * Resolve the ordered list of profile IDs to try for a given provider.
 *
 * Strategy:
 * 1. If credentials.json5 has `llm.order[provider]`, use that explicit order.
 * 2. Otherwise, use round-robin with two-level sort:
 *    - First by credential type priority (OAuth > API key)
 *    - Then by `lastUsed` ascending within each type (oldest first)
 *
 * In both cases:
 * - Profiles with invalid/missing credentials are filtered out
 * - Profiles currently in cooldown are pushed to the end,
 *   sorted by earliest cooldown expiry (soonest-to-recover first)
 * - If `preferredProfile` is set, it is moved to the front
 */
export function resolveAuthProfileOrder(
  provider: string,
  store: AuthProfileStore,
  now?: number,
  options?: AuthProfileOrderOptions,
): string[] {
  const ts = now ?? Date.now();

  // Gather candidates
  const explicitOrder = credentialManager.getLlmOrder(provider);
  const allProfiles = listProfilesForProvider(provider);

  let candidates: string[];
  if (explicitOrder && explicitOrder.length > 0) {
    // Use explicit order, filter to only existing profiles
    const profileSet = new Set(allProfiles);
    candidates = explicitOrder.filter((id) => profileSet.has(id));
    // Append any profiles not in the explicit order
    for (const id of allProfiles) {
      if (!candidates.includes(id)) {
        candidates.push(id);
      }
    }
  } else {
    // Two-level sort: type priority first, then lastUsed within same type
    candidates = [...allProfiles].sort((a, b) => {
      const priorityDiff = getProfileTypePriority(a) - getProfileTypePriority(b);
      if (priorityDiff !== 0) return priorityDiff;

      const statsA = store.usageStats?.[a];
      const statsB = store.usageStats?.[b];
      return (statsA?.lastUsed ?? 0) - (statsB?.lastUsed ?? 0);
    });
  }

  // Deduplicate
  candidates = [...new Set(candidates)];

  // Filter out profiles with invalid/missing credentials
  candidates = candidates.filter((id) => {
    // For OAuth providers, resolveApiKeyForProfile won't find them in credentials.json5
    // but they are still valid candidates (resolved at runtime via OAuth flow)
    const provider = id.includes(":") ? id.split(":")[0]! : id;
    if (isOAuthProvider(provider)) return true;

    return resolveApiKeyForProfile(id) !== undefined;
  });

  // Partition into available and in-cooldown
  const available: string[] = [];
  const inCooldown: string[] = [];

  for (const id of candidates) {
    const stats = store.usageStats?.[id];
    if (stats && isProfileInCooldown(stats, ts)) {
      inCooldown.push(id);
    } else {
      available.push(id);
    }
  }

  // Sort cooldown profiles by soonest recovery
  inCooldown.sort((a, b) => {
    const statsA = store.usageStats?.[a] ?? {};
    const statsB = store.usageStats?.[b] ?? {};
    return resolveProfileUnusableUntil(statsA) - resolveProfileUnusableUntil(statsB);
  });

  let result = [...available, ...inCooldown];

  // Move preferred profile to front if specified
  if (options?.preferredProfile && result.includes(options.preferredProfile)) {
    result = [
      options.preferredProfile,
      ...result.filter((id) => id !== options.preferredProfile),
    ];
  }

  return result;
}
