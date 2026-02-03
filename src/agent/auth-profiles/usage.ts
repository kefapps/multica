/**
 * Auth Profile Usage Tracking
 *
 * Tracks per-profile usage, computes cooldown durations with exponential backoff,
 * and manages failure/success state transitions.
 */

import {
  COOLDOWN_BASE_MS,
  COOLDOWN_FACTOR,
  COOLDOWN_MAX_MS,
  BILLING_BACKOFF_HOURS,
  BILLING_MAX_HOURS,
  FAILURE_WINDOW_MS,
} from "./constants.js";
import { updateAuthProfileStore } from "./store.js";
import type {
  AuthProfileFailureReason,
  AuthProfileStore,
  ProfileUsageStats,
} from "./types.js";

// ============================================================
// Cooldown checks
// ============================================================

/** Returns the timestamp until which a profile is unusable (0 if available) */
export function resolveProfileUnusableUntil(stats: ProfileUsageStats): number {
  return Math.max(stats.cooldownUntil ?? 0, stats.disabledUntil ?? 0);
}

/** Check if a profile is currently in cooldown or disabled */
export function isProfileInCooldown(stats: ProfileUsageStats, now?: number): boolean {
  return resolveProfileUnusableUntil(stats) > (now ?? Date.now());
}

// ============================================================
// Cooldown duration calculation
// ============================================================

/**
 * Calculate non-billing cooldown duration in milliseconds.
 * Exponential backoff: 1min -> 5min -> 25min -> 1hr (cap).
 *
 * Formula: min(COOLDOWN_MAX_MS, COOLDOWN_BASE_MS * COOLDOWN_FACTOR ^ min(errorCount - 1, 3))
 */
function applyEqualJitter(baseMs: number, rng?: () => number): number {
  if (baseMs <= 0) return 0;
  const rand = Math.min(1, Math.max(0, (rng ?? Math.random)()));
  const half = Math.floor(baseMs / 2);
  return half + Math.floor(rand * (baseMs - half));
}

export function calculateCooldownMs(errorCount: number, rng?: () => number): number {
  if (errorCount <= 0) return 0;
  const exponent = Math.min(errorCount - 1, 3);
  const base = Math.min(COOLDOWN_MAX_MS, COOLDOWN_BASE_MS * COOLDOWN_FACTOR ** exponent);
  return applyEqualJitter(base, rng);
}

/**
 * Calculate billing disable duration in milliseconds.
 * Exponential backoff: 5h -> 10h -> 20h -> 24h (cap).
 *
 * Formula: min(BILLING_MAX_HOURS, BILLING_BACKOFF_HOURS * 2 ^ (count - 1)) * hours_to_ms
 */
export function calculateBillingDisableMs(billingFailCount: number, rng?: () => number): number {
  if (billingFailCount <= 0) return 0;
  const hours = Math.min(
    BILLING_MAX_HOURS,
    BILLING_BACKOFF_HOURS * 2 ** (billingFailCount - 1),
  );
  const base = hours * 60 * 60 * 1000;
  return applyEqualJitter(base, rng);
}

// ============================================================
// State transitions
// ============================================================

function ensureUsageStats(store: AuthProfileStore, profileId: string): ProfileUsageStats {
  if (!store.usageStats) store.usageStats = {};
  if (!store.usageStats[profileId]) store.usageStats[profileId] = {};
  return store.usageStats[profileId];
}

/**
 * Compute updated usage stats after a failure.
 * Pure function — does not mutate the input stats.
 */
export function computeNextProfileUsageStats(
  stats: ProfileUsageStats,
  reason: AuthProfileFailureReason,
  now?: number,
  rng?: () => number,
): ProfileUsageStats {
  const ts = now ?? Date.now();
  const next = { ...stats };

  // Reset counters if last failure is outside the failure window
  if (next.lastFailureAt && ts - next.lastFailureAt > FAILURE_WINDOW_MS) {
    next.errorCount = 0;
    next.failureCounts = {};
  }

  // Increment counters
  next.errorCount = (next.errorCount ?? 0) + 1;
  next.lastFailureAt = ts;

  if (!next.failureCounts) next.failureCounts = {};
  next.failureCounts = {
    ...next.failureCounts,
    [reason]: (next.failureCounts[reason] ?? 0) + 1,
  };

  // Apply cooldown based on failure reason
  if (reason === "billing") {
    const billingCount = next.failureCounts.billing ?? 1;
    const disableMs = calculateBillingDisableMs(billingCount, rng);
    next.disabledUntil = ts + disableMs;
    next.disabledReason = "billing";
  } else {
    const cooldownMs = calculateCooldownMs(next.errorCount, rng);
    next.cooldownUntil = ts + cooldownMs;
  }

  return next;
}

/**
 * Mark a profile as having failed. Persists updated stats to disk.
 */
export function markAuthProfileFailure(
  profileId: string,
  reason: AuthProfileFailureReason,
  now?: number,
): void {
  updateAuthProfileStore((store) => {
    const current = ensureUsageStats(store, profileId);
    const next = computeNextProfileUsageStats(current, reason, now);
    store.usageStats![profileId] = next;
  });
}

/**
 * Mark a profile as successfully used. Resets all cooldown/error state.
 */
export function markAuthProfileUsed(profileId: string, now?: number): void {
  updateAuthProfileStore((store) => {
    const stats = ensureUsageStats(store, profileId);
    stats.lastUsed = now ?? Date.now();
    stats.errorCount = 0;
    stats.cooldownUntil = undefined;
    stats.disabledUntil = undefined;
    stats.disabledReason = undefined;
    stats.failureCounts = undefined;
  });
}

/**
 * Mark a profile as the last known good for a provider.
 */
export function markAuthProfileGood(provider: string, profileId: string): void {
  updateAuthProfileStore((store) => {
    if (!store.lastGood) store.lastGood = {};
    store.lastGood[provider] = profileId;
  });
}

/**
 * Clear cooldown for a specific profile.
 */
export function clearAuthProfileCooldown(profileId: string): void {
  updateAuthProfileStore((store) => {
    const stats = ensureUsageStats(store, profileId);
    stats.errorCount = 0;
    stats.cooldownUntil = undefined;
  });
}
