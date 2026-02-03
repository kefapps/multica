import { describe, it, expect } from "vitest";
import {
  calculateCooldownMs,
  calculateBillingDisableMs,
  computeNextProfileUsageStats,
  isProfileInCooldown,
  resolveProfileUnusableUntil,
} from "./usage.js";
import {
  COOLDOWN_BASE_MS,
  COOLDOWN_MAX_MS,
  FAILURE_WINDOW_MS,
} from "./constants.js";
import type { ProfileUsageStats } from "./types.js";

// ============================================================
// calculateCooldownMs
// ============================================================

describe("calculateCooldownMs", () => {
  it("applies exponential backoff with a 1h cap", () => {
    const max = () => 1; // equal-jitter max
    expect(calculateCooldownMs(1, max)).toBe(60_000);         // 1 min
    expect(calculateCooldownMs(2, max)).toBe(5 * 60_000);     // 5 min
    expect(calculateCooldownMs(3, max)).toBe(25 * 60_000);    // 25 min
    expect(calculateCooldownMs(4, max)).toBe(60 * 60_000);    // 1 hour (cap)
    expect(calculateCooldownMs(5, max)).toBe(60 * 60_000);    // 1 hour (cap)
    expect(calculateCooldownMs(100, max)).toBe(60 * 60_000);  // still capped
  });

  it("returns 0 for errorCount <= 0", () => {
    expect(calculateCooldownMs(0)).toBe(0);
    expect(calculateCooldownMs(-1)).toBe(0);
  });

  it("applies equal jitter with a 50% floor", () => {
    const min = () => 0;
    expect(calculateCooldownMs(1, min)).toBe(30_000); // 50% of 1 min
  });
});

// ============================================================
// calculateBillingDisableMs
// ============================================================

describe("calculateBillingDisableMs", () => {
  it("applies exponential backoff with a 24h cap", () => {
    const h = 60 * 60 * 1000;
    const max = () => 1;
    expect(calculateBillingDisableMs(1, max)).toBe(5 * h);    // 5h
    expect(calculateBillingDisableMs(2, max)).toBe(10 * h);   // 10h
    expect(calculateBillingDisableMs(3, max)).toBe(20 * h);   // 20h
    expect(calculateBillingDisableMs(4, max)).toBe(24 * h);   // 24h (cap)
    expect(calculateBillingDisableMs(5, max)).toBe(24 * h);   // still capped
  });

  it("returns 0 for count <= 0", () => {
    expect(calculateBillingDisableMs(0)).toBe(0);
    expect(calculateBillingDisableMs(-1)).toBe(0);
  });
});

// ============================================================
// isProfileInCooldown / resolveProfileUnusableUntil
// ============================================================

describe("isProfileInCooldown", () => {
  const now = 1_000_000;

  it("returns false for empty stats", () => {
    expect(isProfileInCooldown({}, now)).toBe(false);
  });

  it("returns true when cooldownUntil is in the future", () => {
    expect(isProfileInCooldown({ cooldownUntil: now + 1000 }, now)).toBe(true);
  });

  it("returns false when cooldownUntil has passed", () => {
    expect(isProfileInCooldown({ cooldownUntil: now - 1 }, now)).toBe(false);
  });

  it("returns true when disabledUntil is in the future", () => {
    expect(isProfileInCooldown({ disabledUntil: now + 1000 }, now)).toBe(true);
  });

  it("uses max of cooldownUntil and disabledUntil", () => {
    const stats: ProfileUsageStats = {
      cooldownUntil: now - 1,
      disabledUntil: now + 5000,
    };
    expect(isProfileInCooldown(stats, now)).toBe(true);
    expect(resolveProfileUnusableUntil(stats)).toBe(now + 5000);
  });
});

// ============================================================
// computeNextProfileUsageStats
// ============================================================

describe("computeNextProfileUsageStats", () => {
  const now = 1_700_000_000_000;

  it("increments errorCount and sets cooldown for non-billing failure", () => {
    const next = computeNextProfileUsageStats({}, "rate_limit", now, () => 1);
    expect(next.errorCount).toBe(1);
    expect(next.lastFailureAt).toBe(now);
    expect(next.cooldownUntil).toBe(now + COOLDOWN_BASE_MS);
    expect(next.failureCounts?.rate_limit).toBe(1);
    expect(next.disabledUntil).toBeUndefined();
  });

  it("applies exponential backoff on consecutive failures", () => {
    const stats: ProfileUsageStats = {
      errorCount: 2,
      lastFailureAt: now - 1000,
      failureCounts: { rate_limit: 2 },
    };
    const next = computeNextProfileUsageStats(stats, "rate_limit", now, () => 1);
    expect(next.errorCount).toBe(3);
    // Error 3 -> 25 min cooldown
    expect(next.cooldownUntil).toBe(now + 25 * 60_000);
  });

  it("sets disabledUntil for billing failures (~5h by default)", () => {
    const next = computeNextProfileUsageStats({}, "billing", now, () => 1);
    expect(next.errorCount).toBe(1);
    expect(next.disabledUntil).toBe(now + 5 * 60 * 60 * 1000);
    expect(next.disabledReason).toBe("billing");
    expect(next.failureCounts?.billing).toBe(1);
  });

  it("resets counters when lastFailureAt is outside the failure window", () => {
    const oldFailure = now - FAILURE_WINDOW_MS - 1000;
    const stats: ProfileUsageStats = {
      errorCount: 5,
      lastFailureAt: oldFailure,
      failureCounts: { auth: 3, rate_limit: 2 },
    };
    const next = computeNextProfileUsageStats(stats, "auth", now, () => 1);
    // Counters reset, so this is treated as error #1
    expect(next.errorCount).toBe(1);
    expect(next.failureCounts?.auth).toBe(1);
    expect(next.cooldownUntil).toBe(now + COOLDOWN_BASE_MS);
  });

  it("caps cooldown at COOLDOWN_MAX_MS", () => {
    const stats: ProfileUsageStats = {
      errorCount: 10,
      lastFailureAt: now - 1000,
    };
    const next = computeNextProfileUsageStats(stats, "unknown", now, () => 1);
    expect(next.cooldownUntil).toBe(now + COOLDOWN_MAX_MS);
  });
});
