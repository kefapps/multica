/**
 * Auth Profile Types
 *
 * Type definitions for the auth profile rotation and cooldown system.
 */

/** Reason for an auth profile failure, determines cooldown behavior */
export type AuthProfileFailureReason =
  | "auth"
  | "format"
  | "rate_limit"
  | "billing"
  | "timeout"
  | "unknown";

/** Per-profile usage and cooldown state (persisted in auth-profiles.json) */
export type ProfileUsageStats = {
  /** Timestamp of last successful use */
  lastUsed?: number | undefined;
  /** Cooldown expiry for non-billing failures (rate_limit, auth, timeout, unknown) */
  cooldownUntil?: number | undefined;
  /** Disable expiry for billing failures (longer backoff) */
  disabledUntil?: number | undefined;
  /** Reason for the current disable period */
  disabledReason?: AuthProfileFailureReason | undefined;
  /** Consecutive error count (resets on success or after failure window) */
  errorCount?: number | undefined;
  /** Per-reason failure counts within the failure window */
  failureCounts?: Partial<Record<AuthProfileFailureReason, number>> | undefined;
  /** Timestamp of the last failure (used for failure window expiry) */
  lastFailureAt?: number | undefined;
};

/** Persisted runtime store for auth profile state */
export type AuthProfileStore = {
  version: number;
  /** Last known good profile per provider */
  lastGood?: Record<string, string> | undefined;
  /** Per-profile usage/cooldown stats */
  usageStats?: Record<string, ProfileUsageStats> | undefined;
};

/** Resolved auth info returned by profile-aware key resolution */
export type ResolvedProfileAuth = {
  apiKey: string;
  profileId: string;
  provider: string;
};
