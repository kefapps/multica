/**
 * Auth Profile Constants
 *
 * Cooldown timings, store version, and file names.
 */

/** Store format version */
export const AUTH_STORE_VERSION = 1;

/** Runtime store filename (inside ~/.super-multica/) */
export const AUTH_PROFILE_STORE_FILENAME = "auth-profiles.json";

// ============================================================
// Non-billing cooldown (rate_limit, auth, timeout, unknown)
// Progression: 1min -> 5min -> 25min -> 1hr (cap)
// Formula: min(MAX, BASE * FACTOR ^ min(errorCount - 1, 3))
// ============================================================

/** Base cooldown duration in milliseconds (1 minute) */
export const COOLDOWN_BASE_MS = 60_000;

/** Exponential factor for cooldown progression */
export const COOLDOWN_FACTOR = 5;

/** Maximum cooldown duration in milliseconds (1 hour) */
export const COOLDOWN_MAX_MS = 3_600_000;

// ============================================================
// Billing disable (longer backoff for payment/quota issues)
// Progression: 5h -> 10h -> 20h -> 24h (cap)
// Formula: min(MAX_HOURS, BASE_HOURS * 2 ^ (count - 1))
// ============================================================

/** Base billing disable duration in hours */
export const BILLING_BACKOFF_HOURS = 5;

/** Maximum billing disable duration in hours */
export const BILLING_MAX_HOURS = 24;

// ============================================================
// Failure window
// ============================================================

/** Failure window in milliseconds (24 hours) — errors older than this are forgotten */
export const FAILURE_WINDOW_MS = 24 * 60 * 60 * 1000;
