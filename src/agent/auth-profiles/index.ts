/**
 * Auth Profiles — barrel export
 */

export type {
  AuthProfileFailureReason,
  AuthProfileStore,
  ProfileUsageStats,
  ResolvedProfileAuth,
} from "./types.js";

export {
  AUTH_STORE_VERSION,
  AUTH_PROFILE_STORE_FILENAME,
  COOLDOWN_BASE_MS,
  COOLDOWN_FACTOR,
  COOLDOWN_MAX_MS,
  BILLING_BACKOFF_HOURS,
  BILLING_MAX_HOURS,
  FAILURE_WINDOW_MS,
} from "./constants.js";

export {
  resolveAuthStorePath,
  coerceStore,
  ensureAuthStoreFile,
  loadAuthProfileStore,
  saveAuthProfileStore,
  updateAuthProfileStore,
} from "./store.js";

export {
  listProfilesForProvider,
  resolveAuthProfileOrder,
  type AuthProfileOrderOptions,
} from "./order.js";

export {
  isProfileInCooldown,
  resolveProfileUnusableUntil,
  calculateCooldownMs,
  calculateBillingDisableMs,
  computeNextProfileUsageStats,
  markAuthProfileFailure,
  markAuthProfileUsed,
  markAuthProfileGood,
  clearAuthProfileCooldown,
} from "./usage.js";
