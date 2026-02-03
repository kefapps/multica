/**
 * Provider Management
 *
 * Unified exports for LLM provider management:
 * - Registry: Provider metadata, status checking, listing
 * - Resolver: API key resolution, model resolution
 */

// Registry exports
export {
  type AuthMethod,
  type ProviderInfo,
  type ProviderMeta,
  PROVIDER_ALIAS,
  isOAuthProvider,
  isProviderAvailable,
  getCurrentProvider,
  getProviderMeta,
  getDefaultModel,
  getProviderList,
  getAvailableProviders,
  formatProviderStatus,
  getLoginInstructions,
} from "./registry.js";

// Resolver exports
export {
  type ProviderConfig,
  resolveProviderConfig,
  resolveApiKey,
  resolveApiKeyForProfile,
  resolveApiKeyForProvider,
  resolveBaseUrl,
  resolveModelId,
  resolveModel,
} from "./resolver.js";
