/**
 * Provider Resolver
 *
 * Resolves provider configuration for making API calls,
 * including API keys, OAuth tokens, and model selection.
 */

import { getModel } from "@mariozechner/pi-ai";
import { credentialManager } from "../credentials.js";
import {
  readClaudeCliCredentials,
  readCodexCliCredentials,
} from "./oauth/cli-credentials.js";
import {
  PROVIDER_ALIAS,
  getProviderMeta,
  getDefaultModel,
  isOAuthProvider,
} from "./registry.js";
import type { AgentOptions } from "../types.js";
import {
  loadAuthProfileStore,
  resolveAuthProfileOrder,
  isProfileInCooldown,
} from "../auth-profiles/index.js";
import type { ResolvedProfileAuth } from "../auth-profiles/index.js";

// ============================================================
// Types
// ============================================================

export interface ProviderConfig {
  provider: string;
  model?: string | undefined;
  apiKey?: string | undefined;
  baseUrl?: string | undefined;
  // OAuth specific
  accessToken?: string | undefined;
  refreshToken?: string | undefined;
  expires?: number | undefined;
}

// ============================================================
// Provider Config Resolution
// ============================================================

/**
 * Get provider config for making API calls.
 * Handles both OAuth and API Key authentication.
 */
export function resolveProviderConfig(providerId: string): ProviderConfig | null {
  const meta = getProviderMeta(providerId);
  if (!meta) return null;

  if (meta.authMethod === "oauth") {
    if (providerId === "claude-code") {
      const creds = readClaudeCliCredentials();
      if (!creds) return null;

      const accessToken = creds.type === "oauth" ? creds.access : creds.token;
      return {
        provider: "anthropic", // Use anthropic API
        apiKey: accessToken,
        accessToken,
        refreshToken: creds.type === "oauth" ? creds.refresh : undefined,
        expires: creds.expires,
      };
    }

    if (providerId === "openai-codex") {
      const creds = readCodexCliCredentials();
      if (!creds) return null;

      return {
        provider: "openai-codex",
        accessToken: creds.access,
        refreshToken: creds.refresh,
        expires: creds.expires,
      };
    }
  }

  // API Key based
  const config = credentialManager.getLlmProviderConfig(providerId);
  if (!config?.apiKey) return null;

  return {
    provider: providerId,
    model: config.model,
    apiKey: config.apiKey,
    baseUrl: config.baseUrl,
  };
}

// ============================================================
// API Key Resolution
// ============================================================

/**
 * Get API Key based on provider.
 * Priority: explicit key > OAuth credentials > credentials.json5 config.
 */
export function resolveApiKey(provider: string, explicitKey?: string): string | undefined {
  if (explicitKey) return explicitKey;

  // Try OAuth providers first (claude-code, openai-codex)
  const providerConfig = resolveProviderConfig(provider);
  if (providerConfig?.apiKey) {
    return providerConfig.apiKey;
  }
  if (providerConfig?.accessToken) {
    return providerConfig.accessToken;
  }

  // Fall back to credentials.json5
  return credentialManager.getLlmProviderConfig(provider)?.apiKey;
}

/**
 * Get Base URL based on provider.
 * Priority: explicit URL > credentials.json5 config.
 */
export function resolveBaseUrl(provider: string, explicitUrl?: string): string | undefined {
  if (explicitUrl) return explicitUrl;
  return credentialManager.getLlmProviderConfig(provider)?.baseUrl;
}

/**
 * Get Model ID based on provider.
 * Priority: explicit model > credentials.json5 config > default.
 */
export function resolveModelId(provider: string, explicitModel?: string): string | undefined {
  if (explicitModel) return explicitModel;
  return credentialManager.getLlmProviderConfig(provider)?.model ?? getDefaultModel(provider);
}

// ============================================================
// Profile-aware API Key Resolution
// ============================================================

/**
 * Resolve API key for a specific auth profile ID.
 * Profile IDs follow the convention: "provider" or "provider:label".
 */
export function resolveApiKeyForProfile(profileId: string): string | undefined {
  const config = credentialManager.getLlmProviderConfig(profileId);
  return config?.apiKey;
}

/**
 * Resolve API key by iterating auth profiles for a provider.
 * Returns the first available (non-cooldown) profile with a valid key.
 * Falls back to the legacy single-key resolution if no profiles are configured.
 */
export function resolveApiKeyForProvider(
  provider: string,
  explicitKey?: string,
): ResolvedProfileAuth | undefined {
  if (explicitKey) {
    return { apiKey: explicitKey, profileId: provider, provider };
  }

  // Try OAuth providers first
  const providerConfig = resolveProviderConfig(provider);
  if (providerConfig?.apiKey || providerConfig?.accessToken) {
    const key = providerConfig.apiKey ?? providerConfig.accessToken;
    if (key) return { apiKey: key, profileId: provider, provider };
  }

  // Try auth profiles (multi-key rotation)
  const store = loadAuthProfileStore();
  const candidates = resolveAuthProfileOrder(provider, store);

  if (candidates.length > 0) {
    for (const profileId of candidates) {
      const stats = store.usageStats?.[profileId];
      if (stats && isProfileInCooldown(stats)) continue;

      const apiKey = resolveApiKeyForProfile(profileId);
      if (apiKey) {
        return { apiKey, profileId, provider };
      }
    }
    // All in cooldown — return the first one (will be retried when cooldown expires)
    for (const profileId of candidates) {
      const apiKey = resolveApiKeyForProfile(profileId);
      if (apiKey) {
        return { apiKey, profileId, provider };
      }
    }
  }

  // Fall back to single-key credentials.json5
  const fallbackKey = credentialManager.getLlmProviderConfig(provider)?.apiKey;
  if (fallbackKey) {
    return { apiKey: fallbackKey, profileId: provider, provider };
  }

  return undefined;
}

// ============================================================
// Model Resolution
// ============================================================

/**
 * Resolve model for pi-ai based on provider and options.
 */
export function resolveModel(options: AgentOptions) {
  if (options.provider && options.model) {
    // Map provider alias (e.g., claude-code -> anthropic)
    const actualProvider = PROVIDER_ALIAS[options.provider] ?? options.provider;

    // Type assertion needed because provider/model come from dynamic user config
    return (getModel as (p: string, m: string) => ReturnType<typeof getModel>)(
      actualProvider,
      options.model,
    );
  }

  // If only provider specified, use default model for that provider
  if (options.provider) {
    const actualProvider = PROVIDER_ALIAS[options.provider] ?? options.provider;
    const defaultModel = getDefaultModel(options.provider) ?? getDefaultModel(actualProvider);
    if (defaultModel) {
      return (getModel as (p: string, m: string) => ReturnType<typeof getModel>)(
        actualProvider,
        defaultModel,
      );
    }
  }

  return getModel("kimi-coding", "kimi-k2-thinking");
}

// Re-export for convenience
export { isOAuthProvider };
