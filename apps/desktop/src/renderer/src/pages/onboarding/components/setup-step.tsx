import { useState } from 'react'
import { Button } from '@multica/ui/components/ui/button'
import { HugeiconsIcon } from '@hugeicons/react'
import { ArrowLeft02Icon } from '@hugeicons/core-free-icons'
import { useProvider } from '../../../hooks/use-provider'
import { ApiKeyDialog } from '../../../components/api-key-dialog'
import { OAuthDialog } from '../../../components/oauth-dialog'
import { ProviderSetup } from '../../../components/onboarding/provider-setup'
import { TutorialStep } from '../../../components/onboarding/tutorial-step'
import { useOnboardingStore } from '../../../stores/onboarding'

interface SetupStepProps {
  onNext: () => void
  onBack: () => void
}

export default function SetupStep({ onNext, onBack }: SetupStepProps) {
  const { providers, current, loading, error, refresh, setProvider } =
    useProvider()
  const { setProviderConfigured } = useOnboardingStore()

  const [apiKeyDialogOpen, setApiKeyDialogOpen] = useState(false)
  const [oauthDialogOpen, setOauthDialogOpen] = useState(false)
  const [selectedProvider, setSelectedProvider] =
    useState<ProviderStatus | null>(null)
  const [focusedProvider, setFocusedProvider] =
    useState<ProviderStatus | null>(null)

  const hasActiveProvider = current?.available === true

  const handleConfigure = (provider: ProviderStatus) => {
    setSelectedProvider(provider)
    if (provider.authMethod === 'oauth') {
      setOauthDialogOpen(true)
    } else {
      setApiKeyDialogOpen(true)
    }
  }

  const handleSelect = async (provider: ProviderStatus) => {
    await setProvider(provider.id)
  }

  const handleProviderSuccess = async (modelId?: string) => {
    await refresh()
    if (selectedProvider) {
      await setProvider(selectedProvider.id, modelId)
      setFocusedProvider(selectedProvider)
    }
    setProviderConfigured(true)
  }

  return (
    <div className="h-full flex">
      {/* Left column — main content, centered both axes */}
      <div className="flex-1 flex items-center justify-center px-12 py-8">
        <div className="max-w-md w-full space-y-6">
          <button
            onClick={onBack}
            className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            <HugeiconsIcon icon={ArrowLeft02Icon} className="size-4" />
            Back
          </button>

          <div className="space-y-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Connect an LLM provider
            </h1>
            <p className="text-sm text-muted-foreground">
              Multica needs at least one LLM provider to power your AI agent.
              Add your API key below.
            </p>
          </div>

          <ProviderSetup
            providers={providers}
            loading={loading}
            activeProviderId={current?.available ? current.provider : undefined}
            onConfigure={handleConfigure}
            onSelect={handleSelect}
            onFocus={setFocusedProvider}
          />

          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}

          <div className="flex justify-end">
            <Button
              size="lg"
              onClick={onNext}
              disabled={!hasActiveProvider}
            >
              Continue
            </Button>
          </div>
        </div>
      </div>

      {/* Right column — provider tutorial */}
      <div className="flex-1 flex items-center justify-center bg-muted/30 px-12 py-8">
        <div className="max-w-sm space-y-6">
          {focusedProvider ? (
            <ProviderTutorial provider={focusedProvider} />
          ) : (
            <DefaultProviderInfo />
          )}
        </div>
      </div>

      {/* Dialogs */}
      {selectedProvider && selectedProvider.authMethod === 'api-key' && (
        <ApiKeyDialog
          open={apiKeyDialogOpen}
          onOpenChange={setApiKeyDialogOpen}
          providerId={selectedProvider.id}
          providerName={selectedProvider.name}
          showModelInput={selectedProvider.id === 'openrouter'}
          onSuccess={handleProviderSuccess}
        />
      )}
      {selectedProvider && selectedProvider.authMethod === 'oauth' && (
        <OAuthDialog
          open={oauthDialogOpen}
          onOpenChange={setOauthDialogOpen}
          providerId={selectedProvider.id}
          providerName={selectedProvider.name}
          loginCommand={selectedProvider.loginCommand}
          onSuccess={handleProviderSuccess}
        />
      )}
    </div>
  )
}

function ProviderTutorial({ provider }: { provider: ProviderStatus }) {
  return (
    <div className="space-y-5">
      <div className="space-y-2">
        <h3 className="text-lg font-medium">Set up {provider.name}</h3>
        <p className="text-sm text-muted-foreground">
          Follow these steps to get started:
        </p>
      </div>

      <div className="space-y-4">
        {provider.authMethod === 'api-key' ? (
          <>
            <TutorialStep
              number={1}
              text={`Go to ${provider.loginUrl ?? 'the provider dashboard'}`}
              link={provider.loginUrl}
            />
            <TutorialStep number={2} text="Create a new API key" />
            <TutorialStep
              number={3}
              text='Click "Configure" and paste your key'
            />
          </>
        ) : (
          <>
            <TutorialStep
              number={1}
              text={`Open terminal and run: ${provider.loginCommand}`}
            />
            <TutorialStep
              number={2}
              text="Complete login in your browser"
            />
            <TutorialStep
              number={3}
              text='Click "Configure" then "Refresh"'
            />
          </>
        )}
      </div>

      <div className="space-y-2">
        <h4 className="text-sm font-medium text-muted-foreground">
          API keys stay local
        </h4>
        <p className="text-xs text-muted-foreground leading-relaxed">
          Your API keys are stored securely in{' '}
          <code className="bg-muted px-1.5 py-0.5 rounded text-xs">
            ~/.super-multica/credentials.json5
          </code>{' '}
          and never leave your device.
        </p>
      </div>
    </div>
  )
}

function DefaultProviderInfo() {
  return (
    <div className="space-y-6">
      <div className="space-y-3">
        <h3 className="text-lg font-medium">Supported providers</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Multica supports multiple LLM providers including OpenAI,
          Anthropic, DeepSeek, and more. You can configure additional
          providers later in settings.
        </p>
      </div>
      <div className="space-y-3">
        <h3 className="text-lg font-medium">API keys stay local</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Your API keys are stored securely in{' '}
          <code className="text-xs bg-muted px-1.5 py-0.5 rounded">
            ~/.super-multica/credentials.json5
          </code>{' '}
          and never leave your device.
        </p>
      </div>
    </div>
  )
}
