import { useState } from 'react'
import { Button } from '@multica/ui/components/ui/button'
import { Input } from '@multica/ui/components/ui/input'
import { Badge } from '@multica/ui/components/ui/badge'
import { HugeiconsIcon } from '@hugeicons/react'
import { ArrowLeft02Icon, Loading03Icon } from '@hugeicons/core-free-icons'
import { useChannels } from '../../../hooks/use-channels'
import { TutorialStep } from '../../../components/onboarding/tutorial-step'
import { useOnboardingStore } from '../../../stores/onboarding'

function statusVariant(
  status: string
): 'default' | 'secondary' | 'destructive' | 'outline' {
  switch (status) {
    case 'running':
      return 'default'
    case 'starting':
      return 'secondary'
    case 'error':
      return 'destructive'
    default:
      return 'outline'
  }
}

interface ConnectStepProps {
  onNext: () => void
  onBack: () => void
}

export default function ConnectStep({ onNext, onBack }: ConnectStepProps) {
  const { states, config, saveToken, loading: channelLoading } = useChannels()
  const { setClientConnected } = useOnboardingStore()

  const [token, setToken] = useState('')
  const [saving, setSaving] = useState(false)
  const [localError, setLocalError] = useState<string | null>(null)

  const state = states.find(
    (s) => s.channelId === 'telegram' && s.accountId === 'default'
  )
  const savedConfig = config['telegram']?.['default'] as
    | { botToken?: string }
    | undefined
  const hasToken = Boolean(savedConfig?.botToken)
  const isRunning = state?.status === 'running'
  const isStarting = state?.status === 'starting'

  const handleConnect = async () => {
    if (!token.trim()) return
    setSaving(true)
    setLocalError(null)
    const result = await saveToken('telegram', 'default', token.trim())
    if (!result.ok) {
      setLocalError(result.error ?? 'Failed to connect')
    } else {
      setToken('')
      setClientConnected(true)
    }
    setSaving(false)
  }

  return (
    <div className="h-full flex">
      {/* Left column */}
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
              Connect a client
            </h1>
            <p className="text-sm text-muted-foreground">
              Connect a Telegram bot so you can chat with your agent from
              anywhere. You can always set this up later in settings.
            </p>
          </div>

          {channelLoading ? (
            <div className="h-24 rounded-xl border border-border bg-card animate-pulse" />
          ) : hasToken ? (
            <div className="p-4 rounded-xl border border-primary/30 bg-card space-y-3">
              <div className="flex items-center justify-between">
                <p className="font-medium text-sm">Telegram Bot</p>
                {state && (
                  <Badge variant={statusVariant(state.status)}>
                    {state.status}
                  </Badge>
                )}
              </div>
              {state?.status === 'error' && state.error && (
                <p className="text-sm text-destructive">{state.error}</p>
              )}
              {isRunning && (
                <p className="text-sm text-muted-foreground">
                  Your bot is running. Send it a message on Telegram to test.
                </p>
              )}
              {isStarting && (
                <p className="text-sm text-muted-foreground">
                  Starting your bot...
                </p>
              )}
            </div>
          ) : (
            <div className="space-y-3">
              <Input
                type="password"
                placeholder="Paste your bot token from @BotFather"
                value={token}
                onChange={(e) => setToken(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleConnect()}
              />
              {localError && (
                <p className="text-sm text-destructive">{localError}</p>
              )}
              <Button
                size="sm"
                onClick={handleConnect}
                disabled={saving || !token.trim()}
              >
                {saving && (
                  <HugeiconsIcon
                    icon={Loading03Icon}
                    className="size-4 animate-spin mr-2"
                  />
                )}
                Connect
              </Button>
            </div>
          )}

          <div className="flex justify-end gap-2">
            {!hasToken && (
              <Button size="lg" variant="ghost" onClick={onNext}>
                Skip
              </Button>
            )}
            <Button size="lg" onClick={onNext} disabled={!isRunning}>
              Continue
            </Button>
          </div>
        </div>
      </div>

      {/* Right column — BotFather tutorial */}
      <div className="flex-1 flex items-center justify-center bg-muted/30 px-12 py-8">
        <div className="max-w-sm space-y-6">
          <div className="space-y-2">
            <h3 className="text-lg font-medium">Create a Telegram bot</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Follow these steps to create your bot:
            </p>
          </div>

          <div className="space-y-4">
            <TutorialStep
              number={1}
              text="Open Telegram and search for @BotFather"
            />
            <TutorialStep
              number={2}
              text="Send /newbot and follow the prompts to name your bot"
            />
            <TutorialStep
              number={3}
              text="BotFather will give you a token like 123456:ABC-DEF..."
            />
            <TutorialStep
              number={4}
              text='Paste the token on the left and click "Connect"'
            />
          </div>

          <div className="space-y-2">
            <h4 className="text-sm font-medium text-muted-foreground">
              Why connect Telegram?
            </h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Once connected, you can chat with your Multica agent directly
              from Telegram on any device — phone, tablet, or desktop.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
