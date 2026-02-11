import { Button } from '@multica/ui/components/ui/button'
import { Loading } from '@multica/ui/components/ui/loading'
import { ChatView } from '@multica/ui/components/chat-view'
import { HugeiconsIcon } from '@hugeicons/react'
import { ArrowLeft02Icon } from '@hugeicons/core-free-icons'
import { SamplePrompt } from '../../../components/onboarding/sample-prompt'
import { useLocalChat } from '../../../hooks/use-local-chat'

const samplePrompts = [
  {
    title: 'Latest AI news',
    prompt:
      "Search the web for today's top AI news and give me a 3-bullet summary with sources.",
  },
  {
    title: 'Analyze this project',
    prompt:
      'Look at the files in my current directory and give me a brief summary of what this project is about.',
  },
  {
    title: 'Quick task',
    prompt:
      'Write a one-liner shell command that shows my system info (OS, CPU cores, memory) and run it.',
  },
]

interface TryItStepProps {
  onComplete: () => void
  onBack: () => void
}

export default function TryItStep({ onComplete, onBack }: TryItStepProps) {
  const {
    agentId,
    initError,
    messages,
    streamingIds,
    isLoading,
    isLoadingHistory,
    isLoadingMore,
    hasMore,
    error,
    pendingApprovals,
    sendMessage,
    loadMore,
    resolveApproval,
  } = useLocalChat()

  return (
    <div className="h-full flex">
      {/* Left column — prompts */}
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
              Try it out
            </h1>
            <p className="text-sm text-muted-foreground">
              Your agent can search the web, read files, and run commands.
              Click a prompt to see it in action.
            </p>
          </div>

          <div className="space-y-2">
            {samplePrompts.map((sp) => (
              <SamplePrompt
                key={sp.title}
                title={sp.title}
                prompt={sp.prompt}
                onClick={() => sendMessage(sp.prompt)}
              />
            ))}
          </div>

          <div className="flex justify-end">
            <Button size="lg" onClick={onComplete}>
              Open Multica
            </Button>
          </div>
        </div>
      </div>

      {/* Right column — live chat */}
      <div className="flex-1 flex flex-col min-h-0 border-l">
        {initError ? (
          <div className="flex-1 flex items-center justify-center text-sm text-destructive px-8 text-center">
            {initError}
          </div>
        ) : !agentId ? (
          <div className="flex-1 flex items-center justify-center gap-2 text-muted-foreground text-sm">
            <Loading />
            Initializing agent...
          </div>
        ) : (
          <ChatView
            messages={messages}
            streamingIds={streamingIds}
            isLoading={isLoading}
            isLoadingHistory={isLoadingHistory}
            isLoadingMore={isLoadingMore}
            hasMore={hasMore}
            error={error}
            pendingApprovals={pendingApprovals}
            sendMessage={sendMessage}
            loadMore={loadMore}
            resolveApproval={resolveApproval}
          />
        )}
      </div>
    </div>
  )
}
