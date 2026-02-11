import { Button } from '@multica/ui/components/ui/button'
import {
  FolderOpenIcon,
  CommandLineIcon,
  AiBrainIcon,
  Database01Icon,
} from '@hugeicons/core-free-icons'
import { AcknowledgementItem } from '../../../components/onboarding/permission-item'
import { PrivacyPanel } from '../../../components/onboarding/privacy-panel'
import { useOnboardingStore } from '../../../stores/onboarding'

const acknowledgementItems = [
  {
    key: 'fileSystem' as const,
    icon: FolderOpenIcon,
    title: 'File system access',
    description:
      'Multica reads and writes files on your machine to complete tasks you assign.',
  },
  {
    key: 'shellExecution' as const,
    icon: CommandLineIcon,
    title: 'Shell command execution',
    description:
      'The agent may run shell commands. Every command requires your explicit approval.',
  },
  {
    key: 'llmRequests' as const,
    icon: AiBrainIcon,
    title: 'LLM API requests',
    description:
      'Multica sends prompts to your configured LLM provider. Your API key is used directly.',
  },
  {
    key: 'localStorage' as const,
    icon: Database01Icon,
    title: 'Local data storage',
    description:
      'Sessions, profiles, and credentials are stored locally in ~/.super-multica/',
  },
]

interface PermissionsStepProps {
  onNext: () => void
}

export default function PermissionsStep({ onNext }: PermissionsStepProps) {
  const { acknowledgements, allAcknowledged, setAcknowledgement } =
    useOnboardingStore()

  return (
    <div className="h-full flex">
      {/* Left column — main content, centered both axes */}
      <div className="flex-1 flex items-center justify-center px-12 py-8">
        <div className="max-w-md w-full space-y-6">
          <div className="space-y-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Privacy & trust
            </h1>
            <p className="text-sm text-muted-foreground">
              Multica works locally on your machine. Review what it accesses
              and toggle each item to acknowledge.
            </p>
          </div>

          <div className="space-y-3">
            {acknowledgementItems.map((item) => (
              <AcknowledgementItem
                key={item.key}
                icon={item.icon}
                title={item.title}
                description={item.description}
                checked={acknowledgements[item.key]}
                onCheckedChange={(checked) =>
                  setAcknowledgement(item.key, checked)
                }
              />
            ))}
          </div>

          <div className="flex justify-end">
            <Button
              size="lg"
              onClick={onNext}
              disabled={!allAcknowledged}
            >
              Continue
            </Button>
          </div>
        </div>
      </div>

      {/* Right column — privacy panel */}
      <div className="flex-1 flex items-center justify-center bg-muted/30 px-12 py-8">
        <div className="max-w-sm">
          <PrivacyPanel />
        </div>
      </div>
    </div>
  )
}
