import { useNavigate } from 'react-router-dom'
import { Stepper } from '../../components/onboarding/stepper'
import { useOnboardingStore } from '../../stores/onboarding'
import WelcomeStep from './components/welcome-step'
import PermissionsStep from './components/permissions-step'
import SetupStep from './components/setup-step'
import ConnectStep from './components/connect-step'
import TryItStep from './components/try-it-step'

export default function OnboardingPage() {
  const navigate = useNavigate()
  const { currentStep, nextStep, prevStep, completeOnboarding } = useOnboardingStore()

  const handleComplete = () => {
    completeOnboarding()
    navigate('/')
  }

  // Welcome step (step 0) has no stepper
  if (currentStep === 0) {
    return (
      <div className="h-dvh flex flex-col bg-background">
        {/* Draggable title bar region for macOS */}
        <header
          className="shrink-0 h-8"
          style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}
        />
        <main className="flex-1 overflow-auto">
          <WelcomeStep onStart={nextStep} />
        </main>
      </div>
    )
  }

  return (
    <div className="h-dvh flex flex-col bg-background">
      {/* Draggable title bar region for macOS + stepper */}
      <header
        className="shrink-0 px-6 pt-3 pb-2"
        style={{ WebkitAppRegion: 'drag' } as React.CSSProperties}
      >
        {/* Spacer for traffic lights */}
        <div className="h-5" />
        <Stepper currentStep={currentStep} />
      </header>

      {/* Step content */}
      <main className="flex-1 overflow-auto">
        {currentStep === 1 && <PermissionsStep onNext={nextStep} />}
        {currentStep === 2 && <SetupStep onNext={nextStep} onBack={prevStep} />}
        {currentStep === 3 && <ConnectStep onNext={nextStep} onBack={prevStep} />}
        {currentStep === 4 && <TryItStep onComplete={handleComplete} onBack={prevStep} />}
      </main>
    </div>
  )
}
