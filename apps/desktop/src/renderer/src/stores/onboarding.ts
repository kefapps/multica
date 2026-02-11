import { create } from "zustand"
import { persist } from "zustand/middleware"

interface AcknowledgementsState {
  fileSystem: boolean
  shellExecution: boolean
  llmRequests: boolean
  localStorage: boolean
}

interface OnboardingStore {
  completed: boolean
  forceOnboarding: boolean
  currentStep: number
  acknowledgements: AcknowledgementsState
  allAcknowledged: boolean
  providerConfigured: boolean
  clientConnected: boolean
  setStep: (step: number) => void
  nextStep: () => void
  prevStep: () => void
  setAcknowledgement: (key: keyof AcknowledgementsState, value: boolean) => void
  setProviderConfigured: (configured: boolean) => void
  setClientConnected: (connected: boolean) => void
  completeOnboarding: () => void
  initForceFlag: () => Promise<void>
}

export const useOnboardingStore = create<OnboardingStore>()(
  persist(
    (set, get) => ({
      completed: false,
      forceOnboarding: false,
      currentStep: 0,

      acknowledgements: {
        fileSystem: false,
        shellExecution: false,
        llmRequests: false,
        localStorage: false,
      },
      allAcknowledged: false,
      providerConfigured: false,
      clientConnected: false,

      setStep: (step) => set({ currentStep: step }),
      nextStep: () => set({ currentStep: Math.min(get().currentStep + 1, 4) }),
      prevStep: () => set({ currentStep: Math.max(get().currentStep - 1, 0) }),

      setAcknowledgement: (key, value) => {
        const acknowledgements = { ...get().acknowledgements, [key]: value }
        const allAcknowledged = Object.values(acknowledgements).every(Boolean)
        set({ acknowledgements, allAcknowledged })
      },

      setProviderConfigured: (configured) => set({ providerConfigured: configured }),

      setClientConnected: (connected) => set({ clientConnected: connected }),

      completeOnboarding: () => set({ completed: true, forceOnboarding: false, currentStep: 0 }),

      initForceFlag: async () => {
        const flags = await window.electronAPI.app.getFlags()
        if (flags.forceOnboarding) {
          set({ forceOnboarding: true })
        }
      },
    }),
    {
      name: 'multica-onboarding',
      partialize: (state) => ({ completed: state.completed }),
    }
  )
)
