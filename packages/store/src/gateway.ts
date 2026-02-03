import { create } from "zustand"
import { GatewayClient, StreamAction, extractTextFromEvent, type ConnectionState, type DeviceInfo, type SendErrorResponse, type StreamPayload, type StreamMessageEvent } from "@multica/sdk"
import { useMessagesStore } from "./messages"

const DEFAULT_GATEWAY_URL = "http://localhost:3000"

interface GatewayState {
  gatewayUrl: string
  connectionState: ConnectionState
  hubId: string | null
  hubs: DeviceInfo[]
  lastError: SendErrorResponse | null
}

interface GatewayActions {
  setGatewayUrl: (url: string) => void
  connect: (deviceId: string) => void
  disconnect: () => void
  setHubId: (hubId: string) => void
  listDevices: () => Promise<DeviceInfo[]>
  send: (to: string, action: string, payload: unknown) => void
  request: <T = unknown>(method: string, params?: unknown) => Promise<T>
}

export type GatewayStore = GatewayState & GatewayActions

let client: GatewayClient | null = null

export const useGatewayStore = create<GatewayStore>()((set, get) => ({
  gatewayUrl: DEFAULT_GATEWAY_URL,
  connectionState: "disconnected",
  hubId: null,
  hubs: [],
  lastError: null,

  setGatewayUrl: (url) => set({ gatewayUrl: url }),

  connect: (deviceId) => {
    if (client) return

    client = new GatewayClient({
      url: get().gatewayUrl,
      deviceId,
      deviceType: "client",
    })
      .onStateChange((connectionState) => set({ connectionState }))
      .onMessage((msg) => {
        // Handle streaming messages (new protocol: payload.event is a raw AgentEvent)
        if (msg.action === StreamAction) {
          const payload = msg.payload as StreamPayload
          const store = useMessagesStore.getState()
          const { event } = payload

          switch (event.type) {
            case "message_start": {
              store.startStream(payload.streamId, payload.agentId)
              const text = extractTextFromEvent(event as StreamMessageEvent)
              if (text) store.appendStream(payload.streamId, text)
              break
            }
            case "message_update": {
              const text = extractTextFromEvent(event as StreamMessageEvent)
              store.appendStream(payload.streamId, text)
              break
            }
            case "message_end": {
              const text = extractTextFromEvent(event as StreamMessageEvent)
              store.endStream(payload.streamId, text)
              break
            }
            case "tool_execution_start":
            case "tool_execution_end":
              // TODO: surface tool execution status in UI
              break
          }
          return
        }

        // Fallback: complete message handling
        const payload = msg.payload as { agentId?: string; content?: string }
        if (payload?.agentId && payload?.content) {
          useMessagesStore.getState().addAssistantMessage(payload.content, payload.agentId)
        }
      })
      .onSendError((error) => set({ lastError: error }))

    client.connect()
  },

  disconnect: () => {
    if (client) {
      client.disconnect()
      client = null
    }
    set({ connectionState: "disconnected", hubId: null, hubs: [] })
  },

  setHubId: (hubId) => set({ hubId }),

  listDevices: async () => {
    if (!client?.isRegistered) return []
    const devices = await client.listDevices()
    const hubs = devices.filter((d) => d.deviceType === "hub")
    set({ hubs })
    return devices
  },

  send: (to, action, payload) => {
    if (!client?.isRegistered) return
    client.send(to, action, payload)
  },

  request: <T = unknown>(method: string, params?: unknown): Promise<T> => {
    const { hubId } = get()
    if (!client?.isRegistered || !hubId) {
      return Promise.reject(new Error("Not connected"))
    }
    return client.request<T>(hubId, method, params)
  },
}))
