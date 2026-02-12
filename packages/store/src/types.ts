import type { ContentBlock } from "@multica/sdk"

export type ToolStatus = "running" | "success" | "error" | "interrupted"

/** Message source: where did this message come from? */
export type MessageSource =
  | { type: "local" }
  | { type: "gateway"; deviceId: string }
  | { type: "channel"; channelId: string; accountId: string; conversationId: string }

export interface CompactionInfo {
  removed: number
  kept: number
  tokensRemoved?: number
  tokensKept?: number
  reason: string
}

export interface Message {
  id: string
  role: "user" | "assistant" | "toolResult" | "system"
  content: ContentBlock[]
  agentId: string
  stopReason?: string
  toolCallId?: string
  toolName?: string
  toolArgs?: Record<string, unknown>
  toolStatus?: ToolStatus
  isError?: boolean
  systemType?: "compaction"
  compaction?: CompactionInfo
  /** Message source (only for user messages) */
  source?: MessageSource
}
