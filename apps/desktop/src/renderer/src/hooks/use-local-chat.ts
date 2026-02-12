import { useState, useEffect, useCallback, useRef } from 'react'
import { useChat, type MessageSource } from '@multica/hooks/use-chat'
import type {
  StreamPayload,
  ExecApprovalRequestPayload,
  ApprovalDecision,
  AgentMessageItem,
  AgentErrorEvent,
} from '@multica/sdk'
import { DEFAULT_MESSAGES_LIMIT } from '@multica/sdk'

export function useLocalChat() {
  const chat = useChat()
  const chatRef = useRef(chat)
  chatRef.current = chat
  const [agentId, setAgentId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isLoadingHistory, setIsLoadingHistory] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const isLoadingMoreRef = useRef(false)
  const [initError, setInitError] = useState<string | null>(null)
  const initRef = useRef(false)
  const offsetRef = useRef<number | null>(null)

  // Initialize hub and get default agent ID
  useEffect(() => {
    if (initRef.current) return
    initRef.current = true

    window.electronAPI.hub.init()
      .then((result) => {
        const r = result as { defaultAgentId?: string }
        console.log('[LocalChat] hub.init → defaultAgentId:', r.defaultAgentId)
        if (r.defaultAgentId) {
          setAgentId(r.defaultAgentId)
        } else {
          setInitError('No default agent available')
          setIsLoadingHistory(false)
        }
      })
      .catch((err: Error) => {
        setInitError(err.message)
        setIsLoadingHistory(false)
      })
  }, [])

  // Subscribe to events + fetch history once agentId is available
  useEffect(() => {
    if (!agentId) return

    // Subscribe to agent events
    window.electronAPI.localChat.subscribe(agentId).catch(() => {})

    // Listen for stream events
    window.electronAPI.localChat.onEvent((data) => {
      // Cast IPC event to StreamPayload (same shape: { agentId, streamId, event })
      const payload = data as unknown as StreamPayload
      if (!payload.event) return

      // Handle agent error events
      if (payload.event.type === 'agent_error') {
        const errorEvent = payload.event as AgentErrorEvent
        chatRef.current.setError({ code: 'AGENT_ERROR', message: errorEvent.message })
        setIsLoading(false)
        return
      }

      chatRef.current.handleStream(payload)
      if (payload.event.type === 'message_start') setIsLoading(true)
      if (payload.event.type === 'message_end') setIsLoading(false)
    })

    // Listen for exec approval requests
    window.electronAPI.localChat.onApproval((approval) => {
      chatRef.current.addApproval(approval as ExecApprovalRequestPayload)
    })

    // Listen for inbound messages from all sources (gateway, channel)
    // This allows the local UI to display messages from other sources
    window.electronAPI.hub.onInboundMessage((event: InboundMessageEvent) => {
      // Only add non-local messages (local messages are added by sendMessage)
      if (event.source.type !== 'local' && event.agentId === agentId) {
        chatRef.current.addUserMessage(event.content, event.agentId, event.source as MessageSource)
        setIsLoading(true)
      }
    })

    // Fetch history with pagination
    window.electronAPI.localChat.getHistory(agentId, { limit: DEFAULT_MESSAGES_LIMIT })
      .then((result) => {
        console.log('[LocalChat] getHistory result:', result.messages?.length, 'messages, total:', result.total)
        if (result.messages?.length) {
          chatRef.current.setHistory(result.messages as AgentMessageItem[], agentId, {
            total: result.total,
            offset: result.offset,
          })
          offsetRef.current = result.offset
        }
      })
      .catch(() => {})
      .finally(() => setIsLoadingHistory(false))

    return () => {
      window.electronAPI.localChat.offEvent()
      window.electronAPI.localChat.offApproval()
      window.electronAPI.hub.offInboundMessage()
      window.electronAPI.localChat.unsubscribe(agentId).catch(() => {})
    }
  }, [agentId])

  const sendMessage = useCallback(
    (text: string) => {
      const trimmed = text.trim()
      if (!trimmed || !agentId) return
      chatRef.current.addUserMessage(trimmed, agentId, { type: 'local' })
      chatRef.current.setError(null)
      window.electronAPI.localChat.send(agentId, trimmed).catch(() => {})
      setIsLoading(true)
    },
    [agentId],
  )

  const abortGeneration = useCallback(() => {
    if (!agentId) return
    window.electronAPI.localChat.abort(agentId).catch(() => {})
    setIsLoading(false)
  }, [agentId])

  const loadMore = useCallback(async () => {
    const currentOffset = offsetRef.current
    if (!agentId || currentOffset == null || currentOffset <= 0 || isLoadingMoreRef.current) return

    isLoadingMoreRef.current = true
    setIsLoadingMore(true)
    try {
      const newOffset = Math.max(0, currentOffset - DEFAULT_MESSAGES_LIMIT)
      const limit = currentOffset - newOffset
      const result = await window.electronAPI.localChat.getHistory(agentId, { offset: newOffset, limit })
      if (result.messages?.length) {
        chatRef.current.prependHistory(result.messages as AgentMessageItem[], agentId, {
          total: result.total,
          offset: result.offset,
        })
        offsetRef.current = result.offset
      }
    } catch {
      // Best-effort — pagination failure does not block chat
    } finally {
      isLoadingMoreRef.current = false
      setIsLoadingMore(false)
    }
  }, [agentId])

  const resolveApproval = useCallback(
    (approvalId: string, decision: ApprovalDecision) => {
      chatRef.current.removeApproval(approvalId)
      window.electronAPI.localChat.resolveExecApproval(approvalId, decision).catch(() => {})
    },
    [],
  )

  const clearError = useCallback(() => {
    chatRef.current.setError(null)
  }, [])

  return {
    agentId,
    initError,
    messages: chat.messages,
    streamingIds: chat.streamingIds,
    isLoading,
    isLoadingHistory,
    isLoadingMore,
    hasMore: chat.hasMore,
    error: chat.error,
    pendingApprovals: chat.pendingApprovals,
    sendMessage,
    abortGeneration,
    loadMore,
    resolveApproval,
    clearError,
  }
}
