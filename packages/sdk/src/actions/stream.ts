/** Stream Action - 流式消息传输 */

export const StreamAction = "stream" as const;

/**
 * AgentEvent types forwarded by the Hub to frontend clients.
 * These mirror the subset of AgentEvent from @mariozechner/pi-agent-core
 * that the Hub forwards (filtered at the Hub layer).
 */
export interface StreamMessageEvent {
  type: "message_start" | "message_update" | "message_end";
  message: {
    id?: string;
    role: string;
    content?: Array<{ type: string; text?: string }>;
  };
  assistantMessageEvent?: unknown;
}

export interface StreamToolEvent {
  type: "tool_execution_start" | "tool_execution_end";
  toolCallId: string;
  toolName: string;
  args?: unknown;
  result?: unknown;
  isError?: boolean;
}

export type StreamEvent = StreamMessageEvent | StreamToolEvent;

/** 流消息 payload — wraps a raw AgentEvent with stream/agent identifiers */
export interface StreamPayload {
  /** 流 ID，关联同一个流的所有消息 */
  streamId: string;
  /** 所属 agent ID */
  agentId: string;
  /** Raw agent event from the engine */
  event: StreamEvent;
}

/** Extract plain text from an AgentMessage content array */
export function extractTextFromEvent(event: StreamMessageEvent): string {
  const content = event.message?.content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((c) => c.type === "text")
    .map((c) => c.text ?? "")
    .join("");
}
