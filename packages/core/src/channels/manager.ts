/**
 * Channel Manager — bridges messaging channels to the Hub's agent.
 *
 * Design: One Hub, one Agent. Channels are just alternative input/output surfaces.
 * - Incoming: channel message → agent.write(text)  (same as desktop/gateway)
 * - Outgoing: agent reply → check lastRoute → forward to originating channel
 *
 * Uses "last route" pattern: whoever sent the last message gets the reply.
 *
 * @see docs/channels/README.md — Channel system overview
 * @see docs/channels/media-handling.md — Media processing pipeline
 * @see docs/message-paths.md — All three message paths (Desktop / Web / Channel)
 */

import type { Hub } from "../hub/hub.js";
import type {
  ChannelPlugin,
  ChannelMessage,
  ChannelAccountState,
  DeliveryContext,
} from "./types.js";
import { listChannels } from "./registry.js";
import { loadChannelsConfig } from "./config.js";
import { MessageAggregator, DEFAULT_CHUNKER_CONFIG } from "../hub/message-aggregator.js";
import { isHeartbeatAckEvent } from "../hub/heartbeat-filter.js";
import type { AsyncAgent } from "../agent/async-agent.js";
import { transcribeAudio } from "../media/transcribe.js";
import { describeImage } from "../media/describe-image.js";
import { describeVideo } from "../media/describe-video.js";
import { InboundDebouncer } from "./inbound-debouncer.js";

interface AccountHandle {
  channelId: string;
  accountId: string;
  abortController: AbortController;
  state: ChannelAccountState;
}

/** Tracks where the last message came from, so replies go back there. */
interface LastRoute {
  plugin: ChannelPlugin;
  deliveryCtx: DeliveryContext;
}

export class ChannelManager {
  private readonly hub: Hub;
  /** Running accounts keyed by "channelId:accountId" */
  private readonly accounts = new Map<string, AccountHandle>();
  /** Where the last channel message came from (used for typing/reactions/errors) */
  private lastRoute: LastRoute | null = null;
  /**
   * FIFO queue of route snapshots + their ack targets, captured at each debouncer flush.
   * Each agent.write() gets its own entry; dequeued on agent_start.
   */
  private pendingRoutes: { route: LastRoute; acks: LastRoute[] }[] = [];
  /** Route for the currently active agent run (set on agent_start, cleared on agent_end). */
  private activeRoute: LastRoute | null = null;
  /** All messages in the current run's batch that have 👀 (cleared on agent_end). */
  private activeAcks: LastRoute[] = [];
  /** Accumulates message routes for 👀 removal between debouncer flushes. */
  private ackBuffer: LastRoute[] = [];
  /** Unsubscribe function for the agent subscriber */
  private agentUnsubscribe: (() => void) | null = null;
  /** Session ID of the currently subscribed agent (for stale detection) */
  private subscribedAgentId: string | null = null;
  /** Current aggregator for buffering streaming responses */
  private aggregator: MessageAggregator | null = null;
  /** Typing indicator interval (repeats every 5s to keep Telegram typing visible) */
  private typingTimer: ReturnType<typeof setInterval> | null = null;
  /**
   * Inbound message debouncer — batches rapid-fire messages from the same
   * conversation into a single agent.write() call.
   * Initialized lazily on first message; uses the current agent reference.
   */
  private debouncer: InboundDebouncer | null = null;

  constructor(hub: Hub) {
    this.hub = hub;
  }

  /** Start all configured channel accounts */
  async startAll(): Promise<void> {
    console.log("[Channels] Starting all channels...");
    const config = loadChannelsConfig();
    const plugins = listChannels();

    if (plugins.length === 0) {
      console.log("[Channels] No plugins registered");
      return;
    }

    for (const plugin of plugins) {
      const accountIds = plugin.config.listAccountIds(config);
      if (accountIds.length === 0) {
        console.log(`[Channels] Skipping ${plugin.id} (not configured)`);
        continue;
      }

      for (const accountId of accountIds) {
        const account = plugin.config.resolveAccount(config, accountId);
        if (!account || !plugin.config.isConfigured(account)) {
          console.log(`[Channels] Skipping ${plugin.id}:${accountId} (incomplete config)`);
          continue;
        }
        await this.startAccount(plugin.id, accountId, account);
      }
    }

    // Try to subscribe eagerly; if no agent yet, routeIncoming will retry lazily
    this.ensureSubscribed();
  }

  /**
   * Start a specific channel account.
   * Public so the desktop IPC layer can call it after saving config.
   */
  async startAccount(
    channelId: string,
    accountId: string,
    accountConfig: Record<string, unknown>,
  ): Promise<void> {
    const key = `${channelId}:${accountId}`;
    if (this.accounts.has(key)) {
      console.warn(`[Channels] ${key} is already running`);
      return;
    }

    const plugin = listChannels().find((p) => p.id === channelId);
    if (!plugin) {
      console.error(`[Channels] Plugin "${channelId}" not found`);
      return;
    }

    const abortController = new AbortController();
    const handle: AccountHandle = {
      channelId,
      accountId,
      abortController,
      state: { channelId, accountId, status: "starting" },
    };
    this.accounts.set(key, handle);

    console.log(`[Channels] Starting ${key}`);

    try {
      const startPromise = plugin.gateway.start(
        accountId,
        accountConfig,
        (message: ChannelMessage) => {
          this.routeIncoming(plugin, accountId, message);
        },
        abortController.signal,
      );

      await Promise.race([
        startPromise,
        new Promise<void>((resolve) => setTimeout(resolve, 3000)),
      ]);

      handle.state = { channelId, accountId, status: "running" };
      console.log(`[Channels] ${key} is running`);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      handle.state = { channelId, accountId, status: "error", error: errorMsg };
      console.error(`[Channels] Failed to start ${key}: ${errorMsg}`);
    }
  }

  /** Get the Hub's current agent (the first active one) */
  private getHubAgent(): AsyncAgent | undefined {
    const agentIds = this.hub.listAgents();
    if (agentIds.length === 0) {
      console.warn("[Channels] No agent available in Hub");
      return undefined;
    }
    const agent = this.hub.getAgent(agentIds[0]!);
    return agent;
  }

  /**
   * Ensure we're subscribed to the current Hub agent for outbound routing.
   * Lazily called from routeIncoming — handles agent not yet available at
   * startup and re-subscribes if the agent has changed.
   */
  private ensureSubscribed(): void {
    const agent = this.getHubAgent();
    if (!agent) return;

    // Already subscribed to the current agent
    if (this.subscribedAgentId === agent.sessionId) return;

    // Unsubscribe from stale agent
    if (this.agentUnsubscribe) {
      console.log(`[Channels] Agent changed, re-subscribing (${this.subscribedAgentId} → ${agent.sessionId})`);
      this.agentUnsubscribe();
    }

    console.log(`[Channels] Subscribing to agent ${agent.sessionId} for outbound routing`);
    this.subscribedAgentId = agent.sessionId;

    this.agentUnsubscribe = agent.subscribe((event) => {
      const maybeMessage = (event as { message?: { role?: string } }).message;
      const role = maybeMessage?.role;

      // Activate the next pending route + acks when a new agent run starts.
      if (event.type === "agent_start") {
        const entry = this.pendingRoutes.shift();
        if (entry) {
          this.activeRoute = entry.route;
          this.activeAcks = entry.acks;
          console.log(`[Channels] agent_start: activeRoute replyTo=${entry.route.deliveryCtx.replyToMessageId}, acks=${entry.acks.length}`);
        }
      }

      // Agent run complete — remove 👀 from all batch messages, conditionally stop typing.
      if (event.type === "agent_end") {
        for (const ack of this.activeAcks) {
          if (ack.plugin.outbound.removeReaction) {
            console.log(`[Channels] agent_end: removing 👀 from replyTo=${ack.deliveryCtx.replyToMessageId}`);
            void ack.plugin.outbound.removeReaction(ack.deliveryCtx).catch(() => {});
          }
        }
        this.activeRoute = null;
        this.activeAcks = [];
        if (this.pendingRoutes.length === 0) {
          console.log("[Channels] agent_end: no more pending, stopping typing");
          this.stopTyping();
        } else {
          console.log(`[Channels] agent_end: ${this.pendingRoutes.length} pending run(s), keeping typing`);
        }
      }

      // No active channel route — skip (reply goes to desktop/gateway only)
      if (!this.lastRoute) return;

      // Handle agent errors — notify the channel user
      if (event.type === "agent_error") {
        this.stopTyping();
        for (const ack of this.activeAcks) {
          if (ack.plugin.outbound.removeReaction) {
            void ack.plugin.outbound.removeReaction(ack.deliveryCtx).catch(() => {});
          }
        }
        this.activeRoute = null;
        this.activeAcks = [];
        const errorMsg = (event as { message?: string }).message ?? "Unknown error";
        console.error(`[Channels] Agent error: ${errorMsg}`);
        const route = this.lastRoute;
        if (route) {
          void route.plugin.outbound.sendText(route.deliveryCtx, `[Error] ${errorMsg}`).catch((err) => {
            console.error(`[Channels] Failed to send error to channel: ${err}`);
          });
        }
        return;
      }

      // Only forward assistant message events
      if (event.type === "message_start" || event.type === "message_update" || event.type === "message_end") {
        if (role !== "assistant") return;
      } else {
        // Non-message events (tool_execution etc.) — skip for channels
        return;
      }

      // Keep heartbeat acknowledgements internal (same behavior as desktop/gateway stream path).
      if (isHeartbeatAckEvent(event)) {
        if (event.type === "message_end") {
          this.aggregator = null;
        }
        return;
      }

      // Ensure aggregator exists for this response
      if (event.type === "message_start") {
        this.createAggregator();
      }

      if (this.aggregator) {
        this.aggregator.handleEvent(event);
      }

      // Finalize aggregator per assistant message (may fire multiple times in multi-turn runs).
      // Typing and ack removal are handled at agent_end, not here.
      if (event.type === "message_end" && role === "assistant") {
        this.aggregator = null;
      }
    });
  }

  /**
   * Create a fresh aggregator wired to the activeRoute (snapshotted at flush time).
   * Falls back to lastRoute for non-debounced paths (e.g. direct writes).
   */
  private createAggregator(): void {
    const route = this.activeRoute ?? this.lastRoute;
    if (!route) return;

    const { plugin, deliveryCtx } = route;
    console.log(`[Channels] createAggregator: replyTo=${deliveryCtx.replyToMessageId} (source=${this.activeRoute ? "activeRoute" : "lastRoute"})`);
    const chunkerConfig = plugin.chunkerConfig ?? DEFAULT_CHUNKER_CONFIG;

    this.aggregator = new MessageAggregator(
      chunkerConfig,
      async (block) => {
        try {
          console.log(`[Channels] Sending block ${block.index} (${block.text.length} chars${block.isFinal ? ", final" : ""}) → ${deliveryCtx.channel}:${deliveryCtx.conversationId} replyTo=${deliveryCtx.replyToMessageId}`);
          if (block.index === 0) {
            await plugin.outbound.replyText(deliveryCtx, block.text);
          } else {
            await plugin.outbound.sendText(deliveryCtx, block.text);
          }
        } catch (err) {
          console.error(`[Channels] Failed to send reply: ${err}`);
        }
      },
      () => {},
    );
  }

  /**
   * Incoming channel message → update lastRoute → forward to Hub's agent.
   */
  private routeIncoming(
    plugin: ChannelPlugin,
    accountId: string,
    message: ChannelMessage,
  ): void {
    const { conversationId, senderId, text, messageId } = message;
    console.log(
      `[Channels] Incoming: channel=${plugin.id} conv=${conversationId} sender=${senderId} text="${text.slice(0, 50)}${text.length > 50 ? "..." : ""}"`,
    );

    const agent = this.getHubAgent();
    if (!agent) {
      console.error("[Channels] No agent available, dropping message");
      return;
    }

    // Ensure we're subscribed to this agent (handles late startup / agent change)
    this.ensureSubscribed();

    // Update last route — replies will go back here
    this.lastRoute = {
      plugin,
      deliveryCtx: {
        channel: plugin.id,
        accountId,
        conversationId,
        replyToMessageId: messageId,
      },
    };
    console.log(`[Channels] lastRoute updated → ${plugin.id}:${conversationId} replyTo=${messageId}`);
    console.log(`[Channels] Forwarding to agent ${agent.sessionId}`);

    // Show typing indicator and 👀 ack on this message
    this.startTyping();
    const ackRoute: LastRoute = { ...this.lastRoute };
    if (ackRoute.plugin.outbound.addReaction) {
      console.log(`[Channels] Adding 👀 to replyTo=${messageId}`);
      void ackRoute.plugin.outbound.addReaction(ackRoute.deliveryCtx, "👀").catch(() => {});
    }
    this.ackBuffer.push(ackRoute);

    // Handle media messages (processed async, then fed through debouncer)
    if (message.media && plugin.downloadMedia) {
      void this.routeMedia(plugin, accountId, message, agent);
    } else {
      // Text messages go through debouncer to batch rapid-fire sends
      this.getDebouncer(agent).push(conversationId, text);
    }
  }

  /**
   * Download media file, process it (transcribe/describe), and forward
   * the resulting text through the debouncer to the agent.
   * Media results are also debounced so that a rapid "photo + text" combo
   * from the same conversation gets batched into one agent prompt.
   */
  private async routeMedia(
    plugin: ChannelPlugin,
    accountId: string,
    message: ChannelMessage,
    agent: AsyncAgent,
  ): Promise<void> {
    const media = message.media!;
    const debouncer = this.getDebouncer(agent);

    try {
      const filePath = await plugin.downloadMedia!(media.fileId, accountId);

      if (media.type === "image") {
        // Images: describe via Vision API before reaching agent
        const description = await describeImage(filePath);
        if (description) {
          const parts = ["[Image]", `Description: ${description}`];
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        } else {
          // No API key — fall back to file path
          const parts = ["[image message received]", `File: ${filePath}`];
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        }
      } else if (media.type === "audio") {
        // Audio: transcribe via Whisper API before reaching agent
        const transcript = await transcribeAudio(filePath);
        if (transcript) {
          const parts = ["[Voice Message]", `Transcript: ${transcript}`];
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        } else {
          // No API key configured — fall back to file path
          const parts = ["[audio message received]", `File: ${filePath}`];
          if (media.mimeType) parts.push(`Type: ${media.mimeType}`);
          if (media.duration) parts.push(`Duration: ${media.duration}s`);
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        }
      } else if (media.type === "video") {
        // Video: extract frame + describe via Vision API
        const description = await describeVideo(filePath);
        if (description) {
          const parts = ["[Video]", `Description: ${description}`];
          if (media.duration) parts.push(`Duration: ${media.duration}s`);
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        } else {
          // ffmpeg unavailable or no API key — fall back to file path
          const parts = ["[video message received]", `File: ${filePath}`];
          if (media.mimeType) parts.push(`Type: ${media.mimeType}`);
          if (media.duration) parts.push(`Duration: ${media.duration}s`);
          if (media.caption) parts.push(`Caption: ${media.caption}`);
          debouncer.push(message.conversationId, parts.join("\n"));
        }
      } else {
        // Document: tell agent the file path
        const parts: string[] = [];
        parts.push(`[document message received]`);
        parts.push(`File: ${filePath}`);
        if (media.mimeType) parts.push(`Type: ${media.mimeType}`);
        if (media.caption) parts.push(`Caption: ${media.caption}`);
        debouncer.push(message.conversationId, parts.join("\n"));
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[Channels] Failed to process media: ${msg}`);
      debouncer.push(message.conversationId, message.text || `[Failed to process ${media.type}]`);
    }
  }

  /**
   * Get or create the inbound debouncer, wired to the given agent.
   * The debouncer batches rapid-fire messages by conversationId, then
   * calls agent.write() once with the combined text.
   */
  private getDebouncer(agent: AsyncAgent): InboundDebouncer {
    if (!this.debouncer) {
      this.debouncer = new InboundDebouncer(
        (_conversationId, combinedText) => {
          // Snapshot the current route + pending acks for this batch.
          const route = this.lastRoute ? { ...this.lastRoute } : null;
          const acks = [...this.ackBuffer];
          this.ackBuffer = [];
          const source = route ? {
            type: "channel" as const,
            channelId: route.plugin.id,
            accountId: route.deliveryCtx.accountId,
            conversationId: route.deliveryCtx.conversationId,
          } : undefined;
          if (route) {
            this.pendingRoutes.push({ route, acks });
            // Broadcast inbound message to local listeners (Desktop UI)
            this.hub.broadcastInbound({
              agentId: agent.sessionId,
              content: combinedText,
              source: source!,
              timestamp: Date.now(),
            });
          }
          const replyTo = route?.deliveryCtx.replyToMessageId ?? "?";
          console.log(`[Channels] Debouncer flushing ${combinedText.length} chars to agent (queued route replyTo=${replyTo}, acks=${acks.length})`);
          agent.write(combinedText, { source });
        },
      );
    }
    return this.debouncer;
  }

  /** Start sending typing indicators (repeats every 5s until stopped) */
  private startTyping(): void {
    this.stopTyping();
    const route = this.lastRoute;
    if (!route?.plugin.outbound.sendTyping) return;

    const send = () => route.plugin.outbound.sendTyping!(route.deliveryCtx).catch(() => {});
    void send();
    this.typingTimer = setInterval(send, 5000);
  }

  /** Stop typing indicator interval */
  private stopTyping(): void {
    if (this.typingTimer) {
      clearInterval(this.typingTimer);
      this.typingTimer = null;
    }
  }

  /**
   * Stop a specific channel account.
   * Public so the desktop IPC layer can call it when removing config.
   * Cleans up typing timer, debouncer, aggregator, and lastRoute if they
   * belong to this account.
   */
  stopAccount(channelId: string, accountId: string): void {
    const key = `${channelId}:${accountId}`;
    const handle = this.accounts.get(key);
    if (!handle) return;

    // Clean up shared resources if they target this account
    if (this.lastRoute && this.lastRoute.plugin.id === channelId && this.lastRoute.deliveryCtx.accountId === accountId) {
      this.stopTyping();
      this.lastRoute = null;
      this.activeRoute = null;
      this.activeAcks = [];
      this.ackBuffer = [];
      this.pendingRoutes = [];
      this.aggregator = null;
    }

    handle.abortController.abort();
    handle.state = { ...handle.state, status: "stopped" };
    this.accounts.delete(key);

    // Dispose debouncer if no accounts remain
    if (this.accounts.size === 0 && this.debouncer) {
      this.debouncer.dispose();
      this.debouncer = null;
    }

    console.log(`[Channels] Stopped ${key}`);
  }

  /** Stop all running channel accounts */
  stopAll(): void {
    console.log("[Channels] Stopping all channels...");
    this.stopTyping();
    this.debouncer?.dispose();
    this.debouncer = null;
    if (this.agentUnsubscribe) {
      this.agentUnsubscribe();
      this.agentUnsubscribe = null;
    }
    for (const [key, handle] of this.accounts) {
      handle.abortController.abort();
      handle.state = { ...handle.state, status: "stopped" };
      console.log(`[Channels] Stopped ${key}`);
    }
    this.accounts.clear();
    this.lastRoute = null;
    this.activeRoute = null;
    this.activeAcks = [];
    this.ackBuffer = [];
    this.pendingRoutes = [];
    this.aggregator = null;
  }

  /** Clear the last route (e.g. when desktop user sends a message) */
  clearLastRoute(): void {
    if (this.lastRoute) {
      this.stopTyping();
      console.log("[Channels] lastRoute cleared (non-channel message received)");
      this.lastRoute = null;
    }
  }

  /** Get status of all accounts */
  listAccountStates(): ChannelAccountState[] {
    return Array.from(this.accounts.values()).map((h) => ({ ...h.state }));
  }
}
