# Dashboard Design Plan

## Overview

Build a runtime dashboard that shows "What is my Agent doing now, what will it do next, what did it do before" instead of "How is my Agent configured".

## Goals

1. Real-time visibility into Agent execution state
2. Monitor all input sources (Desktop IPC, Gateway, Channels)
3. Track sub-agents, processes, scheduled tasks
4. Provide control capabilities (cancel, retry)

---

## Phase 1: Core Infrastructure

### 1.1 Unified Event Stream

Create a dashboard subscription mechanism in Hub that exposes all observable data.

**File:** `packages/core/src/hub/dashboard.ts`

```typescript
interface DashboardEvent {
  timestamp: number;
  agentId: string;

  // Source tracking
  source: {
    type: "local" | "gateway" | "channel";
    deviceId?: string;           // Gateway source
    channel?: string;            // telegram | discord | slack
    accountId?: string;
    conversationId?: string;
  };

  // Original event
  event: AgentEvent | MulticaEvent;
}

interface DashboardSubscription {
  subscribe(callback: (event: DashboardEvent) => void): () => void;

  // Query methods
  getSnapshot(): DashboardSnapshot;
}
```

### 1.2 Dashboard Snapshot

Queryable state for initial load and polling fallback.

```typescript
interface DashboardSnapshot {
  // Agent state
  agents: Array<{
    id: string;
    isRunning: boolean;
    isStreaming: boolean;
    pendingWrites: number;
    lastError?: string;
  }>;

  // Sub-agents
  subagents: SubagentRunRecord[];

  // Processes
  processes: ProcessEntry[];

  // Heartbeat
  lastHeartbeat?: HeartbeatEventPayload;

  // Cron jobs
  cronJobs: CronJobStatus[];

  // Connections
  gateway: {
    connectionState: ConnectionState;
    connectedDevices: number;
  };
  channels: ChannelAccountState[];
}
```

---

## Phase 2: IPC Layer (Desktop)

### 2.1 New IPC Handlers

**File:** `apps/desktop/src/main/ipc/dashboard.ts`

```typescript
// Subscribe to dashboard events
ipcMain.handle('dashboard:subscribe', (agentId?: string) => {
  // Returns subscription ID
});

// Get current snapshot
ipcMain.handle('dashboard:snapshot', () => {
  return hub.getDashboardSnapshot();
});

// Unsubscribe
ipcMain.handle('dashboard:unsubscribe', (subscriptionId) => {});

// Event push to renderer
mainWindow.webContents.send('dashboard:event', event);
```

### 2.2 Preload API

**File:** `apps/desktop/src/preload/index.ts`

```typescript
dashboard: {
  subscribe: () => ipcRenderer.invoke('dashboard:subscribe'),
  unsubscribe: () => ipcRenderer.invoke('dashboard:unsubscribe'),
  getSnapshot: () => ipcRenderer.invoke('dashboard:snapshot'),
  onEvent: (callback) => ipcRenderer.on('dashboard:event', callback),
}
```

---

## Phase 3: Frontend Store

### 3.1 Dashboard Store

**File:** `apps/desktop/src/renderer/src/stores/dashboard.ts`

```typescript
interface DashboardState {
  // Real-time
  events: DashboardEvent[];        // Rolling buffer (last 100)
  currentRun: {
    agentId: string;
    streamId: string;
    messages: StreamingMessage[];
    tools: ToolExecution[];
  } | null;

  // Snapshot data
  subagents: SubagentRunRecord[];
  processes: ProcessEntry[];
  heartbeat: HeartbeatEventPayload | null;
  cronJobs: CronJobStatus[];

  // Connection status
  gateway: GatewayStatus;
  channels: ChannelAccountState[];

  // Actions
  subscribe: () => void;
  unsubscribe: () => void;
  refresh: () => void;
}
```

---

## Phase 4: UI Components

### 4.1 Dashboard Page

**File:** `apps/desktop/src/renderer/src/pages/dashboard.tsx`

Layout:
```
┌─────────────────────────────────────────────────────────┐
│  Dashboard                                    [Refresh] │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │ Agent Status    │  │ Current Activity            │  │
│  │ ● Running       │  │ 💬 Generating response...   │  │
│  │ Pending: 2      │  │ 🔧 exec: npm test           │  │
│  └─────────────────┘  └─────────────────────────────┘  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Sub-agents                                       │   │
│  │ ┌─────────────────────────────────────────────┐ │   │
│  │ │ 🟢 search-docs    task: "Find API docs"     │ │   │
│  │ │ 🔵 analyze-code   task: "Review PR #123"    │ │   │
│  │ │ 🟡 pending        task: "Write tests"       │ │   │
│  │ └─────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Running Processes                                │   │
│  │ ┌─────────────────────────────────────────────┐ │   │
│  │ │ npm test          PID: 12345   ⏱️ 2m 30s    │ │   │
│  │ │ > Running 45/100 tests...                   │ │   │
│  │ └─────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Heartbeat    │  │ Gateway      │  │ Channels     │  │
│  │ ✅ 30s ago   │  │ 🟢 2 devices │  │ 📱 Telegram  │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 4.2 Components

- `AgentStatusCard` - Running/Idle/Error state
- `CurrentActivityFeed` - Real-time event stream
- `SubagentList` - Sub-agent status with progress
- `ProcessList` - Running bash processes with output preview
- `HeartbeatIndicator` - Health status
- `ConnectionStatus` - Gateway + Channels

---

## Phase 5: Control Actions

### 5.1 Process Control

```typescript
// Stop a running process
ipcMain.handle('dashboard:stopProcess', (processId) => {
  return processRegistry.stop(processId);
});
```

### 5.2 Agent Control (Future)

- Cancel current run (requires AbortController wiring)
- Cancel sub-agent
- Retry failed operation

---

## Implementation Order

### Step 1: Core Infrastructure
- [ ] Create `packages/core/src/hub/dashboard.ts`
- [ ] Add `getDashboardSnapshot()` to Hub
- [ ] Add source tracking to event stream

### Step 2: IPC Layer
- [ ] Create `apps/desktop/src/main/ipc/dashboard.ts`
- [ ] Add preload API
- [ ] Wire up event forwarding

### Step 3: Store
- [ ] Create `useDashboardStore`
- [ ] Implement subscription lifecycle

### Step 4: UI
- [ ] Create dashboard page
- [ ] Build individual components
- [ ] Add to navigation

### Step 5: Polish
- [ ] Error handling
- [ ] Loading states
- [ ] Empty states

---

## Data Sources Summary

| Data | Source | Real-time | Query |
|------|--------|-----------|-------|
| Agent events | `agent.subscribe()` | ✅ Push | - |
| Sub-agents | `listSubagentRuns()` | 🔄 Poll | ✅ |
| Processes | `PROCESS_REGISTRY` | 🔄 Poll | ✅ |
| Heartbeat | `onHeartbeatEvent()` | ✅ Push | ✅ |
| Gateway | `onConnectionStateChange()` | ✅ Push | ✅ |
| Channels | `listAccountStates()` | 🔄 Poll | ✅ |

---

## Open Questions

1. **Event buffer size** - How many events to keep in memory?
2. **Polling interval** - For non-push data, how often to refresh?
3. **Sub-agent drill-down** - Can we subscribe to child agent events?
4. **Process output streaming** - Stream tail buffer in real-time?

---

## References

- Agent event types: `packages/core/src/agent/events.ts`
- Sub-agent registry: `packages/core/src/agent/subagent/registry.ts`
- Process registry: `packages/core/src/agent/tools/process-registry.ts`
- Heartbeat: `packages/core/src/heartbeat/`
- Channel manager: `packages/core/src/channels/manager.ts`
