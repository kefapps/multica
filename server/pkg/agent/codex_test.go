package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestCodexClient(t *testing.T) (*codexClient, *fakeStdin, []Message) {
	t.Helper()
	fs := &fakeStdin{}
	var mu sync.Mutex
	var messages []Message

	c := &codexClient{
		cfg:     Config{Logger: slog.Default()},
		stdin:   fs,
		pending: make(map[int]*pendingRPC),
		onMessage: func(msg Message) {
			mu.Lock()
			messages = append(messages, msg)
			mu.Unlock()
		},
		onTurnDone: func(aborted bool) {},
	}
	return c, fs, messages
}

type fakeStdin struct {
	mu   sync.Mutex
	data []byte
}

func (f *fakeStdin) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *fakeStdin) Lines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var lines []string
	for _, line := range splitLines(string(f.data)) {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func TestCodexHandleResponseSuccess(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)

	// Register a pending request
	pr := &pendingRPC{ch: make(chan rpcResult, 1), method: "test"}
	c.mu.Lock()
	c.pending[1] = pr
	c.mu.Unlock()

	c.handleLine(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`)

	res := <-pr.ch
	if res.err != nil {
		t.Fatalf("expected no error, got %v", res.err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(res.result, &parsed); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if parsed["ok"] != true {
		t.Fatalf("expected ok=true, got %v", parsed["ok"])
	}
}

func TestCodexHandleResponseError(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)

	pr := &pendingRPC{ch: make(chan rpcResult, 1), method: "test"}
	c.mu.Lock()
	c.pending[1] = pr
	c.mu.Unlock()

	c.handleLine(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"bad request"}}`)

	res := <-pr.ch
	if res.err == nil {
		t.Fatal("expected error")
	}
	if res.result != nil {
		t.Fatalf("expected nil result, got %v", res.result)
	}
}

func TestCodexHandleServerRequestAutoApproves(t *testing.T) {
	t.Parallel()

	c, fs, _ := newTestCodexClient(t)

	// Command execution approval
	c.handleLine(`{"jsonrpc":"2.0","id":10,"method":"item/commandExecution/requestApproval","params":{}}`)

	lines := fs.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 response, got %d", len(lines))
	}

	var resp map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["id"] != float64(10) {
		t.Fatalf("expected id=10, got %v", resp["id"])
	}
	result := resp["result"].(map[string]any)
	if result["decision"] != "accept" {
		t.Fatalf("expected decision=accept, got %v", result["decision"])
	}
}

func TestCodexHandleServerRequestFileChangeApproval(t *testing.T) {
	t.Parallel()

	c, fs, _ := newTestCodexClient(t)

	c.handleLine(`{"jsonrpc":"2.0","id":11,"method":"applyPatchApproval","params":{}}`)

	lines := fs.Lines()
	if len(lines) != 1 {
		t.Fatalf("expected 1 response, got %d", len(lines))
	}

	var resp map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	result := resp["result"].(map[string]any)
	if result["decision"] != "accept" {
		t.Fatalf("expected decision=accept, got %v", result["decision"])
	}
}

func TestCodexLegacyEventTaskStarted(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	var gotStatus bool
	c.onMessage = func(msg Message) {
		if msg.Type == MessageStatus && msg.Status == "running" {
			gotStatus = true
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"task_started"}}}`)

	if !gotStatus {
		t.Fatal("expected status=running message")
	}
	if !c.turnStarted {
		t.Fatal("expected turnStarted=true")
	}
	if c.notificationProtocol != "legacy" {
		t.Fatalf("expected protocol=legacy, got %q", c.notificationProtocol)
	}
}

func TestCodexLegacyEventAgentMessage(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	var gotText string
	c.onMessage = func(msg Message) {
		if msg.Type == MessageText {
			gotText = msg.Content
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"agent_message","message":"I found the bug"}}}`)

	if gotText != "I found the bug" {
		t.Fatalf("expected text 'I found the bug', got %q", gotText)
	}
}

func TestCodexLegacyEventExecCommand(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	var messages []Message
	c.onMessage = func(msg Message) {
		messages = append(messages, msg)
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"exec_command_begin","call_id":"c1","command":"ls -la"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"exec_command_end","call_id":"c1","output":"total 42"}}}`)

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].Type != MessageToolUse || messages[0].Tool != "exec_command" || messages[0].CallID != "c1" {
		t.Fatalf("unexpected begin message: %+v", messages[0])
	}
	if messages[1].Type != MessageToolResult || messages[1].CallID != "c1" || messages[1].Output != "total 42" {
		t.Fatalf("unexpected end message: %+v", messages[1])
	}
}

func TestCodexLegacyEventTaskComplete(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	var done bool
	c.onTurnDone = func(aborted bool) {
		done = true
		if aborted {
			t.Fatal("expected aborted=false")
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"task_complete"}}}`)

	if !done {
		t.Fatal("expected onTurnDone to be called")
	}
}

func TestCodexLegacyEventTurnAborted(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	var abortedResult bool
	c.onTurnDone = func(aborted bool) {
		abortedResult = aborted
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"turn_aborted"}}}`)

	if !abortedResult {
		t.Fatal("expected aborted=true")
	}
}

func TestCodexRawTurnStarted(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	// The zero value "" doesn't match "unknown", so protocol auto-detection
	// won't trigger. Set it explicitly as production code would.
	c.notificationProtocol = "unknown"

	var gotStatus bool
	c.onMessage = func(msg Message) {
		if msg.Type == MessageStatus && msg.Status == "running" {
			gotStatus = true
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}`)

	if !gotStatus {
		t.Fatal("expected status=running message")
	}
	if c.notificationProtocol != "raw" {
		t.Fatalf("expected protocol=raw, got %q", c.notificationProtocol)
	}
	if c.turnID != "turn-1" {
		t.Fatalf("expected turnID=turn-1, got %q", c.turnID)
	}
}

func TestCodexRawTurnCompleted(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var doneCount int
	c.onTurnDone = func(aborted bool) {
		doneCount++
		if aborted {
			t.Fatal("expected aborted=false")
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}`)

	if doneCount != 1 {
		t.Fatalf("expected onTurnDone called once, got %d", doneCount)
	}
}

func TestCodexRawTurnCompletedDeduplication(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var doneCount int
	c.onTurnDone = func(aborted bool) {
		doneCount++
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}`)

	if doneCount != 1 {
		t.Fatalf("expected deduplication, but onTurnDone called %d times", doneCount)
	}
}

func TestCodexRawTurnCompletedAborted(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var wasAborted bool
	c.onTurnDone = func(aborted bool) {
		wasAborted = aborted
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"turn":{"id":"turn-2","status":"cancelled"}}}`)

	if !wasAborted {
		t.Fatal("expected aborted=true for cancelled status")
	}
}

func TestCodexRawItemCommandExecution(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var messages []Message
	c.onMessage = func(msg Message) {
		messages = append(messages, msg)
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"type":"commandExecution","id":"item-1","command":"git status"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"commandExecution","id":"item-1","aggregatedOutput":"on branch main"}}}`)

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].Type != MessageToolUse || messages[0].Tool != "exec_command" || messages[0].Input["command"] != "git status" {
		t.Fatalf("unexpected start message: %+v", messages[0])
	}
	if messages[1].Type != MessageToolResult || messages[1].Output != "on branch main" {
		t.Fatalf("unexpected complete message: %+v", messages[1])
	}
}

func TestCodexRawItemAgentMessageFinalAnswer(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"
	c.turnStarted = true

	var gotText string
	var turnDone bool
	c.onMessage = func(msg Message) {
		if msg.Type == MessageText {
			gotText = msg.Content
		}
	}
	c.onTurnDone = func(aborted bool) {
		turnDone = true
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-1","text":"Done!","phase":"final_answer"}}}`)

	if gotText != "Done!" {
		t.Fatalf("expected text 'Done!', got %q", gotText)
	}
	if !turnDone {
		t.Fatal("expected onTurnDone for final_answer")
	}
}

func TestCodexRawItemAgentMessageDeltaSuppressesCompletedDuplicate(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"
	c.turnStarted = true

	var texts []string
	var turnDone bool
	c.onMessage = func(msg Message) {
		if msg.Type == MessageText {
			texts = append(texts, msg.Content)
		}
	}
	c.onTurnDone = func(aborted bool) {
		if aborted {
			t.Fatal("expected aborted=false")
		}
		turnDone = true
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"type":"agentMessage","id":"msg-1","text":"","phase":"final_answer"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"msg-1","delta":"Hel"}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"msg-1","delta":"lo"}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-1","text":"Hello","phase":"final_answer"}}}`)

	if got := strings.Join(texts, ""); got != "Hello" {
		t.Fatalf("expected streamed text to reconstruct Hello, got %q", got)
	}
	if len(texts) != 2 {
		t.Fatalf("expected exactly 2 delta messages, got %d (%#v)", len(texts), texts)
	}
	if !turnDone {
		t.Fatal("expected onTurnDone for final_answer completion")
	}
}

func TestCodexRawItemAgentMessageCompletedWithoutDeltaEmitsText(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var texts []string
	c.onMessage = func(msg Message) {
		if msg.Type == MessageText {
			texts = append(texts, msg.Content)
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-1","text":"Hello","phase":"commentary"}}}`)

	if got := strings.Join(texts, ""); got != "Hello" {
		t.Fatalf("expected completed message text, got %q", got)
	}
	if len(texts) != 1 {
		t.Fatalf("expected a single text message, got %d", len(texts))
	}
}

func TestCodexRawCommandExecutionOutputDeltaSuppressesCompletedDuplicate(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var messages []Message
	c.onMessage = func(msg Message) {
		messages = append(messages, msg)
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"type":"commandExecution","id":"call-1","command":"git status"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/commandExecution/outputDelta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"call-1","delta":"on branch "}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/commandExecution/outputDelta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"call-1","delta":"main\n"}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"commandExecution","id":"call-1","aggregatedOutput":"on branch main\n"}}}`)

	if len(messages) != 3 {
		t.Fatalf("expected 3 messages, got %d (%#v)", len(messages), messages)
	}
	if messages[0].Type != MessageToolUse || messages[0].Tool != "exec_command" || messages[0].CallID != "call-1" {
		t.Fatalf("unexpected tool use message: %+v", messages[0])
	}
	if messages[1].Type != MessageToolResult || messages[1].CallID != "call-1" || messages[1].Output != "on branch " {
		t.Fatalf("unexpected first delta message: %+v", messages[1])
	}
	if messages[2].Type != MessageToolResult || messages[2].CallID != "call-1" || messages[2].Output != "main\n" {
		t.Fatalf("unexpected second delta message: %+v", messages[2])
	}
}

func TestCodexRawCommandExecutionCompletedWithoutDeltaEmitsOutput(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"

	var messages []Message
	c.onMessage = func(msg Message) {
		messages = append(messages, msg)
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"item/completed","params":{"item":{"type":"commandExecution","id":"call-1","aggregatedOutput":"done\n"}}}`)

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d (%#v)", len(messages), messages)
	}
	if messages[0].Type != MessageToolResult || messages[0].CallID != "call-1" || messages[0].Output != "done\n" {
		t.Fatalf("unexpected tool result message: %+v", messages[0])
	}
}

func TestCodexRawThreadStatusIdle(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "raw"
	c.turnStarted = true

	var turnDone bool
	c.onTurnDone = func(aborted bool) {
		turnDone = true
		if aborted {
			t.Fatal("expected aborted=false for idle")
		}
	}

	c.handleLine(`{"jsonrpc":"2.0","method":"thread/status/changed","params":{"status":{"type":"idle"}}}`)

	if !turnDone {
		t.Fatal("expected onTurnDone for idle status")
	}
}

func TestCodexCloseAllPending(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)

	pr1 := &pendingRPC{ch: make(chan rpcResult, 1), method: "m1"}
	pr2 := &pendingRPC{ch: make(chan rpcResult, 1), method: "m2"}
	c.mu.Lock()
	c.pending[1] = pr1
	c.pending[2] = pr2
	c.mu.Unlock()

	c.closeAllPending(fmt.Errorf("test error"))

	r1 := <-pr1.ch
	if r1.err == nil {
		t.Fatal("expected error for pending 1")
	}
	r2 := <-pr2.ch
	if r2.err == nil {
		t.Fatal("expected error for pending 2")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) != 0 {
		t.Fatalf("expected empty pending map, got %d", len(c.pending))
	}
}

func TestCodexHandleInvalidJSON(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	// Should not panic
	c.handleLine("not json at all")
	c.handleLine("")
	c.handleLine("{}")
}

func TestExtractThreadID(t *testing.T) {
	t.Parallel()

	data := json.RawMessage(`{"thread":{"id":"t-123"}}`)
	got := extractThreadID(data)
	if got != "t-123" {
		t.Fatalf("expected t-123, got %q", got)
	}
}

func TestExtractThreadIDMissing(t *testing.T) {
	t.Parallel()

	got := extractThreadID(json.RawMessage(`{}`))
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestExtractNestedString(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"a": map[string]any{
			"b": "value",
		},
	}
	got := extractNestedString(m, "a", "b")
	if got != "value" {
		t.Fatalf("expected 'value', got %q", got)
	}
}

func TestExtractNestedStringMissingKey(t *testing.T) {
	t.Parallel()

	m := map[string]any{"a": "flat"}
	got := extractNestedString(m, "a", "b")
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestNilIfEmpty(t *testing.T) {
	t.Parallel()

	if nilIfEmpty("") != nil {
		t.Fatal("expected nil for empty string")
	}
	if nilIfEmpty("hello") != "hello" {
		t.Fatal("expected 'hello'")
	}
}

func TestCodexProtocolDetectionLegacyBlocksRaw(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)

	var messages []Message
	c.onMessage = func(msg Message) {
		messages = append(messages, msg)
	}

	// First: receive a legacy event -> locks to "legacy"
	c.handleLine(`{"jsonrpc":"2.0","method":"codex/event","params":{"msg":{"type":"task_started"}}}`)

	if c.notificationProtocol != "legacy" {
		t.Fatalf("expected legacy, got %q", c.notificationProtocol)
	}

	// Now send a raw notification -> should be ignored
	messagesBefore := len(messages)
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}`)

	if len(messages) != messagesBefore {
		t.Fatal("raw notification should be ignored in legacy mode")
	}
}

func TestCodexExecuteCompletesAfterSilentIdle(t *testing.T) {
	t.Helper()

	oldGrace := codexSilentTurnGracePeriod
	oldPoll := codexSilentTurnPollInterval
	oldShutdown := codexShutdownGracePeriod
	oldForced := codexForcedShutdownWait
	codexSilentTurnGracePeriod = 150 * time.Millisecond
	codexSilentTurnPollInterval = 25 * time.Millisecond
	codexShutdownGracePeriod = 250 * time.Millisecond
	codexForcedShutdownWait = 100 * time.Millisecond
	t.Cleanup(func() {
		codexSilentTurnGracePeriod = oldGrace
		codexSilentTurnPollInterval = oldPoll
		codexShutdownGracePeriod = oldShutdown
		codexForcedShutdownWait = oldForced
	})

	tmpDir := t.TempDir()
	execPath := filepath.Join(tmpDir, "codex")
	script := `#!/usr/bin/env python3
import json
import sys

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"capabilities": {}}})
    elif method == "thread/start":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"thread": {"id": "thread-1"}}})
    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"ok": True}})
        send({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn-1"}}})
        send({"jsonrpc": "2.0", "method": "item/completed", "params": {"item": {"type": "agentMessage", "id": "msg-1", "text": "hello from fake codex"}}})
`
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake codex executable: %v", err)
	}

	backend := &codexBackend{
		cfg: Config{
			ExecutablePath: execPath,
			Logger:         slog.Default(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := backend.Execute(ctx, "diagnose the issue", ExecOptions{
		Cwd:     tmpDir,
		Timeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var sawMessage bool
	for msg := range session.Messages {
		if msg.Type == MessageText && msg.Content == "hello from fake codex" {
			sawMessage = true
		}
	}

	result := <-session.Result
	if result.Status != "completed" {
		t.Fatalf("expected completed status, got %q (error=%q)", result.Status, result.Error)
	}
	if result.Error != "" {
		t.Fatalf("expected no final error, got %q", result.Error)
	}
	if result.Output != "hello from fake codex" {
		t.Fatalf("expected captured output, got %q", result.Output)
	}
	if !sawMessage {
		t.Fatal("expected streamed message before idle completion")
	}
	if len(result.Diagnostics) == 0 {
		t.Fatal("expected diagnostics snapshot to be attached")
	}
	if artifactPath, _ := result.Diagnostics["diagnostic_artifact_path"].(string); artifactPath != "" {
		t.Fatalf("did not expect diagnostic artifact for healthy run, got %q", artifactPath)
	}
}

func TestCodexDiagnosticsCaptureUnhandledNotification(t *testing.T) {
	t.Parallel()

	c, _, _ := newTestCodexClient(t)
	c.notificationProtocol = "unknown"
	c.diagnostics = newCodexDiagnostics(time.Unix(0, 0))

	c.handleLine(`{"jsonrpc":"2.0","method":"turn/progress","params":{"pct":10}}`)

	snapshot := c.diagnostics.snapshot(c.notificationProtocol, c.turnStarted, c.turnID, len(c.completedTurnIDs))
	if got := snapshot["notification_count"]; got != 1 {
		t.Fatalf("expected notification_count=1, got %v", got)
	}
	if got := snapshot["unhandled_notification_count"]; got != 1 {
		t.Fatalf("expected unhandled_notification_count=1, got %v", got)
	}
	events, _ := snapshot["recent_unhandled_events"].([]string)
	if len(events) != 1 || events[0] != "notification:turn/progress" {
		t.Fatalf("unexpected unhandled events: %#v", events)
	}
}

func TestCodexShouldPersistDiagnosticsForSilentTurn(t *testing.T) {
	t.Parallel()

	silent := map[string]any{
		"turn_started_seen":            true,
		"unhandled_notification_count": 0,
		"malformed_line_count":         0,
		"message_counts":               map[string]any{},
	}
	if !codexShouldPersistDiagnostics(silent) {
		t.Fatal("expected silent turn diagnostics to be persisted")
	}

	withText := map[string]any{
		"turn_started_seen":            true,
		"unhandled_notification_count": 0,
		"malformed_line_count":         0,
		"message_counts": map[string]any{
			string(MessageText): 1,
		},
	}
	if codexShouldPersistDiagnostics(withText) {
		t.Fatal("did not expect diagnostics persistence when usable output exists")
	}
}

func TestCodexExecuteSilentRunPersistsDiagnostics(t *testing.T) {
	oldGrace := codexSilentTurnGracePeriod
	oldPoll := codexSilentTurnPollInterval
	oldShutdown := codexShutdownGracePeriod
	oldForced := codexForcedShutdownWait
	codexSilentTurnGracePeriod = 150 * time.Millisecond
	codexSilentTurnPollInterval = 25 * time.Millisecond
	codexShutdownGracePeriod = 250 * time.Millisecond
	codexForcedShutdownWait = 100 * time.Millisecond
	t.Cleanup(func() {
		codexSilentTurnGracePeriod = oldGrace
		codexSilentTurnPollInterval = oldPoll
		codexShutdownGracePeriod = oldShutdown
		codexForcedShutdownWait = oldForced
	})

	tmpDir := t.TempDir()
	execPath := filepath.Join(tmpDir, "codex")
	script := `#!/usr/bin/env python3
import json
import sys

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"capabilities": {}}})
    elif method == "thread/start":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"thread": {"id": "thread-1"}}})
    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"ok": True}})
        send({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn-1"}}})
        send({"jsonrpc": "2.0", "method": "turn/progress", "params": {"pct": 10}})
`
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake codex executable: %v", err)
	}

	backend := &codexBackend{
		cfg: Config{
			ExecutablePath: execPath,
			Logger:         slog.Default(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := backend.Execute(ctx, "diagnose the silent turn", ExecOptions{
		Cwd:     tmpDir,
		Timeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var sawDiagnosticLog bool
	for msg := range session.Messages {
		if msg.Type == MessageLog && strings.Contains(msg.Content, "silent turn diagnostics") {
			sawDiagnosticLog = true
		}
	}

	result := <-session.Result
	if result.Status != "completed" {
		t.Fatalf("expected completed status, got %q (error=%q)", result.Status, result.Error)
	}
	if result.Output != "" {
		t.Fatalf("expected empty output, got %q", result.Output)
	}
	if !sawDiagnosticLog {
		t.Fatal("expected watchdog diagnostic log message")
	}
	if len(result.Diagnostics) == 0 {
		t.Fatal("expected diagnostics snapshot to be attached")
	}
	artifactPath, _ := result.Diagnostics["diagnostic_artifact_path"].(string)
	if artifactPath == "" {
		t.Fatal("expected diagnostic artifact path to be recorded")
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("expected diagnostic artifact to exist: %v", err)
	}
}
