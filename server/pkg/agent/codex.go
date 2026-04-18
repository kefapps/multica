package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// codexBlockedArgs are flags hardcoded by the daemon that must not be
// overridden by user-configured custom_args.
var codexBlockedArgs = map[string]blockedArgMode{
	"--listen": blockedWithValue, // stdio:// transport for daemon communication
}

var (
	// codexShutdownGracePeriod bounds how long we wait for the Codex app-server
	// to exit cleanly after the turn is finished.
	codexShutdownGracePeriod = 5 * time.Second
	// codexForcedShutdownWait bounds the final wait after we forcibly kill a
	// stuck Codex process during session shutdown.
	codexForcedShutdownWait = 2 * time.Second
	// codexSilentTurnGracePeriod is the fallback idle window after which we
	// conclude that Codex failed to emit an explicit turn completion event.
	codexSilentTurnGracePeriod = 90 * time.Second
	// codexSilentTurnPollInterval controls how often the idle watchdog checks
	// for a stalled turn.
	codexSilentTurnPollInterval = 1 * time.Second
)

const codexDiagnosticHistoryLimit = 8

type codexDiagnostics struct {
	start time.Time

	mu sync.Mutex

	rawLineCount               int
	malformedLineCount         int
	responseCount              int
	serverRequestCount         int
	notificationCount          int
	legacyEventCount           int
	rawNotificationCount       int
	unhandledNotificationCount int

	firstLineMs         int64
	firstNotificationMs int64
	turnStartedMs       int64
	firstMappedMsgMs    int64
	lastLineMs          int64
	lastMappedMsgMs     int64

	recentNotificationMethods []string
	recentLegacyEventTypes    []string
	recentUnhandledEvents     []string
	messageCounts             map[MessageType]int

	lastMalformedLine string
	lastReaderError   string
}

func newCodexDiagnostics(start time.Time) *codexDiagnostics {
	return &codexDiagnostics{
		start:               start,
		firstLineMs:         -1,
		firstNotificationMs: -1,
		turnStartedMs:       -1,
		firstMappedMsgMs:    -1,
		lastMappedMsgMs:     -1,
		messageCounts:       make(map[MessageType]int),
	}
}

func (d *codexDiagnostics) elapsedMs(now time.Time) int64 {
	return now.Sub(d.start).Milliseconds()
}

func (d *codexDiagnostics) pushRecent(dst *[]string, value string) {
	if value == "" {
		return
	}
	*dst = append(*dst, value)
	if len(*dst) > codexDiagnosticHistoryLimit {
		*dst = append([]string(nil), (*dst)[len(*dst)-codexDiagnosticHistoryLimit:]...)
	}
}

func (d *codexDiagnostics) noteLine(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rawLineCount++
	elapsed := d.elapsedMs(now)
	if d.firstLineMs < 0 {
		d.firstLineMs = elapsed
	}
	d.lastLineMs = elapsed
}

func (d *codexDiagnostics) noteMalformedLine(line string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.malformedLineCount++
	if len(line) > 200 {
		line = line[:200]
	}
	d.lastMalformedLine = line
}

func (d *codexDiagnostics) noteResponse() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.responseCount++
}

func (d *codexDiagnostics) noteServerRequest() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.serverRequestCount++
}

func (d *codexDiagnostics) noteNotification(now time.Time, method string, protocol string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.notificationCount++
	if d.firstNotificationMs < 0 {
		d.firstNotificationMs = d.elapsedMs(now)
	}
	switch protocol {
	case "legacy":
		d.legacyEventCount++
	case "raw":
		d.rawNotificationCount++
	}
	d.pushRecent(&d.recentNotificationMethods, method)
}

func (d *codexDiagnostics) noteLegacyEvent(msgType string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pushRecent(&d.recentLegacyEventTypes, msgType)
}

func (d *codexDiagnostics) noteUnhandledEvent(detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.unhandledNotificationCount++
	d.pushRecent(&d.recentUnhandledEvents, detail)
}

func (d *codexDiagnostics) noteTurnStarted(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.turnStartedMs < 0 {
		d.turnStartedMs = d.elapsedMs(now)
	}
}

func (d *codexDiagnostics) noteMessage(now time.Time, msg Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messageCounts[msg.Type]++
	elapsed := d.elapsedMs(now)
	if d.firstMappedMsgMs < 0 {
		d.firstMappedMsgMs = elapsed
	}
	d.lastMappedMsgMs = elapsed
}

func (d *codexDiagnostics) noteReaderError(err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastReaderError = err.Error()
}

func (d *codexDiagnostics) snapshot(protocol string, turnStarted bool, turnID string, completedTurnCount int) map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()

	messageCounts := make(map[string]any, len(d.messageCounts))
	for msgType, count := range d.messageCounts {
		messageCounts[string(msgType)] = count
	}

	snapshot := map[string]any{
		"protocol":                     protocol,
		"raw_line_count":               d.rawLineCount,
		"malformed_line_count":         d.malformedLineCount,
		"response_count":               d.responseCount,
		"server_request_count":         d.serverRequestCount,
		"notification_count":           d.notificationCount,
		"legacy_event_count":           d.legacyEventCount,
		"raw_notification_count":       d.rawNotificationCount,
		"unhandled_notification_count": d.unhandledNotificationCount,
		"turn_started_seen":            turnStarted,
		"turn_id":                      turnID,
		"completed_turn_count":         completedTurnCount,
		"first_line_ms":                d.firstLineMs,
		"first_notification_ms":        d.firstNotificationMs,
		"turn_started_ms":              d.turnStartedMs,
		"first_mapped_message_ms":      d.firstMappedMsgMs,
		"last_line_ms":                 d.lastLineMs,
		"last_mapped_message_ms":       d.lastMappedMsgMs,
		"recent_notification_methods":  append([]string(nil), d.recentNotificationMethods...),
		"recent_legacy_event_types":    append([]string(nil), d.recentLegacyEventTypes...),
		"recent_unhandled_events":      append([]string(nil), d.recentUnhandledEvents...),
		"message_counts":               messageCounts,
	}
	if d.lastMalformedLine != "" {
		snapshot["last_malformed_line"] = d.lastMalformedLine
	}
	if d.lastReaderError != "" {
		snapshot["reader_error"] = d.lastReaderError
	}
	return snapshot
}

func codexDiagnosticsSummary(snapshot map[string]any) string {
	if len(snapshot) == 0 {
		return ""
	}
	parts := []string{
		fmt.Sprintf("protocol=%v", snapshot["protocol"]),
		fmt.Sprintf("lines=%v", snapshot["raw_line_count"]),
		fmt.Sprintf("notifications=%v", snapshot["notification_count"]),
		fmt.Sprintf("responses=%v", snapshot["response_count"]),
		fmt.Sprintf("unhandled=%v", snapshot["unhandled_notification_count"]),
		fmt.Sprintf("turn_started=%v", snapshot["turn_started_seen"]),
	}
	if methods, ok := snapshot["recent_notification_methods"].([]string); ok && len(methods) > 0 {
		parts = append(parts, fmt.Sprintf("recent_methods=%s", strings.Join(methods, ",")))
	}
	if events, ok := snapshot["recent_unhandled_events"].([]string); ok && len(events) > 0 {
		parts = append(parts, fmt.Sprintf("recent_unhandled=%s", strings.Join(events, ",")))
	}
	return strings.Join(parts, " ")
}

func codexShouldPersistDiagnostics(snapshot map[string]any) bool {
	if len(snapshot) == 0 {
		return false
	}
	if count, _ := snapshot["unhandled_notification_count"].(int); count > 0 {
		return true
	}
	if count, _ := snapshot["malformed_line_count"].(int); count > 0 {
		return true
	}
	if turnStarted, _ := snapshot["turn_started_seen"].(bool); !turnStarted {
		return false
	}
	if counts, ok := snapshot["message_counts"].(map[string]any); ok {
		for _, key := range []string{string(MessageText), string(MessageThinking), string(MessageToolUse), string(MessageToolResult)} {
			if n, ok := counts[key].(int); ok && n > 0 {
				return false
			}
		}
		return true
	}
	return false
}

func persistCodexDiagnosticsArtifact(cwd string, snapshot map[string]any, logger *slog.Logger) string {
	if cwd == "" || len(snapshot) == 0 {
		return ""
	}
	debugDir := filepath.Join(cwd, ".multica-debug")
	if err := os.MkdirAll(debugDir, 0o755); err != nil {
		if logger != nil {
			logger.Warn("codex diagnostics artifact mkdir failed", "error", err)
		}
		return ""
	}
	path := filepath.Join(debugDir, fmt.Sprintf("codex-silent-turn-%d.json", time.Now().UnixNano()))
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		if logger != nil {
			logger.Warn("codex diagnostics artifact marshal failed", "error", err)
		}
		return ""
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		if logger != nil {
			logger.Warn("codex diagnostics artifact write failed", "error", err)
		}
		return ""
	}
	return path
}

// codexBackend implements Backend by spawning `codex app-server --listen stdio://`
// and communicating via JSON-RPC 2.0 over stdin/stdout.
type codexBackend struct {
	cfg Config
}

func (b *codexBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "codex"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("codex executable not found at %q: %w", execPath, err)
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 20 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)

	codexArgs := append([]string{"app-server", "--listen", "stdio://"}, filterCustomArgs(opts.CustomArgs, codexBlockedArgs, b.cfg.Logger)...)
	cmd := exec.CommandContext(runCtx, execPath, codexArgs...)
	b.cfg.Logger.Debug("agent command", "exec", execPath, "args", codexArgs)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("codex stdin pipe: %w", err)
	}
	cmd.Stderr = newLogWriter(b.cfg.Logger, "[codex:stderr] ")

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start codex: %w", err)
	}

	b.cfg.Logger.Info("codex started app-server", "pid", cmd.Process.Pid, "cwd", opts.Cwd)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)
	diagnostics := newCodexDiagnostics(time.Now())

	var outputMu sync.Mutex
	var output strings.Builder
	var inFlightTools atomic.Int32
	var sawActivity atomic.Bool
	var lastActivity atomic.Int64
	markActivity := func() {
		sawActivity.Store(true)
		lastActivity.Store(time.Now().UnixNano())
	}

	// turnDone is set before starting the reader goroutine so there is no
	// race between the lifecycle goroutine writing and the reader reading.
	turnDone := make(chan bool, 1) // true = aborted

	c := &codexClient{
		cfg:                  b.cfg,
		stdin:                stdin,
		pending:              make(map[int]*pendingRPC),
		notificationProtocol: "unknown",
		diagnostics:          diagnostics,
		onMessage: func(msg Message) {
			diagnostics.noteMessage(time.Now(), msg)
			markActivity()
			if msg.Type == MessageText {
				outputMu.Lock()
				output.WriteString(msg.Content)
				outputMu.Unlock()
			}
			switch msg.Type {
			case MessageToolUse:
				inFlightTools.Add(1)
			case MessageToolResult:
				for {
					current := inFlightTools.Load()
					if current == 0 {
						break
					}
					if inFlightTools.CompareAndSwap(current, current-1) {
						break
					}
				}
			}
			trySend(msgCh, msg)
		},
		onTurnDone: func(aborted bool) {
			select {
			case turnDone <- aborted:
			default:
			}
		},
	}

	// Start reading stdout in background
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			diagnostics.noteLine(time.Now())
			c.handleLine(line)
		}
		diagnostics.noteReaderError(scanner.Err())
		c.closeAllPending(fmt.Errorf("codex process exited"))
	}()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	// Drive the session lifecycle in a goroutine.
	// Shutdown sequence: lifecycle goroutine closes stdin + cancels context →
	// codex process exits → reader goroutine's scanner.Scan() returns false →
	// readerDone closes → lifecycle goroutine collects final output and sends Result.
	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)

		startTime := time.Now()
		finalStatus := "completed"
		var finalError string

		// 1. Initialize handshake
		_, err := c.request(runCtx, "initialize", map[string]any{
			"clientInfo": map[string]any{
				"name":    "multica-agent-sdk",
				"title":   "Multica Agent SDK",
				"version": "0.2.0",
			},
			"capabilities": map[string]any{
				"experimentalApi": true,
			},
		})
		if err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("codex initialize failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}
		c.notify("initialized")

		// 2. Start thread
		threadResult, err := c.request(runCtx, "thread/start", map[string]any{
			"model":                  nilIfEmpty(opts.Model),
			"modelProvider":          nil,
			"profile":                nil,
			"cwd":                    opts.Cwd,
			"approvalPolicy":         nil,
			"sandbox":                nil,
			"config":                 nil,
			"baseInstructions":       nil,
			"developerInstructions":  nilIfEmpty(opts.SystemPrompt),
			"compactPrompt":          nil,
			"includeApplyPatchTool":  nil,
			"experimentalRawEvents":  false,
			"persistExtendedHistory": true,
		})
		if err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("codex thread/start failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}

		threadID := extractThreadID(threadResult)
		if threadID == "" {
			finalStatus = "failed"
			finalError = "codex thread/start returned no thread ID"
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}
		c.threadID = threadID
		b.cfg.Logger.Info("codex thread started", "thread_id", threadID)

		// 3. Send turn and wait for completion
		markActivity()
		_, err = c.request(runCtx, "turn/start", map[string]any{
			"threadId": threadID,
			"input": []map[string]any{
				{"type": "text", "text": prompt},
			},
		})
		if err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("codex turn/start failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}

		watchdogDone := make(chan struct{})
		defer close(watchdogDone)
		go func() {
			ticker := time.NewTicker(codexSilentTurnPollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-watchdogDone:
					return
				case <-runCtx.Done():
					return
				case <-ticker.C:
					if !c.turnStarted || !sawActivity.Load() || inFlightTools.Load() != 0 {
						continue
					}
					last := time.Unix(0, lastActivity.Load())
					idleFor := time.Since(last)
					if idleFor < codexSilentTurnGracePeriod {
						continue
					}
					b.cfg.Logger.Warn(
						"codex turn produced no completion event; forcing completion after inactivity",
						"pid", cmd.Process.Pid,
						"idle_ms", idleFor.Milliseconds(),
					)
					snapshot := diagnostics.snapshot(c.notificationProtocol, c.turnStarted, c.turnID, len(c.completedTurnIDs))
					summary := codexDiagnosticsSummary(snapshot)
					b.cfg.Logger.Warn("codex silent turn diagnostics", "pid", cmd.Process.Pid, "summary", summary, "diagnostics", snapshot)
					trySend(msgCh, Message{
						Type:    MessageLog,
						Level:   "warn",
						Content: "codex silent turn diagnostics: " + summary,
					})
					select {
					case turnDone <- false:
					default:
					}
					return
				}
			}
		}()

		// Wait for turn completion or context cancellation
		select {
		case aborted := <-turnDone:
			if aborted {
				finalStatus = "aborted"
				finalError = "turn was aborted"
			}
		case <-runCtx.Done():
			if runCtx.Err() == context.DeadlineExceeded {
				finalStatus = "timeout"
				finalError = fmt.Sprintf("codex timed out after %s", timeout)
			} else {
				finalStatus = "aborted"
				finalError = "execution cancelled"
			}
		}

		duration := time.Since(startTime)
		b.cfg.Logger.Info("codex finished", "pid", cmd.Process.Pid, "status", finalStatus, "duration", duration.Round(time.Millisecond).String())

		if err := shutdownCodexSession(cmd, stdin, cancel, readerDone, waitDone, b.cfg.Logger); err != nil {
			b.cfg.Logger.Warn("codex shutdown required forced termination", "error", err)
		}

		outputMu.Lock()
		finalOutput := output.String()
		outputMu.Unlock()

		// Build usage map from accumulated codex usage.
		// First check JSON-RPC notifications (often empty for Codex).
		var usageMap map[string]TokenUsage
		c.usageMu.Lock()
		u := c.usage
		c.usageMu.Unlock()

		// Fallback: if no usage from JSON-RPC, scan Codex session JSONL logs.
		// Codex writes token_count events to ~/.codex/sessions/YYYY/MM/DD/*.jsonl.
		if u.InputTokens == 0 && u.OutputTokens == 0 {
			if scanned := scanCodexSessionUsage(startTime); scanned != nil {
				u = scanned.usage
				if scanned.model != "" && opts.Model == "" {
					opts.Model = scanned.model
				}
			}
		}

		if u.InputTokens > 0 || u.OutputTokens > 0 || u.CacheReadTokens > 0 || u.CacheWriteTokens > 0 {
			model := opts.Model
			if model == "" {
				model = "unknown"
			}
			usageMap = map[string]TokenUsage{model: u}
		}

		diagnosticSnapshot := diagnostics.snapshot(c.notificationProtocol, c.turnStarted, c.turnID, len(c.completedTurnIDs))
		if codexShouldPersistDiagnostics(diagnosticSnapshot) {
			if artifactPath := persistCodexDiagnosticsArtifact(opts.Cwd, diagnosticSnapshot, b.cfg.Logger); artifactPath != "" {
				diagnosticSnapshot["diagnostic_artifact_path"] = artifactPath
			}
		}

		resCh <- Result{
			Status:      finalStatus,
			Output:      finalOutput,
			Error:       finalError,
			DurationMs:  duration.Milliseconds(),
			Usage:       usageMap,
			Diagnostics: diagnosticSnapshot,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

func shutdownCodexSession(
	cmd *exec.Cmd,
	stdin interface{ Close() error },
	cancel context.CancelFunc,
	readerDone <-chan struct{},
	waitDone <-chan error,
	logger *slog.Logger,
) error {
	stdin.Close()
	cancel()

	readerTimer := time.NewTimer(codexShutdownGracePeriod)
	defer readerTimer.Stop()

	readerExited := false
	select {
	case <-readerDone:
		readerExited = true
	case <-readerTimer.C:
		if cmd.Process != nil {
			logger.Warn("codex stdout reader did not exit after turn completion; killing process", "pid", cmd.Process.Pid)
			_ = cmd.Process.Kill()
		}
	}

	if !readerExited {
		forcedTimer := time.NewTimer(codexForcedShutdownWait)
		defer forcedTimer.Stop()
		select {
		case <-readerDone:
			readerExited = true
		case <-forcedTimer.C:
			logger.Warn("codex stdout reader still active after forced kill grace period")
		}
	}

	waitTimer := time.NewTimer(codexShutdownGracePeriod)
	defer waitTimer.Stop()

	select {
	case err := <-waitDone:
		return normalizeCodexWaitErr(err)
	case <-waitTimer.C:
		if cmd.Process != nil {
			logger.Warn("codex process wait exceeded grace period after turn completion", "pid", cmd.Process.Pid)
			_ = cmd.Process.Kill()
		}
		finalTimer := time.NewTimer(codexForcedShutdownWait)
		defer finalTimer.Stop()
		select {
		case err := <-waitDone:
			return normalizeCodexWaitErr(err)
		case <-finalTimer.C:
			return fmt.Errorf("codex process did not exit after forced shutdown")
		}
	}
}

func normalizeCodexWaitErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "signal: killed") || strings.Contains(msg, "signal: terminated") {
		return nil
	}
	return err
}

// ── codexClient: JSON-RPC 2.0 transport ──

type codexClient struct {
	cfg        Config
	stdin      interface{ Write([]byte) (int, error) }
	mu         sync.Mutex
	nextID     int
	pending    map[int]*pendingRPC
	threadID   string
	turnID     string
	onMessage  func(Message)
	onTurnDone func(aborted bool)

	notificationProtocol string // "unknown", "legacy", "raw"
	turnStarted          bool
	completedTurnIDs     map[string]bool
	streamedAgentItems   map[string]bool
	streamedToolItems    map[string]bool
	diagnostics          *codexDiagnostics

	usageMu sync.Mutex
	usage   TokenUsage // accumulated from turn events
}

type pendingRPC struct {
	ch     chan rpcResult
	method string
}

type rpcResult struct {
	result json.RawMessage
	err    error
}

func (c *codexClient) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	pr := &pendingRPC{ch: make(chan rpcResult, 1), method: method}
	c.pending[id] = pr
	c.mu.Unlock()

	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}
	data = append(data, '\n')
	if _, err := c.stdin.Write(data); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("write %s: %w", method, err)
	}

	select {
	case res := <-pr.ch:
		return res.result, res.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *codexClient) notify(method string) {
	msg := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')
	_, _ = c.stdin.Write(data)
}

func (c *codexClient) respond(id int, result any) {
	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	}
	data, _ := json.Marshal(msg)
	data = append(data, '\n')
	_, _ = c.stdin.Write(data)
}

func (c *codexClient) closeAllPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, pr := range c.pending {
		pr.ch <- rpcResult{err: err}
		delete(c.pending, id)
	}
}

func (c *codexClient) handleLine(line string) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		if c.diagnostics != nil {
			c.diagnostics.noteMalformedLine(line)
		}
		return
	}

	// Check if it's a response to our request
	if _, hasID := raw["id"]; hasID {
		if _, hasResult := raw["result"]; hasResult {
			c.handleResponse(raw)
			return
		}
		if _, hasError := raw["error"]; hasError {
			c.handleResponse(raw)
			return
		}
		// Server request (has id + method)
		if _, hasMethod := raw["method"]; hasMethod {
			c.handleServerRequest(raw)
			return
		}
	}

	// Notification (no id, has method)
	if _, hasMethod := raw["method"]; hasMethod {
		c.handleNotification(raw)
	}
}

func (c *codexClient) handleResponse(raw map[string]json.RawMessage) {
	if c.diagnostics != nil {
		c.diagnostics.noteResponse()
	}
	var id int
	if err := json.Unmarshal(raw["id"], &id); err != nil {
		return
	}

	c.mu.Lock()
	pr, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.mu.Unlock()

	if !ok {
		return
	}

	if errData, hasErr := raw["error"]; hasErr {
		var rpcErr struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(errData, &rpcErr)
		pr.ch <- rpcResult{err: fmt.Errorf("%s: %s (code=%d)", pr.method, rpcErr.Message, rpcErr.Code)}
	} else {
		pr.ch <- rpcResult{result: raw["result"]}
	}
}

func (c *codexClient) handleServerRequest(raw map[string]json.RawMessage) {
	if c.diagnostics != nil {
		c.diagnostics.noteServerRequest()
	}
	var id int
	_ = json.Unmarshal(raw["id"], &id)

	var method string
	_ = json.Unmarshal(raw["method"], &method)

	// Auto-approve all exec/patch requests in daemon mode
	switch method {
	case "item/commandExecution/requestApproval", "execCommandApproval":
		c.respond(id, map[string]any{"decision": "accept"})
	case "item/fileChange/requestApproval", "applyPatchApproval":
		c.respond(id, map[string]any{"decision": "accept"})
	default:
		c.respond(id, map[string]any{})
	}
}

func (c *codexClient) handleNotification(raw map[string]json.RawMessage) {
	var method string
	_ = json.Unmarshal(raw["method"], &method)
	handled := false

	var params map[string]any
	if p, ok := raw["params"]; ok {
		_ = json.Unmarshal(p, &params)
	}

	// Legacy codex/event notifications
	if method == "codex/event" || strings.HasPrefix(method, "codex/event/") {
		c.notificationProtocol = "legacy"
		if c.diagnostics != nil {
			c.diagnostics.noteNotification(time.Now(), method, "legacy")
		}
		handled = true
		msgData, ok := params["msg"]
		if !ok {
			return
		}
		msgMap, ok := msgData.(map[string]any)
		if !ok {
			return
		}
		c.handleEvent(msgMap)
		return
	}

	// Raw v2 notifications
	if c.notificationProtocol != "legacy" {
		if (c.notificationProtocol == "" || c.notificationProtocol == "unknown") &&
			(method == "turn/started" || method == "turn/completed" ||
				method == "thread/started" || strings.HasPrefix(method, "item/") ||
				method == "account/rateLimits/updated" ||
				method == "thread/tokenUsage/updated" ||
				method == "turn/diff/updated" ||
				method == "mcpServer/startupStatus/updated") {
			c.notificationProtocol = "raw"
		}

		if c.notificationProtocol == "raw" {
			if c.diagnostics != nil {
				c.diagnostics.noteNotification(time.Now(), method, "raw")
			}
			c.handleRawNotification(method, params)
			handled = true
		}
	}

	if !handled && c.diagnostics != nil {
		c.diagnostics.noteNotification(time.Now(), method, c.notificationProtocol)
		c.diagnostics.noteUnhandledEvent("notification:" + method)
	}
}

func (c *codexClient) handleEvent(msg map[string]any) {
	msgType, _ := msg["type"].(string)
	if c.diagnostics != nil {
		c.diagnostics.noteLegacyEvent(msgType)
	}

	switch msgType {
	case "task_started":
		c.turnStarted = true
		if c.diagnostics != nil {
			c.diagnostics.noteTurnStarted(time.Now())
		}
		if c.onMessage != nil {
			c.onMessage(Message{Type: MessageStatus, Status: "running"})
		}
	case "agent_message":
		text, _ := msg["message"].(string)
		if text != "" && c.onMessage != nil {
			c.onMessage(Message{Type: MessageText, Content: text})
		}
	case "exec_command_begin":
		callID, _ := msg["call_id"].(string)
		command, _ := msg["command"].(string)
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolUse,
				Tool:   "exec_command",
				CallID: callID,
				Input:  map[string]any{"command": command},
			})
		}
	case "exec_command_end":
		callID, _ := msg["call_id"].(string)
		output, _ := msg["output"].(string)
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolResult,
				Tool:   "exec_command",
				CallID: callID,
				Output: output,
			})
		}
	case "patch_apply_begin":
		callID, _ := msg["call_id"].(string)
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolUse,
				Tool:   "patch_apply",
				CallID: callID,
			})
		}
	case "patch_apply_end":
		callID, _ := msg["call_id"].(string)
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolResult,
				Tool:   "patch_apply",
				CallID: callID,
			})
		}
	case "task_complete":
		// Extract usage from legacy task_complete if present.
		c.extractUsageFromMap(msg)
		if c.onTurnDone != nil {
			c.onTurnDone(false)
		}
	case "turn_aborted":
		if c.onTurnDone != nil {
			c.onTurnDone(true)
		}
	default:
		if c.diagnostics != nil {
			c.diagnostics.noteUnhandledEvent("legacy:" + msgType)
		}
	}
}

func (c *codexClient) handleRawNotification(method string, params map[string]any) {
	switch method {
	case "turn/started":
		c.turnStarted = true
		if turnID := extractNestedString(params, "turn", "id"); turnID != "" {
			c.turnID = turnID
		}
		if c.diagnostics != nil {
			c.diagnostics.noteTurnStarted(time.Now())
		}
		if c.onMessage != nil {
			c.onMessage(Message{Type: MessageStatus, Status: "running"})
		}

	case "turn/completed":
		turnID := extractNestedString(params, "turn", "id")
		status := extractNestedString(params, "turn", "status")
		aborted := status == "cancelled" || status == "canceled" ||
			status == "aborted" || status == "interrupted"

		if c.completedTurnIDs == nil {
			c.completedTurnIDs = map[string]bool{}
		}
		if turnID != "" {
			if c.completedTurnIDs[turnID] {
				return
			}
			c.completedTurnIDs[turnID] = true
		}

		// Extract usage from turn/completed if present (e.g. params.turn.usage).
		if turn, ok := params["turn"].(map[string]any); ok {
			c.extractUsageFromMap(turn)
		}

		if c.onTurnDone != nil {
			c.onTurnDone(aborted)
		}

	case "thread/started":
		if threadID := extractNestedString(params, "thread", "id"); threadID != "" {
			c.threadID = threadID
		}

	case "thread/status/changed":
		statusType := extractNestedString(params, "status", "type")
		if statusType == "idle" && c.turnStarted {
			if c.onTurnDone != nil {
				c.onTurnDone(false)
			}
		}

	case "thread/tokenUsage/updated":
		if thread, ok := params["thread"].(map[string]any); ok {
			c.extractUsageFromMap(thread)
		}
		c.extractUsageFromMap(params)

	case "account/rateLimits/updated", "turn/diff/updated", "mcpServer/startupStatus/updated":
		// Metadata-only notifications. They are useful for diagnostics, but they
		// do not correspond to user-visible output or completion semantics.
		return

	default:
		if strings.HasPrefix(method, "item/") {
			if handled, detail := c.handleItemNotification(method, params); !handled && c.diagnostics != nil {
				if detail == "" {
					detail = method
				}
				c.diagnostics.noteUnhandledEvent(detail)
			}
			return
		}
		if c.diagnostics != nil {
			c.diagnostics.noteUnhandledEvent(method)
		}
	}
}

func (c *codexClient) handleItemNotification(method string, params map[string]any) (bool, string) {
	if method == "item/agentMessage/delta" {
		itemID, _ := params["itemId"].(string)
		delta, _ := params["delta"].(string)
		if delta == "" {
			return true, method + ":agentMessage"
		}
		if itemID != "" {
			if c.streamedAgentItems == nil {
				c.streamedAgentItems = make(map[string]bool)
			}
			c.streamedAgentItems[itemID] = true
		}
		if c.onMessage != nil {
			c.onMessage(Message{Type: MessageText, Content: delta})
		}
		return true, method + ":agentMessage"
	}
	if method == "item/commandExecution/outputDelta" {
		itemID, _ := params["itemId"].(string)
		delta, _ := params["delta"].(string)
		if delta == "" {
			return true, method + ":commandExecution"
		}
		if itemID != "" {
			if c.streamedToolItems == nil {
				c.streamedToolItems = make(map[string]bool)
			}
			c.streamedToolItems[itemID] = true
		}
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolResult,
				Tool:   "exec_command",
				CallID: itemID,
				Output: delta,
			})
		}
		return true, method + ":commandExecution"
	}

	item, ok := params["item"].(map[string]any)
	if !ok {
		return false, method
	}

	itemType, _ := item["type"].(string)
	itemID, _ := item["id"].(string)
	detail := method
	if itemType != "" {
		detail = method + ":" + itemType
	}

	switch {
	case method == "item/started" && itemType == "agentMessage":
		// Agent message lifecycle starts before any deltas or final completion.
		// The user-visible content is emitted by delta/completed notifications.
		return true, detail

	case method == "item/started" && itemType == "reasoning":
		// Reasoning events are internal lifecycle notices. They should not count
		// as unhandled noise in diagnostics.
		return true, detail

	case method == "item/completed" && itemType == "reasoning":
		return true, detail

	case method == "item/started" && itemType == "userMessage":
		return true, detail

	case method == "item/completed" && itemType == "userMessage":
		return true, detail

	case method == "item/started" && itemType == "commandExecution":
		command, _ := item["command"].(string)
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolUse,
				Tool:   "exec_command",
				CallID: itemID,
				Input:  map[string]any{"command": command},
			})
		}
		return true, detail

	case method == "item/completed" && itemType == "commandExecution":
		output, _ := item["aggregatedOutput"].(string)
		streamed := false
		if itemID != "" && c.streamedToolItems != nil {
			streamed = c.streamedToolItems[itemID]
			delete(c.streamedToolItems, itemID)
		}
		if c.onMessage != nil && !streamed {
			c.onMessage(Message{
				Type:   MessageToolResult,
				Tool:   "exec_command",
				CallID: itemID,
				Output: output,
			})
		}
		return true, detail

	case method == "item/started" && itemType == "fileChange":
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolUse,
				Tool:   "patch_apply",
				CallID: itemID,
			})
		}
		return true, detail

	case method == "item/completed" && itemType == "fileChange":
		if c.onMessage != nil {
			c.onMessage(Message{
				Type:   MessageToolResult,
				Tool:   "patch_apply",
				CallID: itemID,
			})
		}
		return true, detail

	case method == "item/completed" && itemType == "agentMessage":
		text, _ := item["text"].(string)
		streamed := false
		if itemID != "" && c.streamedAgentItems != nil {
			streamed = c.streamedAgentItems[itemID]
			delete(c.streamedAgentItems, itemID)
		}
		if text != "" && !streamed && c.onMessage != nil {
			c.onMessage(Message{Type: MessageText, Content: text})
		}
		phase, _ := item["phase"].(string)
		if phase == "final_answer" && c.turnStarted {
			if c.onTurnDone != nil {
				c.onTurnDone(false)
			}
		}
		return true, detail
	}
	return false, detail
}

// extractUsageFromMap extracts token usage from a map that may contain
// "usage", "token_usage", or "tokens" fields. Handles various Codex formats.
func (c *codexClient) extractUsageFromMap(data map[string]any) {
	// Try common field names for usage data.
	var usageMap map[string]any
	for _, key := range []string{"usage", "token_usage", "tokens"} {
		if v, ok := data[key].(map[string]any); ok {
			usageMap = v
			break
		}
	}
	if usageMap == nil {
		return
	}

	c.usageMu.Lock()
	defer c.usageMu.Unlock()

	// Try various key conventions.
	c.usage.InputTokens += codexInt64(usageMap, "input_tokens", "input", "prompt_tokens")
	c.usage.OutputTokens += codexInt64(usageMap, "output_tokens", "output", "completion_tokens")
	c.usage.CacheReadTokens += codexInt64(usageMap, "cache_read_tokens", "cache_read_input_tokens")
	c.usage.CacheWriteTokens += codexInt64(usageMap, "cache_write_tokens", "cache_creation_input_tokens")
}

// codexInt64 returns the first non-zero int64 value from the map for the given keys.
func codexInt64(m map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch v := m[key].(type) {
		case float64:
			if v != 0 {
				return int64(v)
			}
		case int64:
			if v != 0 {
				return v
			}
		}
	}
	return 0
}

// ── Codex session log scanner ──

// codexSessionUsage holds usage extracted from a Codex session JSONL file.
type codexSessionUsage struct {
	usage TokenUsage
	model string
}

// scanCodexSessionUsage scans Codex session JSONL files written after startTime
// to extract token usage. Codex writes token_count events to
// ~/.codex/sessions/YYYY/MM/DD/*.jsonl.
func scanCodexSessionUsage(startTime time.Time) *codexSessionUsage {
	root := codexSessionRoot()
	if root == "" {
		return nil
	}

	// Look in today's session directory.
	dateDir := filepath.Join(root,
		fmt.Sprintf("%04d", startTime.Year()),
		fmt.Sprintf("%02d", int(startTime.Month())),
		fmt.Sprintf("%02d", startTime.Day()),
	)

	files, err := filepath.Glob(filepath.Join(dateDir, "*.jsonl"))
	if err != nil || len(files) == 0 {
		return nil
	}

	// Only scan files modified after startTime (this task's session).
	var result codexSessionUsage
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil || info.ModTime().Before(startTime) {
			continue
		}
		if u := parseCodexSessionFile(f); u != nil {
			// Take the last matching file's data (usually there's only one per task).
			result = *u
		}
	}

	if result.usage.InputTokens == 0 && result.usage.OutputTokens == 0 {
		return nil
	}
	return &result
}

// codexSessionRoot returns the Codex sessions directory.
func codexSessionRoot() string {
	if codexHome := os.Getenv("CODEX_HOME"); codexHome != "" {
		dir := filepath.Join(codexHome, "sessions")
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return dir
		}
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	dir := filepath.Join(home, ".codex", "sessions")
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		return dir
	}
	return ""
}

// codexSessionTokenCount represents a token_count event in Codex JSONL.
type codexSessionTokenCount struct {
	Type    string `json:"type"`
	Payload *struct {
		Type string `json:"type"`
		Info *struct {
			TotalTokenUsage *struct {
				InputTokens           int64 `json:"input_tokens"`
				OutputTokens          int64 `json:"output_tokens"`
				CachedInputTokens     int64 `json:"cached_input_tokens"`
				CacheReadInputTokens  int64 `json:"cache_read_input_tokens"`
				ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
			} `json:"total_token_usage"`
			LastTokenUsage *struct {
				InputTokens           int64 `json:"input_tokens"`
				OutputTokens          int64 `json:"output_tokens"`
				CachedInputTokens     int64 `json:"cached_input_tokens"`
				CacheReadInputTokens  int64 `json:"cache_read_input_tokens"`
				ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
			} `json:"last_token_usage"`
			Model string `json:"model"`
		} `json:"info"`
		Model string `json:"model"`
	} `json:"payload"`
}

// parseCodexSessionFile extracts the final token_count from a Codex session file.
func parseCodexSessionFile(path string) *codexSessionUsage {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var result codexSessionUsage
	found := false

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()

		// Fast pre-filter.
		if !bytesContainsStr(line, "token_count") && !bytesContainsStr(line, "turn_context") {
			continue
		}

		var evt codexSessionTokenCount
		if err := json.Unmarshal(line, &evt); err != nil || evt.Payload == nil {
			continue
		}

		// Track model from turn_context events.
		if evt.Type == "turn_context" && evt.Payload.Model != "" {
			result.model = evt.Payload.Model
			continue
		}

		// Extract token usage from token_count events.
		if evt.Payload.Type == "token_count" && evt.Payload.Info != nil {
			usage := evt.Payload.Info.TotalTokenUsage
			if usage == nil {
				usage = evt.Payload.Info.LastTokenUsage
			}
			if usage != nil {
				cachedTokens := usage.CachedInputTokens
				if cachedTokens == 0 {
					cachedTokens = usage.CacheReadInputTokens
				}
				result.usage = TokenUsage{
					InputTokens:     usage.InputTokens,
					OutputTokens:    usage.OutputTokens + usage.ReasoningOutputTokens,
					CacheReadTokens: cachedTokens,
				}
				if evt.Payload.Info.Model != "" {
					result.model = evt.Payload.Info.Model
				}
				found = true
			}
		}
	}

	if !found {
		return nil
	}
	return &result
}

// bytesContainsStr checks if b contains the string s (without allocating).
func bytesContainsStr(b []byte, s string) bool {
	return strings.Contains(string(b), s)
}

// ── Helpers ──

func extractThreadID(result json.RawMessage) string {
	var r struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(result, &r); err != nil {
		return ""
	}
	return r.Thread.ID
}

func extractNestedString(m map[string]any, keys ...string) string {
	current := any(m)
	for _, key := range keys {
		obj, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = obj[key]
	}
	s, _ := current.(string)
	return s
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
