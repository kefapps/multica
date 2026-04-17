package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestNormalizeServerBaseURL(t *testing.T) {
	t.Parallel()

	got, err := NormalizeServerBaseURL("ws://localhost:8080/ws")
	if err != nil {
		t.Fatalf("NormalizeServerBaseURL returned error: %v", err)
	}
	if got != "http://localhost:8080" {
		t.Fatalf("expected http://localhost:8080, got %s", got)
	}
}

func TestBuildPromptContainsIssueID(t *testing.T) {
	t.Parallel()

	issueID := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	prompt := BuildPrompt(Task{
		IssueID: issueID,
		Agent: &AgentData{
			Name: "Local Codex",
			Skills: []SkillData{
				{Name: "Concise", Content: "Be concise."},
			},
		},
	})

	// Prompt should contain the issue ID and local-context hint.
	for _, want := range []string{
		issueID,
		".agent_context/",
		"multica` CLI read commands",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q", want)
		}
	}

	// Skills should NOT be inlined in the prompt (they're in runtime config).
	for _, absent := range []string{"## Agent Skills", "Be concise."} {
		if strings.Contains(prompt, absent) {
			t.Fatalf("prompt should NOT contain %q (skills are in runtime config)", absent)
		}
	}
}

func TestBuildPromptNoIssueDetails(t *testing.T) {
	t.Parallel()

	prompt := BuildPrompt(Task{
		IssueID: "test-id",
		Agent:   &AgentData{Name: "Test"},
	})

	// Prompt should not contain issue title/description (agent fetches via CLI).
	for _, absent := range []string{"**Issue:**", "**Summary:**"} {
		if strings.Contains(prompt, absent) {
			t.Fatalf("prompt should NOT contain %q — agent fetches details via CLI", absent)
		}
	}
}

func TestBuildPromptCommentTriggered(t *testing.T) {
	t.Parallel()

	issueID := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	commentID := "c1c2c3c4-d5d6-7890-abcd-ef1234567890"
	commentContent := "请把报告翻译成英文"

	prompt := BuildPrompt(Task{
		IssueID:               issueID,
		TriggerCommentID:      commentID,
		TriggerCommentContent: commentContent,
		Agent:                 &AgentData{Name: "Test"},
	})

	// Prompt should contain the comment content directly.
	for _, want := range []string{
		issueID,
		commentContent,
		"comment that triggered this task",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q", want)
		}
	}

	// Should still contain local-context guidance.
	if !strings.Contains(prompt, ".agent_context/") {
		t.Fatal("prompt missing local context guidance")
	}
}

func TestBuildPromptCommentTriggeredNoContent(t *testing.T) {
	t.Parallel()

	// When TriggerCommentID is set but content is empty (e.g. fetch failed),
	// it should still use the comment prompt path.
	prompt := BuildPrompt(Task{
		IssueID:          "test-id",
		TriggerCommentID: "comment-id",
		Agent:            &AgentData{Name: "Test"},
	})

	if !strings.Contains(prompt, ".agent_context/") {
		t.Fatal("prompt missing local context guidance")
	}
}

func TestIsWorkspaceNotFoundError(t *testing.T) {
	t.Parallel()

	err := &requestError{
		Method:     http.MethodPost,
		Path:       "/api/daemon/register",
		StatusCode: http.StatusNotFound,
		Body:       `{"error":"workspace not found"}`,
	}
	if !isWorkspaceNotFoundError(err) {
		t.Fatal("expected workspace not found error to be recognized")
	}

	if isWorkspaceNotFoundError(&requestError{StatusCode: http.StatusInternalServerError, Body: `{"error":"workspace not found"}`}) {
		t.Fatal("did not expect 500 to be treated as workspace not found")
	}
}

func TestClientGetIssueDataIncludesWorkspaceID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("workspace_id"); got != "ws-123" {
			t.Fatalf("expected workspace_id=ws-123, got %q", got)
		}
		if r.URL.Path != "/api/issues/issue-1" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "issue-1"})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	issue, err := client.GetIssueData(context.Background(), "ws-123", "issue-1")
	if err != nil {
		t.Fatalf("GetIssueData returned error: %v", err)
	}
	if got := issue["id"]; got != "issue-1" {
		t.Fatalf("expected issue id issue-1, got %v", got)
	}
}

func TestClientListIssueCommentsDataIncludesWorkspaceID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("workspace_id"); got != "ws-123" {
			t.Fatalf("expected workspace_id=ws-123, got %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "7" {
			t.Fatalf("expected limit=7, got %q", got)
		}
		if r.URL.Path != "/api/issues/issue-1/comments" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "comment-1"}})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	comments, err := client.ListIssueCommentsData(context.Background(), "ws-123", "issue-1", 7)
	if err != nil {
		t.Fatalf("ListIssueCommentsData returned error: %v", err)
	}
	if len(comments) != 1 || comments[0]["id"] != "comment-1" {
		t.Fatalf("unexpected comments payload: %+v", comments)
	}
}

func TestMergeUsage(t *testing.T) {
	t.Parallel()

	a := map[string]agent.TokenUsage{
		"model-a": {InputTokens: 10, OutputTokens: 5},
	}
	b := map[string]agent.TokenUsage{
		"model-a": {InputTokens: 20, OutputTokens: 10, CacheReadTokens: 3},
		"model-b": {InputTokens: 100},
	}
	merged := mergeUsage(a, b)

	if got := merged["model-a"]; got.InputTokens != 30 || got.OutputTokens != 15 || got.CacheReadTokens != 3 {
		t.Fatalf("model-a: expected {30,15,3,0}, got %+v", got)
	}
	if got := merged["model-b"]; got.InputTokens != 100 {
		t.Fatalf("model-b: expected InputTokens=100, got %+v", got)
	}

	if got := mergeUsage(nil, b); len(got) != 2 {
		t.Fatal("mergeUsage(nil, b) should return b")
	}
	if got := mergeUsage(a, nil); len(got) != 1 {
		t.Fatal("mergeUsage(a, nil) should return a")
	}
}

// fakeBackend is a test double for agent.Backend that returns preconfigured
// results. Each call to Execute pops the next entry from the results slice.
type fakeBackend struct {
	calls   []agent.ExecOptions
	results []agent.Result
	errors  []error
	idx     atomic.Int32
}

func (b *fakeBackend) Execute(_ context.Context, _ string, opts agent.ExecOptions) (*agent.Session, error) {
	i := int(b.idx.Add(1)) - 1
	b.calls = append(b.calls, opts)
	if i < len(b.errors) && b.errors[i] != nil {
		return nil, b.errors[i]
	}
	msgCh := make(chan agent.Message)
	resCh := make(chan agent.Result, 1)
	close(msgCh)
	resCh <- b.results[i]
	return &agent.Session{Messages: msgCh, Result: resCh}, nil
}

func newTestDaemon(t *testing.T) *Daemon {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return &Daemon{
		client: NewClient(srv.URL),
		logger: slog.Default(),
	}
}

func TestExecuteAndDrain_ResumeFailureFallback(t *testing.T) {
	t.Parallel()

	d := newTestDaemon(t)
	ctx := context.Background()
	taskLog := slog.Default()

	fb := &fakeBackend{
		results: []agent.Result{
			{Status: "failed", Error: "session not found", Usage: map[string]agent.TokenUsage{
				"m1": {InputTokens: 5},
			}},
			{Status: "completed", Output: "done", SessionID: "new-sess", Usage: map[string]agent.TokenUsage{
				"m1": {InputTokens: 10, OutputTokens: 20},
			}},
		},
	}

	// First attempt: resume fails (no SessionID in result).
	opts := agent.ExecOptions{ResumeSessionID: "stale-id"}
	result, _, err := d.executeAndDrain(ctx, fb, "prompt", opts, taskLog, "task-1")
	if err != nil {
		t.Fatalf("first call error: %v", err)
	}
	if result.Status != "failed" || result.SessionID != "" {
		t.Fatalf("expected failed result with empty SessionID, got %+v", result)
	}

	// Simulate the retry logic from runTask.
	if result.Status == "failed" && result.SessionID == "" {
		firstUsage := result.Usage
		opts.ResumeSessionID = ""
		retryResult, _, retryErr := d.executeAndDrain(ctx, fb, "prompt", opts, taskLog, "task-1")
		if retryErr != nil {
			t.Fatalf("retry error: %v", retryErr)
		}
		result = retryResult
		result.Usage = mergeUsage(firstUsage, result.Usage)
	}

	if result.Status != "completed" || result.Output != "done" {
		t.Fatalf("expected completed result, got %+v", result)
	}
	if result.SessionID != "new-sess" {
		t.Fatalf("expected new-sess, got %s", result.SessionID)
	}
	// Usage should be merged.
	if u := result.Usage["m1"]; u.InputTokens != 15 || u.OutputTokens != 20 {
		t.Fatalf("expected merged usage {15,20}, got %+v", u)
	}
	// Second call should NOT have ResumeSessionID.
	if fb.calls[1].ResumeSessionID != "" {
		t.Fatal("retry should not have ResumeSessionID")
	}
}

func TestExecuteAndDrain_NoRetryWhenSessionEstablished(t *testing.T) {
	t.Parallel()

	d := newTestDaemon(t)

	fb := &fakeBackend{
		results: []agent.Result{
			{Status: "failed", Error: "model error", SessionID: "valid-sess"},
		},
	}

	opts := agent.ExecOptions{ResumeSessionID: "some-id"}
	result, _, err := d.executeAndDrain(context.Background(), fb, "p", opts, slog.Default(), "t")
	if err != nil {
		t.Fatal(err)
	}

	// SessionID is set → session was established → should NOT retry.
	shouldRetry := result.Status == "failed" && result.SessionID == ""
	if shouldRetry {
		t.Fatal("should not retry when SessionID is present")
	}
	if int(fb.idx.Load()) != 1 {
		t.Fatalf("expected 1 call, got %d", fb.idx.Load())
	}
}
