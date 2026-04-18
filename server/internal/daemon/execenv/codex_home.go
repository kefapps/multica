package execenv

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/multica-ai/multica/server/internal/daemon/repocache"
	toml "github.com/pelletier/go-toml/v2"
)

// Directories to symlink from the shared ~/.codex/ into the per-task CODEX_HOME.
// The shared directory is created if it doesn't exist, ensuring Codex session
// logs are always written to the global home where users can find them.
var codexSymlinkedDirs = []string{
	"sessions",
}

// Files to symlink from the shared ~/.codex/ into the per-task CODEX_HOME.
// Symlinks share state (e.g. auth tokens) so changes propagate automatically.
var codexSymlinkedFiles = []string{
	"auth.json",
}

// Files that used to be copied from ~/.codex/ into the task CODEX_HOME but are
// now deliberately scrubbed to keep task runtimes deterministic and lightweight.
var codexRemovedFiles = []string{
	"config.json",
	"instructions.md",
}

const codexRepoOverlayPath = ".multica/codex-task.toml"

type CodexHomeParams struct {
	CodexHome        string
	WorkspacesRoot   string
	WorkspaceID      string
	Repos            []RepoContextForEnv
	PreferredRepoURL string
	Logger           *slog.Logger
}

// defaultCodexConfig is the minimal config.toml for Codex tasks.
// It sets workspace-write sandbox mode with network access enabled and
// explicitly disables hooks so repo tasks do not inherit arbitrary global
// automations from the user's main Codex home.
const defaultCodexConfig = `sandbox_mode = "workspace-write"
codex_hooks = false

[sandbox_workspace_write]
network_access = true
`

// prepareCodexHome creates a per-task CODEX_HOME directory with a deterministic
// baseline configuration. Only auth/session state is shared from ~/.codex; task
// config is generated fresh and optionally overlaid from repo-local settings.
func prepareCodexHome(params CodexHomeParams) error {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}

	sharedHome := resolveSharedCodexHome()

	if err := os.MkdirAll(params.CodexHome, 0o755); err != nil {
		return fmt.Errorf("create codex-home dir: %w", err)
	}

	// Symlink shared directories (sessions) so logs stay in the global home.
	for _, name := range codexSymlinkedDirs {
		src := filepath.Join(sharedHome, name)
		dst := filepath.Join(params.CodexHome, name)
		if err := ensureDirSymlink(src, dst); err != nil {
			logger.Warn("execenv: codex-home dir symlink failed", "dir", name, "error", err)
		}
	}

	// Symlink shared files (auth).
	for _, name := range codexSymlinkedFiles {
		src := filepath.Join(sharedHome, name)
		dst := filepath.Join(params.CodexHome, name)
		if err := ensureSymlink(src, dst); err != nil {
			logger.Warn("execenv: codex-home symlink failed", "file", name, "error", err)
		}
	}

	// Remove config files inherited by older task envs so reuse stays aligned
	// with the new isolated policy.
	for _, name := range append(codexRemovedFiles, "config.toml") {
		_ = os.Remove(filepath.Join(params.CodexHome, name))
	}

	overlay, overlaySource, overlayErr := loadCodexRepoOverlay(params.WorkspacesRoot, params.WorkspaceID, params.Repos, params.PreferredRepoURL, logger)
	if overlayErr != nil {
		logger.Warn("execenv: codex-home repo overlay unavailable, using baseline config only", "error", overlayErr)
		overlay = ""
		overlaySource = ""
	}

	writableRoots := computeCodexWritableRoots(params.WorkspacesRoot, params.WorkspaceID, params.Repos)
	configBytes, err := renderCodexConfig(writableRoots, overlay)
	if err != nil {
		return fmt.Errorf("render codex config: %w", err)
	}
	configPath := filepath.Join(params.CodexHome, "config.toml")
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil {
		return fmt.Errorf("write config.toml: %w", err)
	}

	configSource := "baseline"
	if overlaySource != "" {
		configSource = "baseline+repo-overlay"
	}
	logger.Info("execenv: prepared codex-home config",
		"codex_home", params.CodexHome,
		"config_source", configSource,
		"overlay_source", overlaySource,
		"writable_roots", writableRoots,
	)

	return nil
}

func renderCodexConfig(writableRoots []string, overlay string) ([]byte, error) {
	base := map[string]any{
		"sandbox_mode": "workspace-write",
		"codex_hooks":  false,
		"sandbox_workspace_write": map[string]any{
			"network_access": true,
		},
	}

	if strings.TrimSpace(overlay) != "" {
		var overlayMap map[string]any
		if err := toml.Unmarshal([]byte(overlay), &overlayMap); err != nil {
			return nil, fmt.Errorf("parse repo overlay: %w", err)
		}
		mergeTOMLMap(base, overlayMap)
	}

	if len(writableRoots) > 0 {
		sandboxCfg, ok := base["sandbox_workspace_write"].(map[string]any)
		if !ok || sandboxCfg == nil {
			sandboxCfg = map[string]any{}
			base["sandbox_workspace_write"] = sandboxCfg
		}
		sandboxCfg["writable_roots"] = mergeWritableRoots(sandboxCfg["writable_roots"], writableRoots)
	}

	return toml.Marshal(base)
}

func computeCodexWritableRoots(workspacesRoot, workspaceID string, repos []RepoContextForEnv) []string {
	if workspacesRoot == "" || workspaceID == "" || len(repos) == 0 {
		return nil
	}

	cacheRoot := filepath.Join(workspacesRoot, ".repos")
	seen := make(map[string]struct{}, len(repos))
	roots := make([]string, 0, len(repos))
	for _, repo := range repos {
		if strings.TrimSpace(repo.URL) == "" {
			continue
		}
		for _, root := range []string{
			repocache.BareRepoDir(cacheRoot, workspaceID, repo.URL),
			repocache.WorktreeAdminDir(cacheRoot, workspaceID, repo.URL),
		} {
			if _, ok := seen[root]; ok {
				continue
			}
			seen[root] = struct{}{}
			roots = append(roots, root)
		}
	}
	sort.Strings(roots)
	return roots
}

func mergeWritableRoots(existing any, required []string) []string {
	if len(required) == 0 {
		return stringSliceFromAny(existing)
	}

	seen := make(map[string]struct{}, len(required))
	merged := make([]string, 0, len(required))
	for _, root := range stringSliceFromAny(existing) {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		merged = append(merged, root)
	}
	for _, root := range required {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		merged = append(merged, root)
	}
	sort.Strings(merged)
	return merged
}

func stringSliceFromAny(v any) []string {
	switch raw := v.(type) {
	case nil:
		return nil
	case []string:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			s, ok := item.(string)
			if !ok {
				continue
			}
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func mergeTOMLMap(dst, src map[string]any) {
	for key, srcVal := range src {
		srcMap, srcIsMap := srcVal.(map[string]any)
		if !srcIsMap {
			dst[key] = srcVal
			continue
		}

		dstMap, dstIsMap := dst[key].(map[string]any)
		if !dstIsMap {
			dst[key] = srcMap
			continue
		}
		mergeTOMLMap(dstMap, srcMap)
	}
}

func loadCodexRepoOverlay(workspacesRoot, workspaceID string, repos []RepoContextForEnv, preferredRepoURL string, logger *slog.Logger) (string, string, error) {
	if workspacesRoot == "" || workspaceID == "" || len(repos) == 0 {
		return "", "", nil
	}
	cache := repocache.New(filepath.Join(workspacesRoot, ".repos"), logger)

	type candidate struct {
		source string
		body   string
	}

	trimmedPreferred := strings.TrimSpace(preferredRepoURL)
	filteredRepos := repos
	if trimmedPreferred != "" {
		filteredRepos = nil
		for _, repo := range repos {
			if strings.EqualFold(strings.TrimSpace(repo.URL), trimmedPreferred) {
				filteredRepos = append(filteredRepos, repo)
				break
			}
		}
		if len(filteredRepos) == 0 {
			return "", "", fmt.Errorf("preferred repo overlay requested for %q but repo not present in task context", trimmedPreferred)
		}
	}

	var matches []candidate
	for _, repo := range filteredRepos {
		barePath := cache.Lookup(workspaceID, repo.URL)
		if barePath == "" {
			continue
		}
		ref := repocache.DefaultBranchRef(barePath)
		if ref == "" {
			logger.Warn("execenv: repo overlay skipped, default branch unresolved", "repo", repo.URL)
			continue
		}
		data, err := repocache.ReadFileAtRef(barePath, ref, codexRepoOverlayPath)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "path '") && strings.Contains(strings.ToLower(err.Error()), "does not exist") {
				continue
			}
			logger.Warn("execenv: repo overlay read failed", "repo", repo.URL, "ref", ref, "error", err)
			continue
		}
		matches = append(matches, candidate{
			source: fmt.Sprintf("%s@%s:%s", repo.URL, ref, codexRepoOverlayPath),
			body:   string(data),
		})
	}

	switch len(matches) {
	case 0:
		return "", "", nil
	case 1:
		return matches[0].body, matches[0].source, nil
	default:
		sources := make([]string, 0, len(matches))
		for _, match := range matches {
			sources = append(sources, match.source)
		}
		return "", "", fmt.Errorf("multiple repo overlays found (%s)", strings.Join(sources, ", "))
	}
}

// resolveSharedCodexHome returns the path to the user's shared Codex home.
// Checks $CODEX_HOME first, falls back to ~/.codex.
func resolveSharedCodexHome() string {
	if v := os.Getenv("CODEX_HOME"); v != "" {
		abs, err := filepath.Abs(v)
		if err == nil {
			return abs
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), ".codex") // last resort fallback
	}
	return filepath.Join(home, ".codex")
}

// ensureDirSymlink creates a symlink dst → src for a directory.
// Unlike ensureSymlink, it creates the source directory if it doesn't exist,
// so Codex can write to it immediately.
func ensureDirSymlink(src, dst string) error {
	if err := os.MkdirAll(src, 0o755); err != nil {
		return fmt.Errorf("create shared dir %s: %w", src, err)
	}

	// Check if dst already exists.
	if fi, err := os.Lstat(dst); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(dst)
			if err == nil && target == src {
				return nil // already correct
			}
			_ = os.Remove(dst)
		} else {
			// Regular file/dir exists — don't overwrite.
			return nil
		}
	}

	return createDirLink(src, dst)
}

// ensureSymlink creates a symlink dst → src. If src doesn't exist, it's a no-op.
// If dst already exists as a correct symlink, it's a no-op. If dst is a broken
// symlink, it's replaced.
func ensureSymlink(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return nil // source doesn't exist — skip
	}

	// Check if dst already exists.
	if fi, err := os.Lstat(dst); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(dst)
			if err == nil && target == src {
				return nil // already correct
			}
			_ = os.Remove(dst)
		} else {
			// Regular file exists — don't overwrite.
			return nil
		}
	}

	return createFileLink(src, dst)
}

// copyFile copies src to dst unconditionally.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s → %s: %w", src, dst, err)
	}
	return nil
}
