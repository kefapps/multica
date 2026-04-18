package execenv

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// writeContextFiles renders and writes .agent_context/issue_context.md and
// structured task context plus skills into the appropriate provider-native
// location.
//
// Claude:   skills → {workDir}/.claude/skills/{name}/SKILL.md  (native discovery)
// Codex:    skills → handled separately in Prepare via codex-home
// OpenCode: skills → {workDir}/.config/opencode/skills/{name}/SKILL.md  (native discovery)
// Default:  skills → {workDir}/.agent_context/skills/{name}/SKILL.md
func writeContextFiles(workDir, provider string, ctx TaskContextForEnv) error {
	contextDir := filepath.Join(workDir, ".agent_context")
	if err := os.MkdirAll(contextDir, 0o755); err != nil {
		return fmt.Errorf("create .agent_context dir: %w", err)
	}

	content := renderIssueContext(provider, ctx)
	path := filepath.Join(contextDir, "issue_context.md")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write issue_context.md: %w", err)
	}

	issueData := ctx.IssueData
	if issueData == nil && ctx.IssueID != "" {
		issueData = map[string]any{"id": ctx.IssueID}
	}
	if err := writeJSONFile(filepath.Join(contextDir, "issue.json"), issueData); err != nil {
		return fmt.Errorf("write issue.json: %w", err)
	}
	if err := writeJSONFile(filepath.Join(contextDir, "comments.json"), nonNilCommentSlice(ctx.CommentsData)); err != nil {
		return fmt.Errorf("write comments.json: %w", err)
	}
	if err := writeJSONFile(filepath.Join(contextDir, "repos.json"), nonNilRepoSlice(ctx.Repos)); err != nil {
		return fmt.Errorf("write repos.json: %w", err)
	}
	if err := writeJSONFile(filepath.Join(contextDir, "task.json"), nonNilTaskData(ctx)); err != nil {
		return fmt.Errorf("write task.json: %w", err)
	}

	if len(ctx.AgentSkills) > 0 {
		skillsDir, err := resolveSkillsDir(workDir, provider)
		if err != nil {
			return fmt.Errorf("resolve skills dir: %w", err)
		}
		// Codex skills are written to codex-home in Prepare; skip here.
		if provider != "codex" {
			if err := writeSkillFiles(skillsDir, ctx.AgentSkills); err != nil {
				return fmt.Errorf("write skill files: %w", err)
			}
		}
	}

	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func nonNilCommentSlice(in []map[string]any) []map[string]any {
	if in == nil {
		return []map[string]any{}
	}
	return in
}

func nonNilRepoSlice(in []RepoContextForEnv) []RepoContextForEnv {
	if in == nil {
		return []RepoContextForEnv{}
	}
	return in
}

func nonNilTaskData(ctx TaskContextForEnv) map[string]any {
	if ctx.TaskData != nil {
		return ctx.TaskData
	}
	return map[string]any{
		"issue_id":                ctx.IssueID,
		"trigger_comment_id":      ctx.TriggerCommentID,
		"trigger_comment_content": ctx.TriggerCommentContent,
		"chat_session_id":         ctx.ChatSessionID,
		"agent_id":                ctx.AgentID,
		"agent_name":              ctx.AgentName,
		"preferred_repo_url":      ctx.PreferredRepoURL,
	}
}

// resolveSkillsDir returns the directory where skills should be written
// based on the agent provider.
func resolveSkillsDir(workDir, provider string) (string, error) {
	var skillsDir string
	switch provider {
	case "claude":
		// Claude Code natively discovers skills from .claude/skills/ in the workdir.
		skillsDir = filepath.Join(workDir, ".claude", "skills")
	case "opencode":
		// OpenCode natively discovers skills from .config/opencode/skills/ in the workdir.
		skillsDir = filepath.Join(workDir, ".config", "opencode", "skills")
	default:
		// Fallback: write to .agent_context/skills/ (referenced by meta config).
		skillsDir = filepath.Join(workDir, ".agent_context", "skills")
	}
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		return "", err
	}
	return skillsDir, nil
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

// sanitizeSkillName converts a skill name to a safe directory name.
func sanitizeSkillName(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = nonAlphaNum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		s = "skill"
	}
	return s
}

// writeSkillFiles writes skill directories into the given parent directory.
// Each skill gets its own subdirectory containing SKILL.md and supporting files.
func writeSkillFiles(skillsDir string, skills []SkillContextForEnv) error {
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		return fmt.Errorf("create skills dir: %w", err)
	}

	for _, skill := range skills {
		dir := filepath.Join(skillsDir, sanitizeSkillName(skill.Name))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}

		// Write main SKILL.md
		if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(skill.Content), 0o644); err != nil {
			return err
		}

		// Write supporting files
		for _, f := range skill.Files {
			fpath := filepath.Join(dir, f.Path)
			if err := os.MkdirAll(filepath.Dir(fpath), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(fpath, []byte(f.Content), 0o644); err != nil {
				return err
			}
		}
	}

	return nil
}

// renderIssueContext builds the markdown content for issue_context.md.
func renderIssueContext(provider string, ctx TaskContextForEnv) string {
	var b strings.Builder

	b.WriteString("# Task Assignment\n\n")
	fmt.Fprintf(&b, "**Issue ID:** %s\n\n", ctx.IssueID)

	if ctx.TriggerCommentID != "" {
		b.WriteString("**Trigger:** Comment Reply\n")
		b.WriteString("**Triggering comment ID:** `" + ctx.TriggerCommentID + "`\n\n")
	} else {
		b.WriteString("**Trigger:** New Assignment\n\n")
	}

	if ctx.TriggerCommentContent != "" {
		b.WriteString("## Triggering Comment\n\n")
		b.WriteString("> " + strings.ReplaceAll(ctx.TriggerCommentContent, "\n", "\n> ") + "\n\n")
	}

	b.WriteString("## Local Context\n\n")
	b.WriteString("Read the injected files in `.agent_context/` before making any Multica CLI read call:\n\n")
	b.WriteString("- `issue_context.md` — concise assignment summary\n")
	b.WriteString("- `issue.json` — full issue payload fetched by the daemon\n")
	b.WriteString("- `comments.json` — current issue comments fetched by the daemon\n")
	b.WriteString("- `repos.json` — repositories available for checkout\n")
	b.WriteString("- `task.json` — task/trigger metadata\n\n")
	b.WriteString("Use the `multica` CLI only when you need to write back to the platform or explicitly refresh data.\n\n")

	if len(ctx.AgentSkills) > 0 {
		b.WriteString("## Agent Skills\n\n")
		b.WriteString("The following skills are available to you:\n\n")
		for _, skill := range ctx.AgentSkills {
			fmt.Fprintf(&b, "- **%s**\n", skill.Name)
		}
		b.WriteString("\n")
	}

	return b.String()
}
