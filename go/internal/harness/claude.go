package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

// claude (Claude Code, checked against 2.1.296's --help): headless with -p, the stream as JSON
// lines, permission mode dontAsk (what is not allowed is denied, never asked), a per-run settings
// file (permissions and the sandbox), --add-dir for every path beyond the cwd. The prompt goes
// after --: --add-dir takes several values.
//
// What it loads (02d): --setting-sources= (empty: no user, project or local settings file; only
// --settings and managed policy. 2.1.296's own --restricted sets the same empty list, and it
// forwards the flag to its children as --setting-sources=<list>) and --strict-mcp-config with no
// --mcp-config (no MCP server at all, the user's in ~/.claude.json included). So no user or
// project hooks, env, plugins or MCP. The person's own CLAUDE.md and rules are excluded by
// claudeMdExcludes; the repos' CLAUDE.md and AGENTS.md still load. User skills and subagents
// (~/.claude/skills, ~/.claude/agents) still load: definitions the agent may use, not code that
// runs by itself. ANFSD_CLAUDE_BARE=on adds --bare (no OAuth or keychain: ANTHROPIC_API_KEY or an
// apiKeyHelper only).
//
// The sandboxed shell: it writes only the job, its TMPDIR, access:'s write paths and the run's
// spool (not the file tools: Edit/Write allows leave the spool out); reads none of Secrets
// (denyRead); has claude's key unset (credentials.envVars deny); and has no network (dontAsk
// refuses every host outside allowedDomains, which is empty). CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1
// strips the credential from every subprocess, hooks included.
//
// Under -p a settings file that fails validation is silently ignored (its --help says so), so
// anfsd writes it from a struct and the tests check its shape; sandbox.failIfUnavailable makes a
// missing sandbox backend a failure instead of an unconfined shell, and allowUnsandboxedCommands
// false keeps a command from asking its way out of the sandbox.
func claude(s Spec) (Cmd, error) {
	settings := filepath.Join(s.RunDir, "claude.json")
	write, read := uniq(s.Write), uniq(append(append([]string{}, s.Read...), s.Write...))
	var allow, deny []string
	for _, p := range read {
		allow = append(allow, "Read("+rulePath(p)+")")
	}
	for _, p := range write {
		allow = append(allow, "Edit("+rulePath(p)+")", "Write("+rulePath(p)+")")
	}
	allow = append(allow, "Bash")
	for _, d := range Denied {
		deny = append(deny, "Bash("+d+")", "Bash("+d+":*)")
	}
	cfg := map[string]any{
		"permissions": map[string]any{
			"defaultMode": "dontAsk",
			"allow":       allow,
			"deny":        deny,
		},
		"sandbox": map[string]any{
			"enabled":                  true,
			"failIfUnavailable":        true,
			"allowUnsandboxedCommands": false,
			"filesystem": map[string]any{
				"allowWrite": uniq(append(append([]string{}, write...), s.Spool)),
				"denyRead":   uniq(s.Secrets),
			},
			"credentials": map[string]any{
				"envVars": []map[string]string{
					{"name": "ANTHROPIC_API_KEY", "mode": "deny"},
					{"name": "CLAUDE_CODE_OAUTH_TOKEN", "mode": "deny"},
				},
			},
		},
	}
	if s.Home != "" {
		cfg["claudeMdExcludes"] = []string{s.Home + "/.claude/CLAUDE.md", s.Home + "/.claude/rules/**"}
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return Cmd{}, err
	}
	argv := []string{"claude", "-p", "--output-format", "stream-json", "--verbose",
		"--permission-mode", "dontAsk", "--settings", settings, "--setting-sources=", "--strict-mcp-config"}
	if s.Bare {
		argv = append(argv, "--bare")
	}
	for _, p := range uniq(append(append([]string{}, s.Read...), s.Write...)) {
		if p != s.Cwd {
			argv = append(argv, "--add-dir", p)
		}
	}
	if s.Model != "" {
		argv = append(argv, "--model", s.Model)
	}
	if s.BudgetUSD > 0 {
		argv = append(argv, "--max-budget-usd", budget(s.BudgetUSD))
	}
	argv = append(argv, "--name", s.Name)
	argv = append(argv, s.Args...)
	argv = append(argv, "--", s.Prompt)
	return Cmd{Argv: argv, Env: []string{"CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1"},
		Files: map[string][]byte{"claude.json": append(b, '\n')}}, nil
}

// An absolute path in a permission rule is //path; everything under it is /**
func rulePath(p string) string { return "/" + strings.TrimSuffix(p, "/") + "/**" }

// The summary is the stream's result event's result
func claudeSummary(out []byte) string {
	res := ""
	for _, l := range bytes.Split(out, []byte("\n")) {
		l = bytes.TrimSpace(l)
		if !bytes.HasPrefix(l, []byte("{")) || !bytes.Contains(l, []byte(`"result"`)) {
			continue
		}
		var m struct {
			Type   string `json:"type"`
			Result string `json:"result"`
		}
		if json.Unmarshal(l, &m) == nil && m.Type == "result" {
			res = m.Result
		}
	}
	if res == "" {
		return lastText(out)
	}
	return res
}

// One stream event as text: the assistant's words, a tool call in a line, the result
func claudeRender(m map[string]any) string {
	t, _ := m["type"].(string)
	switch t {
	case "system":
		if st, _ := m["subtype"].(string); st == "init" {
			model, _ := m["model"].(string)
			return "[session " + fmt.Sprint(m["session_id"]) + ", " + model + "]"
		}
	case "assistant":
		msg, _ := m["message"].(map[string]any)
		var parts []string
		if c, ok := msg["content"].([]any); ok {
			for _, e := range c {
				em, _ := e.(map[string]any)
				switch em["type"] {
				case "text":
					s, _ := em["text"].(string)
					parts = append(parts, s)
				case "tool_use":
					in, _ := json.Marshal(em["input"])
					parts = append(parts, fmt.Sprintf("> %v %s", em["name"], Cut(string(in), 200)))
				}
			}
		}
		return strings.Join(parts, "\n")
	case "result":
		r, _ := m["result"].(string)
		return fmt.Sprintf("[result: %v, %v turns, $%v]\n%s", m["subtype"], m["num_turns"], m["total_cost_usd"], r)
	}
	return ""
}
