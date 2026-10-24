package harness

import "strings"

// pi (1.1.0's --help): headless with -p, JSON events. No permission system: a rule whose access
// is only read gets the read tools and bash (no edit, write); the prompt states the access.
// unenforced. What it loads (02d, its docs/cli.md): no extensions, MCP, skills or prompt
// templates, and no trust-gated project-local files; context files (AGENTS.md) stay.
func pi(s Spec) (Cmd, error) {
	argv := []string{"pi", "-p", "--mode", "json", "--no-extensions", "--no-mcp", "--no-skills", "--no-prompt-templates", "--no-approve"}
	if s.Model != "" {
		argv = append(argv, "--model", s.Model)
	}
	if onlyRead(s) {
		argv = append(argv, "--tools", "read,grep,find,ls,bash")
	}
	argv = append(argv, s.Args...)
	argv = append(argv, "--", s.Prompt)
	return Cmd{Argv: argv}, nil
}

// a rule with no job and no write path: the run writes nothing but its TMPDIR
func onlyRead(s Spec) bool {
	for _, w := range s.Write {
		if w != "" && !strings.HasSuffix(w, "/tmp") {
			return false
		}
	}
	return true
}

func piSummary(out []byte) string { return lastText(out) }
