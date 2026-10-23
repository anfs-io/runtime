// Package harness builds the command line, the extra environment and the files of one run for
// each agent CLI anfsd starts (one file each), and reads a run's output for its summary. Nothing
// here starts anything: the supervisor does.
//
// What a dispatched agent may reach is handed over as each CLI allows (anfs-io/docs,
// units/agents/02-daemon/plan.md, 5): claude enforces read and write per path and confines Bash
// writes with its sandbox; opencode has one permission for outside the project; antigravity a
// sandbox and directories, no read/write split; pi nothing. anfsd never passes a flag that skips
// permissions.
package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Denied are the commands a dispatched agent may not run before 04's approvals (merge, install,
// push and close are the person's): denied to every harness that can deny, and said in every
// footer.
var Denied = []string{
	"git push",
	"ppm install", "ppm remove",
	"anfs install", "anfs remove", "anfs implode", "anfs src update",
	"wsm install", "wsm remove",
	"anfs report close",
	"gh", "sudo", "launchctl", "systemctl",
	"anfs bus", "anfs agents",
}

// SummaryMax is the most bytes of the final answer run.completed carries.
const SummaryMax = 1500

// Spec is one run, as the supervisor knows it.
type Spec struct {
	Harness   string // claude, opencode, pi, antigravity
	Model     string
	BudgetUSD float64
	Name      string // <rule>:<run id's last 8>
	Prompt    string
	Cwd       string        // the job, or the space's directory
	RunDir    string        // where the run's files go
	Timeout   time.Duration // the rule's
	Write     []string      // absolute: the job, its TMPDIR, access: write paths
	Read      []string      // absolute: the space's directory, access: read paths
	Args      []string      // the rule's args:
}

// Cmd is what to run: argv (argv[0] the program), more environment, files to write in RunDir.
type Cmd struct {
	Argv  []string
	Env   []string
	Files map[string][]byte
}

// Build is the command for the spec's harness.
func Build(s Spec) (Cmd, error) {
	switch s.Harness {
	case "claude":
		return claude(s)
	case "opencode":
		return opencode(s)
	case "pi":
		return pi(s)
	case "antigravity":
		return antigravity(s)
	}
	return Cmd{}, fmt.Errorf("no harness %q", s.Harness)
}

// Summary is the final answer in a run's output, cut to SummaryMax bytes.
func Summary(harness string, out []byte) string {
	var s string
	switch harness {
	case "claude":
		s = claudeSummary(out)
	case "opencode":
		s = opencodeSummary(out)
	case "pi":
		s = piSummary(out)
	default:
		s = string(out)
	}
	return Cut(strings.TrimSpace(s), SummaryMax)
}

// Cut keeps the last n bytes of s (the end of an answer is its summary), on a rune boundary.
func Cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[len(s)-n:]
	for i := 0; i < len(s) && i < 4; i++ {
		if s[i]&0xC0 != 0x80 {
			return s[i:]
		}
	}
	return s
}

// Render turns a run's output into text for a person (anfs agents logs): claude's stream-json,
// opencode's and pi's JSON events; anything that is not JSON as it is.
func Render(harness string, line []byte) string {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return string(line)
	}
	var m map[string]any
	if json.Unmarshal(line, &m) != nil {
		return string(line)
	}
	switch harness {
	case "claude":
		return claudeRender(m)
	}
	if t := textOf(m); t != "" {
		return t
	}
	return ""
}

// textOf finds an assistant's text in the shapes opencode and pi print
func textOf(m map[string]any) string {
	if p, ok := m["part"].(map[string]any); ok {
		if t, _ := p["type"].(string); t == "text" {
			s, _ := p["text"].(string)
			return s
		}
	}
	if msg, ok := m["message"].(map[string]any); ok {
		if r, _ := msg["role"].(string); r == "assistant" {
			return contentText(msg["content"])
		}
	}
	return ""
}

func contentText(c any) string {
	switch v := c.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, e := range v {
			if em, ok := e.(map[string]any); ok {
				if t, _ := em["type"].(string); t == "text" {
					s, _ := em["text"].(string)
					if b.Len() > 0 {
						b.WriteString("\n")
					}
					b.WriteString(s)
				}
			}
		}
		return b.String()
	}
	return ""
}

// lastText is the last assistant text among JSON lines
func lastText(out []byte) string {
	last := ""
	for _, l := range bytes.Split(out, []byte("\n")) {
		l = bytes.TrimSpace(l)
		if len(l) == 0 || l[0] != '{' {
			continue
		}
		var m map[string]any
		if json.Unmarshal(l, &m) != nil {
			continue
		}
		if t := textOf(m); strings.TrimSpace(t) != "" {
			last = t
		}
	}
	return last
}

// writable and readable paths, deduplicated, sorted (golden files stay stable)
func uniq(ps []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range ps {
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func budget(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", f), "0"), ".")
}
