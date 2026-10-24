package harness

import (
	"encoding/json"
	"strings"
)

// opencode (1.18.35's `opencode run --help`): run in the cwd with JSON events; access through
// OPENCODE_CONFIG_CONTENT: permission.external_directory (each path allow, everything else deny:
// one permission for read and write) and permission.bash (the denials deny, the rest allow). Its
// own docs call bash's path checks advisory: partial.
func opencode(s Spec) (Cmd, error) {
	ext := map[string]string{"*": "deny"}
	for _, p := range uniq(append(append([]string{s.Spool}, s.Read...), s.Write...)) {
		ext[strings.TrimSuffix(p, "/")+"/**"] = "allow"
	}
	bash := map[string]string{"*": "allow"}
	for _, d := range Denied {
		bash[d] = "deny"
		bash[d+" *"] = "deny"
	}
	cfg, err := json.Marshal(map[string]any{"permission": map[string]any{"external_directory": ext, "bash": bash}})
	if err != nil {
		return Cmd{}, err
	}
	argv := []string{"opencode", "run", "--format", "json", "--dir", s.Cwd}
	if s.Model != "" {
		argv = append(argv, "--model", s.Model)
	}
	argv = append(argv, s.Args...)
	argv = append(argv, s.Prompt) // its help shows no --: the message is the positional
	return Cmd{Argv: argv, Env: []string{"OPENCODE_CONFIG_CONTENT=" + string(cfg)}}, nil
}

func opencodeSummary(out []byte) string { return lastText(out) }
