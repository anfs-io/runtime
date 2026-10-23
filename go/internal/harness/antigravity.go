package harness

import "fmt"

// antigravity (agy 1.3.3, macOS): headless with -p <prompt>, its sandbox, --add-dir per path,
// --print-timeout the rule's timeout. No read/write split: partial. Its --help and --version hang
// (2026-10-23, here), so these flags are from its binary's strings and changelog, not its help.
// An expired --print-timeout exits 0 with the partial answer: anfsd's own timeout (the same) is
// what fails the run.
func antigravity(s Spec) (Cmd, error) {
	argv := []string{"agy", "-p", s.Prompt, "--sandbox"}
	for _, p := range uniq(append(append([]string{}, s.Read...), s.Write...)) {
		if p != s.Cwd {
			argv = append(argv, "--add-dir", p)
		}
	}
	if s.Model != "" {
		argv = append(argv, "--model", s.Model)
	}
	argv = append(argv, "--print-timeout", fmt.Sprintf("%ds", int(s.Timeout.Seconds())))
	argv = append(argv, s.Args...)
	return Cmd{Argv: argv}, nil
}
