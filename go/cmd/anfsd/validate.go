package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
)

var (
	spoolFileRE = regexp.MustCompile(`^([0-9]{4}-[0-9]{2})(\.bus)?\.jsonl$`)
	scopeDirRE  = regexp.MustCompile(`^(host|[a-z0-9]{10})$`)
)

// validateCmd is anfsd validate [FILE...|-]: every line of each file (stdin for -) checked
// against the profile (anfs-io/events), one line per problem, FILE:LINE RULE message. A file in
// the spool's layout (<scope>/<YYYY-MM>.jsonl, the recorder's <YYYY-MM>.bus.jsonl) is also checked
// for its lines' scope (spool.scope) and month (spool.month). A valid line not in the canonical
// form is said (noncanonical) and is no failure. The last line counts. Exit 1 when a line is
// invalid or a file cannot be read.
func validateCmd(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, "anfsd validate: FILE... (or - for stdin)\n")
		return 64
	}
	for _, a := range args {
		if a != "-" && len(a) > 1 && a[0] == '-' {
			fmt.Fprintf(stderr, "anfsd validate: unknown option %s\n", a)
			return 64
		}
	}
	events, invalid, noncanon, files, failed := 0, 0, 0, 0, false
	for _, name := range args {
		var r io.Reader
		if name == "-" {
			r = stdin
		} else {
			f, err := os.Open(name)
			if err != nil {
				fmt.Fprintf(stdout, "%s: cannot read: %v\n", name, errors.Unwrap(err))
				failed = true
				continue
			}
			r = f
		}
		files++
		scope, month := "", ""
		if m := spoolFileRE.FindStringSubmatch(filepath.Base(name)); m != nil && name != "-" {
			if d := filepath.Base(filepath.Dir(name)); scopeDirRE.MatchString(d) {
				scope, month = d, m[1]
			}
		}
		br := bufio.NewReader(r)
		for n := 1; ; n++ {
			line, err := br.ReadBytes('\n')
			if len(line) == 0 && err != nil {
				if err != io.EOF {
					fmt.Fprintf(stdout, "%s: cannot read: %v\n", name, err)
					failed = true
				}
				break
			}
			events++
			say := func(rule, msg string) { fmt.Fprintf(stdout, "%s:%d %s %s\n", name, n, rule, msg) }
			ev, perr := cloudevent.Parse(line)
			switch {
			case perr != nil:
				invalid++
				var e *cloudevent.Error
				if errors.As(perr, &e) {
					say(e.Rule, e.Msg)
				} else {
					say("json.parse", perr.Error())
				}
			case scope != "" && ev.Scope != scope:
				invalid++
				say("spool.scope", fmt.Sprintf("an event of %s (its source %s) in %s's directory", ev.Scope, ev.Source, scope))
			case month != "" && ev.Month() != month:
				invalid++
				say("spool.month", fmt.Sprintf("an event of %s (its time) in %s's file", ev.Month(), month))
			case cloudevent.Noncanonical(line):
				noncanon++
				say("noncanonical", "valid, but not the spool's canonical form (compact, data last, \"anfsv\":1): the bash reader needs it")
			}
		}
		if c, ok := r.(io.Closer); ok && name != "-" {
			c.Close()
		}
	}
	fmt.Fprintf(stdout, "%d events, %d invalid, %d noncanonical (%d files)\n", events, invalid, noncanon, files)
	if invalid > 0 || failed {
		return 1
	}
	return 0
}
