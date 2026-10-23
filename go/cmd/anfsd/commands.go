package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/config"
	"github.com/anfs-io/runtime/go/internal/dispatch"
	"github.com/anfs-io/runtime/go/internal/harness"
	"github.com/anfs-io/runtime/go/internal/space"
	"github.com/anfs-io/runtime/go/internal/supervise"
)

// args: the options (each takes a value unless it is a flag), anywhere on the line, and the rest
type opts struct {
	flags  map[string]bool
	values map[string][]string
	rest   []string
}

func parseOpts(args []string, flags, values []string) (*opts, error) {
	o := &opts{flags: map[string]bool{}, values: map[string][]string{}}
	isFlag, isValue := map[string]bool{}, map[string]bool{}
	for _, f := range flags {
		isFlag[f] = true
	}
	for _, v := range values {
		isValue[v] = true
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		name, val, hasVal := strings.Cut(a, "=")
		switch {
		case a == "-" || !strings.HasPrefix(a, "-"):
			o.rest = append(o.rest, a)
		case isFlag[a]:
			o.flags[a] = true
		case isValue[name] && hasVal:
			o.values[name] = append(o.values[name], val)
		case isValue[a]:
			if i+1 >= len(args) {
				return nil, fmt.Errorf("%s needs a value", a)
			}
			i++
			o.values[a] = append(o.values[a], args[i])
		default:
			return nil, fmt.Errorf("unknown option %s", a)
		}
	}
	return o, nil
}

func (o *opts) value(n string) string {
	if v := o.values[n]; len(v) > 0 {
		return v[len(v)-1]
	}
	return ""
}

// loadSpaces: the files named (each its own space), or every installed space (wsm ls --json). A
// file's space path is what wsm calls the space with its id, else its directory's name.
func loadSpaces(files []string) ([]*space.Space, error) {
	env := validationEnv()
	if len(files) == 0 {
		return space.Discover(context.Background(), "wsm", env)
	}
	paths := map[string]string{}
	if installed, err := space.Discover(context.Background(), "wsm", env); err == nil {
		for _, s := range installed {
			if s.ID != "" {
				paths[s.ID] = s.Path
			}
		}
	}
	var out []*space.Space
	for _, f := range files {
		abs, err := filepath.Abs(f)
		if err != nil {
			return nil, err
		}
		dir := filepath.Dir(abs)
		s := space.Load(abs, filepath.Base(dir), dir, env)
		if p := paths[s.ID]; p != "" {
			s.Path = p
		}
		out = append(out, s)
	}
	return out, nil
}

// ruleRow is a rule as --json gives it (anfs agents rules reads it)
type ruleRow struct {
	Rule     string   `json:"rule"`
	Status   string   `json:"status"`
	Space    string   `json:"space"`
	SpaceID  string   `json:"space_id"`
	On       string   `json:"on"`
	As       string   `json:"as"`
	Harness  string   `json:"harness"`
	Job      string   `json:"job"`
	Enabled  bool     `json:"enabled"`
	Reason   string   `json:"reason"`
	Warnings []string `json:"warnings"`
	File     string   `json:"file"`
}

func onString(r *space.Rule) string {
	parts := []string{r.On.Type}
	if r.On.Subject != "" {
		parts = append(parts, "subject="+r.On.Subject)
	}
	keys := make([]string, 0, len(r.On.Data))
	for k := range r.On.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, k+"="+r.On.Data[k])
	}
	if r.On.Space != "" {
		parts = append(parts, "space="+r.On.Space)
	}
	return strings.Join(parts, " ")
}

func rowOf(r *space.Rule) ruleRow {
	row := ruleRow{Rule: r.Name, Status: r.Status, Space: r.Space.Path, SpaceID: r.Space.ID, On: onString(r),
		As: r.As, Harness: r.Harness, Enabled: r.Enabled, Reason: r.Reason, Warnings: r.Warnings, File: r.Space.File}
	if row.Warnings == nil {
		row.Warnings = []string{}
	}
	if r.Job != nil {
		row.Job = r.Job.Name
	}
	return row
}

func rulesCmd(args []string, stdout, stderr io.Writer) int {
	o, err := parseOpts(args, []string{"--json"}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "anfsd rules: %v\n", err)
		return 64
	}
	spaces, err := loadSpaces(o.rest)
	if err != nil {
		fmt.Fprintf(stderr, "anfsd rules: %v\n", err)
		return 1
	}
	rows := []ruleRow{}
	for _, s := range spaces {
		if s.Err != nil && len(s.Rules) == 0 && len(o.rest) > 0 {
			fmt.Fprintf(stderr, "anfsd rules: %s: %v\n", s.File, s.Err)
		}
		for _, r := range s.Rules {
			rows = append(rows, rowOf(r))
		}
	}
	if o.flags["--json"] {
		b, _ := json.Marshal(rows)
		fmt.Fprintln(stdout, string(b))
		return 0
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RULE\tSTATUS\tSPACE\tON\tAS\tHARNESS")
	for _, r := range rows {
		st := r.Status
		if r.Reason != "" {
			st += ": " + r.Reason
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", r.Rule, st, r.Space, r.On, r.As, dash(r.Harness))
	}
	tw.Flush()
	return 0
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// matchRow is one rule's answer to an event
type matchRow struct {
	Rule    string   `json:"rule"`
	Status  string   `json:"status"` // the rule's
	Space   string   `json:"space"`
	Result  string   `json:"result"` // no match, submit, reject, duplicate, disabled, invalid
	Why     string   `json:"why"`
	Job     string   `json:"job"`
	Repos   []string `json:"repos"`
	Cwd     string   `json:"cwd"`
	Harness string   `json:"harness"`
	Argv    []string `json:"argv"`
	Env     []string `json:"env"`
	Prompt  string   `json:"prompt"`
}

// the event to match: a line from a file or stdin, or one built from the options
func matchEvent(o *opts, stdin io.Reader, spaces []*space.Space) ([]byte, cloudevent.Event, error) {
	if f := o.value("--event"); f != "" {
		var b []byte
		var err error
		if f == "-" {
			b, err = io.ReadAll(stdin)
		} else {
			b, err = os.ReadFile(f)
		}
		if err != nil {
			return nil, cloudevent.Event{}, err
		}
		var last []byte
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(make([]byte, 0, 8192), 1<<20)
		for sc.Scan() {
			if l := bytes.TrimSpace(sc.Bytes()); len(l) > 0 {
				last = append([]byte(nil), l...)
			}
		}
		ev, err := cloudevent.Parse(last)
		if err != nil {
			return nil, ev, fmt.Errorf("--event %s: not an event: %v", f, err)
		}
		return last, ev, nil
	}
	typ := strings.TrimPrefix(o.value("--type"), "io.anfs.")
	if typ == "" {
		return nil, cloudevent.Event{}, fmt.Errorf("--event FILE, or --type to build one")
	}
	data := map[string]string{}
	for _, kv := range o.values["--data"] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, cloudevent.Event{}, fmt.Errorf("--data %s: KEY=VALUE", kv)
		}
		data[k] = v
	}
	scope, spath := "host", ""
	if sp := o.value("--space"); sp != "" && sp != "host" {
		for _, s := range spaces {
			if s.ID != "" && (s.ID == sp || s.Path == sp) {
				scope, spath = s.ID, s.Path
			}
		}
		if spath == "" {
			return nil, cloudevent.Event{}, fmt.Errorf("--space %s: no space with that path or id among the rules' spaces", sp)
		}
	}
	actor := "shell:" + os.Getenv("USER")
	if a := o.value("--actor"); a != "" {
		actor = a
	}
	line, ev, err := cloudevent.Line(cloudevent.New{Scope: scope, Type: typ, Subject: o.value("--subject"),
		Actor: actor, SpacePath: spath, Data: data}, time.Now())
	if err != nil {
		return nil, ev, err
	}
	return bytes.TrimSpace(line), ev, nil
}

func matchCmd(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	o, err := parseOpts(args, []string{"--json"}, []string{"--event", "--type", "--subject", "--data", "--space", "--actor"})
	if err != nil {
		fmt.Fprintf(stderr, "anfsd match: %v\n", err)
		return 64
	}
	spaces, err := loadSpaces(o.rest)
	if err != nil {
		fmt.Fprintf(stderr, "anfsd match: %v\n", err)
		return 1
	}
	line, ev, err := matchEvent(o, stdin, spaces)
	if err != nil {
		fmt.Fprintf(stderr, "anfsd match: %v\n", err)
		return 64
	}
	// the guards read the run records as they are; nothing is written, nothing starts
	cfg, err := config.LoadLocal()
	if err != nil {
		fmt.Fprintf(stderr, "anfsd match: %v\n", err)
		return 1
	}
	sup := &supervise.Supervisor{Store: supervise.Store{Dir: cfg.Paths.Runs}, MaxRuns: cfg.Settings.MaxRuns}
	if err := sup.Open(); err != nil {
		fmt.Fprintf(stderr, "anfsd match: the run records: %v\n", err)
		return 1
	}
	d := &dispatch.Dispatcher{Sup: sup, Load: func(context.Context) ([]*space.Space, error) { return spaces, nil }}
	d.Reload(context.Background())
	rows := []matchRow{}
	for _, s := range spaces {
		for _, r := range s.Rules {
			row := matchRow{Rule: r.Name, Status: r.Status, Space: s.Path, Harness: r.Harness, Repos: []string{}, Argv: []string{}, Env: []string{}}
			if r.Status == space.StatusInvalid {
				row.Result, row.Why = "invalid", r.Reason
				rows = append(rows, row)
				continue
			}
			dec := d.Decide(r, ev, line, true)
			switch {
			case !dec.Match:
				row.Result, row.Why = "no match", dec.Matched
			case dec.Action == "duplicate":
				row.Result, row.Why = "duplicate", "this rule already has a run for this event"
			case dec.Action == "rejected":
				row.Result, row.Why = "reject", dec.Reason+": "+dec.Detail
			default:
				row.Result = "submit"
				if dec.Action == "disabled" {
					row.Result, row.Why = "disabled", "would submit, but the rule has enabled: false"
				}
			}
			if dec.Record != nil && (row.Result == "submit" || row.Result == "disabled" || row.Result == "reject") {
				fillPlan(&row, dec, line)
			}
			rows = append(rows, row)
		}
	}
	if o.flags["--json"] {
		b, _ := json.Marshal(rows)
		fmt.Fprintln(stdout, string(b))
		return 0
	}
	fmt.Fprintf(stdout, "event: %s %s in %s by %s\n", ev.Short(), dash(ev.Subject), dash(d.SpacePath(ev.Scope)+map[bool]string{true: "", false: " (" + ev.Scope + ")"}[d.SpacePath(ev.Scope) != ""]), ev.Actor)
	for _, row := range rows {
		fmt.Fprintf(stdout, "\n%s/%s (%s): %s", row.Space, row.Rule, row.Status, row.Result)
		if row.Why != "" {
			fmt.Fprintf(stdout, ": %s", row.Why)
		}
		fmt.Fprintln(stdout)
		if len(row.Argv) == 0 {
			continue
		}
		job := "none (the space's directory, read-only)"
		if row.Job != "" {
			job = "anfs work new " + row.Job + " " + strings.Join(row.Repos, " ")
		}
		fmt.Fprintf(stdout, "  job:     %s\n  cwd:     %s\n  env:     %s\n  command: %s\n", job, row.Cwd, strings.Join(row.Env, " "), shellJoin(row.Argv))
	}
	return 0
}

// fillPlan: the job, the cwd, the harness's command line as the supervisor would build it (the
// job's directory as anfs work names it; the prompt cut)
func fillPlan(row *matchRow, dec dispatch.Decision, line []byte) {
	r := dec.Record
	row.Job, row.Repos, row.Prompt = r.Job, append([]string{}, r.Repos...), dec.Prompt
	cwd := r.SpaceDir
	tmp := "<run dir>/tmp"
	if r.Job != "" {
		home := os.Getenv("ANFS_WORK_HOME")
		if home == "" {
			home = filepath.Join(os.Getenv("HOME"), "work")
		}
		cwd = filepath.Join(home, r.Job)
		tmp = "<the job's TMPDIR>"
	}
	row.Cwd = cwd
	write := append([]string{tmp}, r.Write...)
	if r.Job != "" {
		write = append([]string{cwd}, write...)
	}
	read := append([]string{r.SpaceDir}, r.Read...)
	spec := harness.Spec{Harness: r.Harness, Model: r.Model, BudgetUSD: r.BudgetUSD, Args: r.Args,
		Name: r.Rule + ":" + r.Last8(), Cwd: cwd, RunDir: "<run dir>", Timeout: r.TimeoutDur(), Write: write, Read: read,
		Prompt: dec.Prompt + supervise.Footer(r, cwd, line, write, read)}
	cmd, err := harness.Build(spec)
	if err != nil {
		row.Why = err.Error()
		return
	}
	argv := append([]string{}, cmd.Argv...)
	for i, a := range argv {
		if a == spec.Prompt {
			argv[i] = firstN(strings.Join(strings.Fields(dec.Prompt), " "), 72) + " (+ anfsd's footer)"
		}
	}
	row.Argv = argv
	row.Env = append(cmd.Env[:0:0], "ANFS_ACTOR="+r.Harness+":"+r.Last8(), "ANFS_AS="+r.Address, "ANFS_RUN="+r.ID, "ANFS_EVENT=<run dir>/event.json")
	for _, e := range cmd.Env {
		row.Env = append(row.Env, firstN(e, 60))
	}
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func shellJoin(argv []string) string {
	out := make([]string, len(argv))
	for i, a := range argv {
		if a != "" && !strings.ContainsAny(a, " \t\n'\"$`\\*?[]{}()<>|&;#~") {
			out[i] = a
			continue
		}
		out[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	return strings.Join(out, " ")
}

func renderCmd(h string, stdin io.Reader, stdout io.Writer) int {
	sc := bufio.NewScanner(stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	w := bufio.NewWriter(stdout)
	defer w.Flush()
	for sc.Scan() {
		if t := harness.Render(h, sc.Bytes()); t != "" {
			fmt.Fprintln(w, t)
			w.Flush()
		}
	}
	return 0
}
