package space

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
)

// every program is found but the ones named missing
func env(t *testing.T, missing ...string) Env {
	home := t.TempDir()
	return Env{
		Home:   home,
		Getenv: func(k string) string { return map[string]string{"NOTES": "/srv/notes", "HOME": home}[k] },
		LookPath: func(p string) (string, error) {
			for _, m := range missing {
				if m == p {
					return "", os.ErrNotExist
				}
			}
			return "/bin/" + p, nil
		},
	}
}

const full = `id: abcde23456
---
repositories: []
access:
  - {path: ~/.local/share/anfs/sources, mode: read}
  - {path: $NOTES/anfs, mode: write}
  - {path: shared, mode: read}
agents:
  maintainer:
    harness: claude
    model: opus
    concurrency: 2
    queue: 3
    budget_usd: 5
dispatch:
  - name: fix-core-report
    on:
      type: report.filed
      subject: "r-*"
      data: {to: file, package: "core/*"}
    as: maintainer
    job: {name: "fix-{{subject}}", repos: [core]}
    prompt: |
      Fix {{subject}} ({{data.title}}) in {{job}}, run {{run}}, from {{space}} by {{actor}} at {{time}}: {{id}} {{type}}
    timeout: 2h
    rate: 5/m
    stale: 1h
    retries: 1
    args: [--effort, high]
    overflow: reject
    hold: 5
`

func TestEveryKey(t *testing.T) {
	e := env(t)
	s := Parse([]byte(full), "anfs-dev", "/spaces/anfs-dev", e)
	if s.Err != nil || s.ID != "abcde23456" {
		t.Fatalf("space: %v %q", s.Err, s.ID)
	}
	want := []Access{{e.Home + "/.local/share/anfs/sources", "read"}, {"/srv/notes/anfs", "write"}, {"/spaces/anfs-dev/shared", "read"}}
	if len(s.Access) != 3 {
		t.Fatalf("access %v", s.Access)
	}
	for i, a := range want {
		if s.Access[i] != a {
			t.Errorf("access[%d] = %v, want %v", i, s.Access[i], a)
		}
	}
	r := s.Rules[0]
	if r.Status != StatusOK || r.Reason != "" {
		t.Fatalf("status %s: %s", r.Status, r.Reason)
	}
	if r.Harness != "claude" || r.Model != "opus" || r.BudgetUSD != 5 || r.Role.Concurrency != 2 || r.Role.Queue != 3 {
		t.Errorf("role: %+v %+v", r, r.Role)
	}
	if r.Timeout != 2*time.Hour || r.Stale != time.Hour || r.Rate != (Rate{5, time.Minute}) || r.Retries != 1 || r.Overflow != "reject" || r.Hold != 5 {
		t.Errorf("limits: %v %v %v %v", r.Timeout, r.Stale, r.Rate, r.Retries)
	}
	if r.Job == nil || r.Job.Name != "fix-{{subject}}" || r.Job.Repos[0] != "core" || r.Address() != "anfs-dev/maintainer" {
		t.Errorf("job %+v address %s", r.Job, r.Address())
	}
	if len(r.Args) != 2 || r.Key() != "abcde23456/fix-core-report" || !r.Loaded() {
		t.Errorf("args %v key %s", r.Args, r.Key())
	}
}

func TestDefaults(t *testing.T) {
	s := Parse([]byte(`id: abcde23456
agents: {m: {harness: claude}}
dispatch:
  - {name: r, on: {type: report.filed}, as: m, prompt: hi}
`), "x", "/x", env(t))
	r := s.Rules[0]
	if r.Status != StatusOK || r.Timeout != time.Hour || r.Stale != 24*time.Hour || r.Rate != (Rate{20, time.Hour}) ||
		r.Retries != 0 || r.Job != nil || r.Role.Concurrency != 1 || r.Role.Queue != 10 || !r.Enabled || r.Overflow != "delay" || r.Hold != 20 {
		t.Errorf("%s %s %+v %+v", r.Status, r.Reason, r, r.Role)
	}
}

func rule(t *testing.T, extraTop, ruleYAML string, e Env) *Rule {
	t.Helper()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "prompts"), 0o755)
	os.WriteFile(filepath.Join(dir, "prompts/p.md"), []byte("A report: {{subject}}\n"), 0o644)
	y := "id: abcde23456\n" + extraTop + "agents:\n  m: {harness: claude}\n  p: {harness: pi}\n  a: {harness: antigravity}\n  o: {harness: opencode, budget_usd: 3}\ndispatch:\n  - " + ruleYAML + "\n"
	s := Parse([]byte(y), "sp", dir, e)
	if len(s.Rules) == 0 {
		t.Fatalf("no rule from %s", y)
	}
	return s.Rules[0]
}

func TestInvalid(t *testing.T) {
	cases := []struct{ top, rule, reason string }{
		{"", `{on: {type: report.filed}, as: m, prompt: x}`, "name: is required"},
		{"", `{name: Bad_Name, on: {type: report.filed}, as: m, prompt: x}`, "a-z, 0-9 and -"},
		{"", `{name: r, as: m, prompt: x}`, "on: is required"},
		{"", `{name: r, on: {subject: x}, as: m, prompt: x}`, "on.type: is required"},
		{"", `{name: r, on: {type: run.completed}, as: m, prompt: x}`, "anfsd's own run.completed"},
		{"", `{name: r, on: {type: "run.*"}, as: m, prompt: x}`, "a run would start runs"},
		{"", `{name: r, on: {type: "*"}, as: m, prompt: x}`, "a run would start runs"},
		{"", `{name: r, on: {type: report.filed, colour: red}, as: m, prompt: x}`, "unknown key colour"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, colour: red}`, "unknown key colour"},
		{"", `{name: r, on: {type: report.filed}, prompt: x}`, "as: is required"},
		{"", `{name: r, on: {type: report.filed}, as: nobody, prompt: x}`, "not a role of agents:"},
		{"", `{name: r, on: {type: report.filed}, as: m, harness: gpt, prompt: x}`, `harness "gpt" is not one of`},
		{"", `{name: r, on: {type: report.filed}, as: m}`, "prompt: or prompt_file: is required"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, prompt_file: prompts/p.md}`, "exactly one"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt_file: prompts/none.md}`, "prompt_file prompts/none.md: no such file"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: "{{colour}}"}`, "{{colour}} is not a template name"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, job: {name: "{{run}}", repos: [core]}}`, "job.name: {{run}} is not a template name"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, job: {name: x}}`, "job: needs a name and repos"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, job: always}`, "job: is {name, repos} or none"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, timeout: 13h}`, "not within 12h"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, timeout: soon}`, "timeout:"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, rate: lots}`, "starts per window"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, retries: 3}`, "retries: 3 is 0 to 2"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, overflow: drop}`, `overflow: "drop" is delay or reject`},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, hold: 201}`, "hold: 201 is 0 to 200"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, hold: -1}`, "hold: -1 is 0 to 200"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, args: [--dangerously-skip-permissions]}`, "anfsd never skips permissions"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, args: ["--permission-mode=bypassPermissions"]}`, "anfsd never skips permissions"},
		{"", `{name: r, on: {type: report.filed}, as: o, prompt: x, args: [--auto]}`, "anfsd never skips permissions"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, args: ["--mcp-config=/x.json"]}`, "anfsd never skips permissions"},
		{"", `{name: r, on: {type: report.filed}, as: m, prompt: x, args: [--plugin-dir, /p]}`, "anfsd never skips permissions"},
		{"access: [{path: /, mode: read}]\n", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, "not / or $HOME itself"},
		{"access: [{path: \"~\", mode: write}]\n", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, "not / or $HOME itself"},
		{"access: [{path: $HOME/, mode: write}]\n", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, "not / or $HOME itself"},
		{"access: [{path: $NOPE/x, mode: read}]\n", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, "$NOPE is not set"},
		{"access: [{path: /srv, mode: rw}]\n", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, "mode is read or write"},
	}
	for _, c := range cases {
		r := rule(t, c.top, c.rule, env(t))
		if r.Status != StatusInvalid || !strings.Contains(r.Reason, c.reason) {
			t.Errorf("%s %s: %s %q, want invalid %q", c.top, c.rule, r.Status, r.Reason, c.reason)
		}
		if r.Loaded() {
			t.Errorf("%s: loaded", c.rule)
		}
	}
}

func TestMissingPrograms(t *testing.T) {
	r := rule(t, "", `{name: r, on: {type: report.filed}, as: m, prompt: x}`, env(t, "claude"))
	if r.Status != StatusInvalid || !strings.Contains(r.Reason, "harness claude: claude is not on anfsd's PATH") {
		t.Errorf("%s %s", r.Status, r.Reason)
	}
	r = rule(t, "", `{name: r, on: {type: report.filed}, as: a, prompt: x}`, env(t, "agy"))
	if r.Status != StatusInvalid || !strings.Contains(r.Reason, "agy is not on") {
		t.Errorf("%s %s", r.Status, r.Reason)
	}
	r = rule(t, "", `{name: r, on: {type: report.filed}, as: m, prompt: x, job: {name: j, repos: [core]}}`, env(t, "anfs-work"))
	if r.Status != StatusInvalid || !strings.Contains(r.Reason, "job: needs anfs work") {
		t.Errorf("%s %s", r.Status, r.Reason)
	}
	// job: none needs no anfs work
	r = rule(t, "", `{name: r, on: {type: report.filed}, as: m, prompt: x, job: none}`, env(t, "anfs-work"))
	if r.Status != StatusOK || r.Job != nil {
		t.Errorf("%s %s", r.Status, r.Reason)
	}
}

func TestStatuses(t *testing.T) {
	e := env(t)
	for _, c := range []struct{ rule, status string }{
		{`{name: r, on: {type: report.filed}, as: m, prompt_file: prompts/p.md, enabled: false}`, StatusDisabled},
		{`{name: r, on: {type: report.filed}, as: p, prompt: x}`, StatusUnenforced},
		{`{name: r, on: {type: report.filed}, as: a, prompt: x}`, StatusPartial},
		{`{name: r, on: {type: report.filed}, as: m, harness: opencode, prompt: x}`, StatusPartial},
		{`{name: r, on: {type: report.filed}, as: m, prompt_file: prompts/p.md}`, StatusOK},
	} {
		r := rule(t, "", c.rule, e)
		if r.Status != c.status {
			t.Errorf("%s: %s (%s), want %s", c.rule, r.Status, r.Reason, c.status)
		}
	}
	r := rule(t, "", `{name: r, on: {type: report.filed}, as: o, prompt: x}`, e)
	if len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "budget_usd is claude's only") {
		t.Errorf("warnings %v", r.Warnings)
	}
	r = rule(t, "", `{name: r, on: {type: report.filed}, as: m, prompt_file: prompts/p.md}`, e)
	if r.Prompt != "A report: {{subject}}\n" || !strings.HasSuffix(r.PromptFile, "/prompts/p.md") {
		t.Errorf("prompt %q from %s", r.Prompt, r.PromptFile)
	}
}

func TestDuplicateAndOthersLoad(t *testing.T) {
	s := Parse([]byte(`id: abcde23456
agents: {m: {harness: claude}}
dispatch:
  - {name: r, on: {type: report.filed}, as: m, prompt: x}
  - {name: r, on: {type: report.closed}, as: m, prompt: x}
  - {name: s, on: {type: report.closed}, as: m, prompt: "{{bogus}}"}
  - {name: t, on: {type: source.updated, space: "*"}, as: m, prompt: x}
`), "sp", "/sp", env(t))
	got := []string{}
	for _, r := range s.Rules {
		got = append(got, r.Name+"="+r.Status)
	}
	if strings.Join(got, " ") != "r=ok r=invalid s=invalid t=ok" {
		t.Errorf("%v", got)
	}
	if !strings.Contains(s.Rules[1].Reason, "named twice") {
		t.Error(s.Rules[1].Reason)
	}
	// no id: every rule invalid
	s = Parse([]byte("agents: {m: {harness: claude}}\ndispatch: [{name: r, on: {type: x.y}, as: m, prompt: x}]\n"), "sp", "/sp", env(t))
	if s.Rules[0].Status != StatusInvalid || !strings.Contains(s.Rules[0].Reason, "no id:") {
		t.Errorf("%s %s", s.Rules[0].Status, s.Rules[0].Reason)
	}
	// a bad role makes only its rules invalid
	s = Parse([]byte("id: abcde23456\nagents: {m: {harness: claude}, b: {harness: claude, concurrency: 0}}\ndispatch: [{name: r, on: {type: x.y}, as: m, prompt: x}, {name: s, on: {type: x.y}, as: b, prompt: x}]\n"), "sp", "/sp", env(t))
	if s.Rules[0].Status != StatusOK || s.Rules[1].Status != StatusInvalid || !strings.Contains(s.Rules[1].Reason, "concurrency is at least 1") {
		t.Errorf("%s / %s %s", s.Rules[0].Status, s.Rules[1].Status, s.Rules[1].Reason)
	}
}

func ev(scope, typ, subject string, data map[string]string) cloudevent.Event {
	return cloudevent.Event{ID: "x", Type: "io.anfs." + typ, Subject: subject, Scope: scope, Data: data, Actor: "shell:t"}
}

func TestMatching(t *testing.T) {
	s := Parse([]byte(full), "anfs-dev", "/spaces/anfs-dev", env(t))
	r := s.Rules[0]
	ok := map[string]string{"to": "file", "package": "core/pcm", "title": "t"}
	cases := []struct {
		e    cloudevent.Event
		path string
		want bool
	}{
		{ev("abcde23456", "report.filed", "r-20261023-1", ok), "anfs-dev", true},
		{ev("abcde23456", "report.closed", "r-20261023-1", ok), "anfs-dev", false},
		{ev("abcde23456", "report.filed", "acme/core#1", ok), "anfs-dev", false},
		{ev("abcde23456", "report.filed", "r-1", map[string]string{"to": "file", "package": "stack/go"}), "anfs-dev", false},
		{ev("abcde23456", "report.filed", "r-1", map[string]string{"to": "file"}), "anfs-dev", false},
		{ev("zzzzz23456", "report.filed", "r-1", ok), "other", false},
		{ev("host", "report.filed", "r-1", ok), "host", false},
	}
	for i, c := range cases {
		if got := r.Matches(c.e, c.path); got != c.want {
			t.Errorf("case %d: %v, want %v", i, got, c.want)
		}
	}
	// space: another, a prefix, any
	for _, c := range []struct {
		spec, scope, path string
		want              bool
	}{
		{"", "abcde23456", "anfs-dev", true},
		{"lgat/", "zzzzz23456", "lgat/tech/web", true},
		{"lgat/", "zzzzz23456", "lgat", true},
		{"lgat/", "zzzzz23456", "lgatx", false},
		{"lgat", "zzzzz23456", "lgat/tech", false},
		{"lgat/tech", "zzzzz23456", "lgat/tech", true},
		{"zzzzz23456", "zzzzz23456", "", true},
		{"*", "host", "host", true},
	} {
		if got := SpecMatches(c.spec, s, c.scope, c.path); got != c.want {
			t.Errorf("spec %q on %s %s: %v", c.spec, c.scope, c.path, got)
		}
	}
}

func TestFilterSubject(t *testing.T) {
	s := &Space{ID: "abcde23456"}
	for _, c := range []struct{ typ, space, want string }{
		{"report.filed", "", "anfs.abcde23456.anfs.report.filed"},
		{"report.*", "", "anfs.abcde23456.anfs.report.>"},
		{"report.fil?d", "", "anfs.abcde23456.anfs.report.>"},
		{"*.filed", "", "anfs.abcde23456.>"},
		{"*", "", "anfs.abcde23456.>"},
		{"container.started", "lgat/", "anfs.*.anfs.container.started"},
		{"cnfs.*", "", "anfs.abcde23456.cnfs.>"},
		{"cnfs.partitia.report.filed", "", "anfs.abcde23456.cnfs.partitia.report.filed"},
		{"lgat.escrow.*.settled", "*", "anfs.*.lgat.escrow.>"},
	} {
		r := &Rule{Space: s, On: On{Type: c.typ, Space: c.space}}
		if got := r.FilterSubject(); got != c.want {
			t.Errorf("%s: %s, want %s", c.typ, got, c.want)
		}
	}
}

// Rules on other namespaces' types use the short type (SPEC.md §2): on.type in either form is
// held short, matches that namespace's events and no anfs one (gap 12)
func TestOtherNamespaces(t *testing.T) {
	for _, typ := range []string{"io.cnfs.partitia.report.*", "cnfs.partitia.report.*"} {
		r := rule(t, "", `{name: r, on: {type: "`+typ+`"}, as: m, prompt: x}`, env(t))
		if r.Reason != "" || r.On.Type != "cnfs.partitia.report.*" {
			t.Fatalf("%s: %q %q", typ, r.On.Type, r.Reason)
		}
		cnfs := cloudevent.Event{Type: "io.cnfs.partitia.report.filed", Scope: "abcde23456", Data: map[string]string{}}
		anfs := ev("abcde23456", "report.filed", "r-1", nil)
		if !r.Matches(cnfs, "sp") || r.Matches(anfs, "sp") {
			t.Errorf("%s: matches cnfs %v, anfs %v", typ, r.Matches(cnfs, "sp"), r.Matches(anfs, "sp"))
		}
		if r.FilterSubject() != "anfs.abcde23456.cnfs.partitia.report.>" {
			t.Errorf("%s: %s", typ, r.FilterSubject())
		}
	}
	for _, typ := range []string{"io.anfs.report.filed", "anfs.report.filed"} {
		if r := rule(t, "", `{name: r, on: {type: "`+typ+`"}, as: m, prompt: x}`, env(t)); r.On.Type != "report.filed" || r.FilterSubject() != "anfs.abcde23456.anfs.report.filed" {
			t.Errorf("%s: %s %s", typ, r.On.Type, r.FilterSubject())
		}
	}
	// on.data and {{data.<key>}} read top-level strings: a number never matches, renders empty
	r := rule(t, "", `{name: r, on: {type: "cnfs.*", data: {n: "*"}}, as: m, prompt: x}`, env(t))
	e := cloudevent.Event{Type: "io.cnfs.x.y", Scope: "abcde23456", Data: map[string]string{}, RawData: []byte(`{"n":1}`)}
	if r.Matches(e, "sp") || Render("[{{data.n}}]", Vars{Event: e}) != "[]" {
		t.Error("a number in data matched or rendered")
	}
}

func TestTemplates(t *testing.T) {
	v := Vars{Event: cloudevent.Event{ID: "01a", Type: "io.anfs.report.filed", Subject: "R-2026/1 x", Time: "2026-10-23T00:00:00.000Z",
		Actor: "shell:me", Data: map[string]string{"title": "Bad thing"}}, Space: "anfs-dev", Run: "run1", Job: "fix-r"}
	if got := Render("{{subject}} {{ data.title }} {{data.none}}|{{space}} {{run}} {{job}} {{actor}} {{type}} {{id}} {{time}}", v); got != "R-2026/1 x Bad thing |anfs-dev run1 fix-r shell:me report.filed 01a 2026-10-23T00:00:00.000Z" {
		t.Errorf("%q", got)
	}
	if got := JobName("fix-{{subject}}", v); got != "fix-r-2026-1-x" {
		t.Errorf("job name %q", got)
	}
}

func TestDiscover(t *testing.T) {
	dir := t.TempDir()
	sp := filepath.Join(dir, "anfs-dev")
	os.MkdirAll(sp, 0o755)
	os.WriteFile(filepath.Join(sp, "space.yml"), []byte(full), 0o644)
	bin := t.TempDir()
	os.WriteFile(filepath.Join(bin, "wsm"), []byte(`#!/bin/sh
cat <<EOF
[{"space":"anfs-dev","status":"ok","dir":"`+sp+`","id":"abcde23456"},
 {"space":"noid","status":"ok","dir":"/nowhere","id":null},
 {"space":"gone","status":null,"dir":null,"id":"zzzzz23456"}]
EOF
`), 0o755)
	spaces, err := Discover(t.Context(), filepath.Join(bin, "wsm"), env(t))
	if err != nil || len(spaces) != 1 || spaces[0].Path != "anfs-dev" || len(Rules(spaces)) != 1 || Rules(spaces)[0].Status != StatusOK {
		t.Fatalf("%v %+v", err, spaces)
	}
	if _, err := Discover(t.Context(), filepath.Join(bin, "nope"), env(t)); err == nil {
		t.Error("no wsm: no error")
	}
}

// dispatcher: none (02d): the rules are parsed and validated, shown off, never loaded; anything
// but anfsd or none makes the space's rules invalid
func TestDispatcherNone(t *testing.T) {
	e := env(t)
	yml := "id: abcde23456\n%s\nagents: {m: {harness: claude}}\ndispatch:\n  - {name: r, on: {type: report.filed}, as: m, prompt: hi}\n  - {name: bad, on: {type: run.failed}, as: m, prompt: hi}\n"
	s := Parse([]byte(fmt.Sprintf(yml, "dispatcher: none")), "x", "/x", e)
	if s.Dispatcher != DispatcherNone || s.Rules[0].Status != StatusOff || s.Rules[0].Reason != "dispatcher: none" || s.Rules[0].Loaded() ||
		s.Rules[1].Status != StatusInvalid {
		t.Errorf("%s %+v %+v", s.Dispatcher, s.Rules[0], s.Rules[1])
	}
	if s := Parse([]byte(fmt.Sprintf(yml, "dispatcher: anfsd")), "x", "/x", e); s.Rules[0].Status != StatusOK {
		t.Errorf("%+v", s.Rules[0])
	}
	if s := Parse([]byte(fmt.Sprintf(yml, "")), "x", "/x", e); s.Dispatcher != DispatcherAnfsd || s.Rules[0].Status != StatusOK {
		t.Errorf("%s %+v", s.Dispatcher, s.Rules[0])
	}
	s = Parse([]byte(fmt.Sprintf(yml, "dispatcher: firstmate")), "x", "/x", e)
	if s.Rules[0].Status != StatusInvalid || !strings.Contains(s.Rules[0].Reason, `dispatcher: "firstmate" is anfsd or none`) {
		t.Errorf("%+v", s.Rules[0])
	}
}
