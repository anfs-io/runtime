package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/testfake"
	"github.com/anfs-io/runtime/go/internal/testnats"
	"github.com/nats-io/nats.go/jetstream"
)

const spaceYML = `id: abcde23456
---
access:
  - {path: ~/notes, mode: write}
agents:
  maintainer: {harness: claude, budget_usd: 2}
dispatch:
  - name: fix-core-report
    on: {type: report.filed, data: {to: file, package: "core/*"}}
    as: maintainer
    job: {name: "fix-{{subject}}", repos: [core]}
    prompt_file: prompts/report-filed.md
    timeout: 2h
  - name: off
    enabled: false
    on: {type: report.filed}
    as: maintainer
    job: none
    prompt: "look at {{subject}}"
  - name: loop
    on: {type: "run.*"}
    as: maintainer
    prompt: x
`

// a space installed in the temp HOME, and the fake wsm listing it
func installSpace(t *testing.T, f *testfake.Fakes, home string) string {
	dir := filepath.Join(home, "spaces", "anfs-dev")
	os.MkdirAll(filepath.Join(dir, "prompts"), 0o755)
	os.WriteFile(filepath.Join(dir, "space.yml"), []byte(spaceYML), 0o644)
	os.WriteFile(filepath.Join(dir, "prompts/report-filed.md"), []byte("Fix {{subject}}: {{data.title}}.\n"), 0o644)
	f.Wsm(`[{"space":"anfs-dev","status":"ok","dir":"` + dir + `","id":"abcde23456"},{"space":"other","status":"ok","dir":null,"id":null}]`)
	return dir
}

func TestRulesCommand(t *testing.T) {
	f := testfake.New(t)
	f.Setenv(t)
	dir := installSpace(t, f, f.Home)
	var out, errb bytes.Buffer
	if c := run([]string{"rules", "--json"}, &out, &errb); c != 0 {
		t.Fatalf("exit %d: %s", c, errb.String())
	}
	var rows []ruleRow
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	got := []string{}
	for _, r := range rows {
		got = append(got, r.Space+"/"+r.Rule+"="+r.Status)
	}
	if strings.Join(got, " ") != "anfs-dev/fix-core-report=ok anfs-dev/off=disabled anfs-dev/loop=invalid" {
		t.Errorf("%v", got)
	}
	if rows[0].On != "report.filed package=core/* to=file" || rows[0].Job != "fix-{{subject}}" || rows[0].As != "maintainer" ||
		rows[0].Harness != "claude" || rows[0].SpaceID != "abcde23456" || !strings.Contains(rows[2].Reason, "a run would start runs") || rows[1].Enabled {
		t.Errorf("%+v", rows)
	}
	// a file named: its space's path from wsm (the id), and the table
	out.Reset()
	if c := run([]string{"rules", filepath.Join(dir, "space.yml")}, &out, &errb); c != 0 {
		t.Fatal(errb.String())
	}
	if !strings.HasPrefix(out.String(), "RULE ") || !strings.Contains(out.String(), "fix-core-report  ok ") || !strings.Contains(out.String(), "anfs-dev") {
		t.Errorf("%s", out.String())
	}
	// no claude on PATH: invalid, saying so
	os.Remove(filepath.Join(f.Bin, "claude"))
	out.Reset()
	run([]string{"rules", "--json"}, &out, &errb)
	if !strings.Contains(out.String(), "claude is not on anfsd's PATH") {
		t.Errorf("%s", out.String())
	}
	if c := run([]string{"rules", "--bogus"}, &out, &errb); c != 64 {
		t.Errorf("exit %d", c)
	}
}

func TestMatchCommand(t *testing.T) {
	f := testfake.New(t)
	f.Setenv(t)
	installSpace(t, f, f.Home)
	var out, errb bytes.Buffer
	args := []string{"match", "--type", "report.filed", "--subject", "r-20261023-9", "--space", "anfs-dev",
		"--data", "to=file", "--data", "package=core/pcm", "--data", "title=It broke"}
	if c := run(args, &out, &errb); c != 0 {
		t.Fatalf("exit %d: %s", c, errb.String())
	}
	s := out.String()
	for _, want := range []string{
		"event: report.filed r-20261023-9 in anfs-dev by shell:",
		"anfs-dev/fix-core-report (ok): submit",
		"job:     anfs work new fix-r-20261023-9 core",
		"command: claude -p --output-format stream-json --verbose --permission-mode dontAsk --settings '<run dir>/claude.json'",
		"--max-budget-usd 2",
		`-- 'Fix r-20261023-9: It broke. (+ anfsd'\''s footer)'`,
		"anfs-dev/off (disabled): disabled: would submit, but the rule has enabled: false",
		"anfs-dev/loop (invalid): invalid: ",
		"ANFS_EVENTS_SPOOL=<run dir>/events",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("no %q in:\n%s", want, s)
		}
	}
	// the names of anfsd's environment that pass: the allow-list, never the fakes' or a token
	if i := strings.Index(s, "passes:  "); i < 0 || strings.Contains(s[i:i+strings.Index(s[i:], "\n")], "FAKE_") ||
		!strings.Contains(s[i:i+strings.Index(s[i:], "\n")], " PATH ") {
		t.Errorf("passes:\n%s", s)
	}
	// nothing written, nothing started
	if _, err := os.Stat(filepath.Join(f.Home, ".local/state/anfsd/runs")); !os.IsNotExist(err) {
		t.Error("match wrote runs/")
	}
	if len(f.Starts()) != 0 || len(f.AnfsCalls()) != 0 {
		t.Error("match started something")
	}
	// an event from a file, not matching
	ev := filepath.Join(t.TempDir(), "ev.json")
	l, _, _ := cloudevent.Line(cloudevent.New{Scope: "abcde23456", Type: "report.filed", Subject: "r-1", Actor: "shell:t",
		Data: map[string]string{"to": "file", "package": "stack/go"}}, time.Now())
	os.WriteFile(ev, l, 0o644)
	out.Reset()
	if c := run([]string{"match", "--json", "--event", ev}, &out, &errb); c != 0 {
		t.Fatal(errb.String())
	}
	var rows []matchRow
	json.Unmarshal(out.Bytes(), &rows)
	if len(rows) != 3 || rows[0].Result != "no match" || rows[0].Why != "data.package: stack/go is not core/*" || rows[1].Result != "disabled" {
		t.Errorf("%+v", rows)
	}
	if c := run([]string{"match"}, &out, &errb); c != 64 {
		t.Errorf("no event: exit %d", c)
	}
}

func TestRenderCommand(t *testing.T) {
	r, w, _ := os.Pipe()
	w.WriteString(`{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}` + "\nplain\n")
	w.Close()
	old := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = old }()
	var out, errb bytes.Buffer
	if c := run([]string{"render", "claude"}, &out, &errb); c != 0 || out.String() != "hello\nplain\n" {
		t.Errorf("%d %q", c, out.String())
	}
}

// anfsd run with a rule: a report.filed line in the spool is a run (the fake harness), and its
// run.submitted, run.working, run.completed are in the spool and on the bus; the report sink got
// the report.filed; SIGTERM leaves anfsd with exit 0
func TestRunDispatches(t *testing.T) {
	srv := testnats.Start(t, testnats.Users{"anfsd": "pw"})
	h := home(t, srv, "pw")
	f, e := env(t, h)
	installSpace(t, f, h)
	// the spool as a host has it (a scope directory made later is seen by the 30 s reconcile)
	os.MkdirAll(filepath.Join(h, ".local/state/anfs/events/abcde23456"), 0o755)
	bin := build(t)
	cmd := exec.Command(bin, "run")
	cmd.Env = e
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	b, err := bus.Connect(bus.Options{URL: srv.URL(), User: "anfsd", Password: "pw"}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	// the consumers exist (deliver new: only what comes after them)
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, e1 := b.JetStream().Consumer(ctx, bus.Stream, "anfsd-dispatch")
		_, e2 := b.JetStream().Consumer(ctx, bus.Stream, "anfsd-sink-report")
		if e1 == nil && e2 == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no consumers; stderr:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	events := filepath.Join(h, ".local/state/anfs/events")
	filed, err := cloudevent.Append(events, cloudevent.New{Scope: "abcde23456", Type: "report.filed", Subject: "r-20261023-5",
		Actor: "shell:t", SpacePath: "anfs-dev", Data: map[string]string{"to": "file", "package": "core/pcm", "title": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	// the run.* on the bus
	want := []string{"run.submitted", "run.working", "run.completed"}
	var got []string
	deadline = time.Now().Add(20 * time.Second)
	for {
		got = got[:0]
		s, _ := b.JetStream().Stream(ctx, bus.Stream)
		c, _ := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{"anfs.abcde23456.run.>"}})
		if c != nil {
			for {
				m, err := c.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
				if err != nil {
					break
				}
				e, _ := cloudevent.Parse(m.Data())
				got = append(got, e.Short())
			}
		}
		if strings.Join(got, " ") == strings.Join(want, " ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run events on the bus: %v\nstderr:\n%s", got, stderr.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	starts := f.Starts()
	if len(starts) != 1 || starts[0].Env["ANFS_AS"] != "anfs-dev/maintainer" || !strings.Contains(starts[0].Argv[len(starts[0].Argv)-1], filed.ID) {
		t.Errorf("starts %+v", starts)
	}
	// the sink: anfs report __sink got the report.filed (and the run.* never)
	deadline = time.Now().Add(10 * time.Second)
	for {
		ents, _ := filepath.Glob(filepath.Join(f.Log, "sink.*"))
		if len(ents) == 1 {
			b, _ := os.ReadFile(ents[0])
			if !strings.Contains(string(b), filed.ID) {
				t.Errorf("sink got %s", b)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink: %d calls\n%s", len(ents), stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// the spool has every transition, each one valid
	n := 0
	fh, _ := os.Open(filepath.Join(events, "abcde23456", filed.Time[:7]+".jsonl"))
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		if _, err := cloudevent.Parse(sc.Bytes()); err != nil {
			t.Errorf("%v: %s", err, sc.Text())
		}
		n++
	}
	fh.Close()
	if n != 4 {
		t.Errorf("%d lines in the spool, want 4", n)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("exit: %v\n%s", err, stderr.String())
	}
	for _, w := range []string{"rules: 1 loaded, 1 disabled, 1 invalid, from 1 spaces", "rule anfs-dev/loop invalid", "consumer anfsd-dispatch: filters anfs.abcde23456.report.filed anfs.host.source.updated", "completed"} {
		if !strings.Contains(stderr.String(), w) {
			t.Errorf("log lacks %q:\n%s", w, stderr.String())
		}
	}
}
