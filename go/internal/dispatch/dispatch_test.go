package dispatch

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/space"
	"github.com/anfs-io/runtime/go/internal/supervise"
	"github.com/anfs-io/runtime/go/internal/testfake"
	"github.com/anfs-io/runtime/go/internal/testnats"
)

const sid = "abcde23456"

const spaceYML = `id: abcde23456
agents:
  maintainer: {harness: claude, concurrency: 1, queue: 1}
dispatch:
  - name: fix-core-report
    on: {type: report.filed, data: {to: file, package: "core/*"}}
    as: maintainer
    job: {name: "fix-{{subject}}", repos: [core]}
    prompt: "Fix {{subject}}: {{data.title}} ({{job}})"
    rate: 2/h
    stale: 1h
  - name: off
    enabled: false
    on: {type: report.filed}
    as: maintainer
    prompt: x
  - name: bad
    on: {type: run.failed}
    as: maintainer
    prompt: x
`

type rig struct {
	d    *Dispatcher
	sup  *supervise.Supervisor
	f    *testfake.Fakes
	now  time.Time
	logs []string
}

func newRig(t *testing.T, yml string) *rig {
	f := testfake.New(t)
	dir := t.TempDir()
	env := space.Env{Home: f.Home, LookPath: f.LookPath}
	r := &rig{f: f, now: time.Date(2026, 10, 23, 18, 0, 0, 0, time.UTC)}
	sp := space.Parse([]byte(yml), "anfs-dev", dir, env)
	r.sup = &supervise.Supervisor{Store: supervise.Store{Dir: filepath.Join(dir, "runs")}, Events: filepath.Join(dir, "events"), MaxRuns: 2}
	if err := r.sup.Open(); err != nil {
		t.Fatal(err)
	}
	r.d = &Dispatcher{Sup: r.sup, Now: func() time.Time { return r.now },
		Load: func(context.Context) ([]*space.Space, error) { return []*space.Space{sp}, nil },
		Logf: func(f string, a ...any) { r.logs = append(r.logs, f) }}
	if err := r.d.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

func event(t *testing.T, scope, typ, subject, actor string, at time.Time, data map[string]string) []byte {
	t.Helper()
	l, _, err := cloudevent.Line(cloudevent.New{Scope: scope, Type: typ, Subject: subject, Actor: actor, Data: data}, at)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func filed(t *testing.T, r *rig, subject, actor string, age time.Duration) []byte {
	return event(t, sid, "report.filed", subject, actor, r.now.Add(-age), map[string]string{"to": "file", "package": "core/pcm", "title": "It broke"})
}

func actions(ds []Decision) string {
	var s []string
	for _, d := range ds {
		a := d.Rule.Name + ":" + d.Action
		if d.Reason != "" {
			a += "(" + d.Reason + ")"
		}
		s = append(s, a)
	}
	return strings.Join(s, " ")
}

func TestLoadAndFilters(t *testing.T) {
	r := newRig(t, spaceYML)
	if rules := r.d.Rules(); len(rules) != 1 || rules[0].Name != "fix-core-report" {
		t.Fatalf("loaded %v", rules)
	}
	if got := strings.Join(r.d.Filters(), " "); got != "anfs.abcde23456.report.filed anfs.host.source.updated" {
		t.Errorf("filters %s", got)
	}
	for _, c := range []struct{ in, want []string }{
		{[]string{"anfs.a.report.filed", "anfs.a.report.>"}, []string{"anfs.a.report.>"}},
		{[]string{"anfs.a.report.filed", "anfs.a.report.filed"}, []string{"anfs.a.report.filed"}},
		{[]string{"anfs.*.report.filed", "anfs.a.>"}, []string{"anfs.>"}},
		{[]string{"anfs.a.x.y", "anfs.b.x.y"}, []string{"anfs.a.x.y", "anfs.b.x.y"}},
		{[]string{"anfs.*.x.y", "anfs.b.x.y"}, []string{"anfs.*.x.y"}},
	} {
		var rules []*space.Rule
		for _, s := range c.in {
			parts := strings.SplitN(s, ".", 3)
			ru := &space.Rule{Space: &space.Space{ID: parts[1]}, On: space.On{Type: strings.ReplaceAll(parts[2], ">", "*")}}
			if parts[1] == "*" {
				ru.Space.ID, ru.On.Space = "zzzzz23456", "lgat/"
			}
			rules = append(rules, ru)
		}
		got := Filters(rules)
		want := append(append([]string{}, c.want...), SourceUpdated)
		if c.want[0] == "anfs.>" {
			want = c.want
		}
		if strings.Join(got, " ") != strings.Join(sortedCopy(want), " ") {
			t.Errorf("%v: %v, want %v", c.in, got, want)
		}
	}
}

func sortedCopy(s []string) []string {
	c := append([]string{}, s...)
	for i := range c {
		for j := i + 1; j < len(c); j++ {
			if c[j] < c[i] {
				c[i], c[j] = c[j], c[i]
			}
		}
	}
	return c
}

func TestSubmitOncePerEvent(t *testing.T) {
	r := newRig(t, spaceYML)
	l := filed(t, r, "r-20261023-4", "shell:me", time.Minute)
	ds, err := r.d.Handle(l)
	if err != nil || actions(ds) != "fix-core-report:submitted" {
		t.Fatalf("%v %s", err, actions(ds))
	}
	rec := ds[0].Record
	if rec.Job != "fix-r-20261023-4" || rec.Repos[0] != "core" || rec.Address != "anfs-dev/maintainer" || rec.Harness != "claude" {
		t.Errorf("%+v", rec)
	}
	if b, _ := os.ReadFile(filepath.Join(r.sup.Store.RunDir(rec.ID), "prompt.md")); string(b) != "Fix r-20261023-4: It broke (fix-r-20261023-4)" {
		t.Errorf("prompt %q", b)
	}
	// delivered twice: the record is found, nothing new
	ds, err = r.d.Handle(l)
	if err != nil || len(ds) != 0 {
		t.Errorf("redelivery: %v %s", err, actions(ds))
	}
	recs, _ := r.sup.Store.All()
	if len(recs) != 1 {
		t.Errorf("%d records", len(recs))
	}
	// not matching: another package, another type, another space
	for _, l := range [][]byte{
		event(t, sid, "report.filed", "r-x", "shell:me", r.now, map[string]string{"to": "file", "package": "stack/go"}),
		event(t, sid, "report.closed", "r-x", "shell:me", r.now, map[string]string{"to": "file", "package": "core/pcm"}),
		event(t, "zzzzz23456", "report.filed", "r-x", "shell:me", r.now, map[string]string{"to": "file", "package": "core/pcm"}),
	} {
		if ds, _ := r.d.Handle(l); len(ds) != 0 {
			t.Errorf("matched: %s", actions(ds))
		}
	}
	if _, err := r.d.Handle([]byte("not an event")); err == nil {
		t.Error("not an event: no error")
	}
}

func TestGuards(t *testing.T) {
	r := newRig(t, strings.Replace(spaceYML, "    stale: 1h\n", "    stale: 1h\n    overflow: reject\n", 1))
	// stale
	ds, _ := r.d.Handle(filed(t, r, "r-1", "shell:me", 2*time.Hour))
	if actions(ds) != "fix-core-report:rejected(stale)" {
		t.Errorf("stale: %s", actions(ds))
	}
	// queue under overflow: reject: concurrency 1 and queue 1, no run started (the supervisor is
	// only open): the first waits, the second is beyond the queue
	ds, _ = r.d.Handle(filed(t, r, "r-2", "shell:me", 0))
	first := ds[0].Record
	l3 := filed(t, r, "r-3", "shell:me", 0)
	ds2, _ := r.d.Handle(l3)
	if actions(ds) != "fix-core-report:submitted" || actions(ds2) != "fix-core-report:rejected(queue)" {
		t.Errorf("queue: %s / %s", actions(ds), actions(ds2))
	}
	// rejected for the queue: seen (a redelivery gives no second run.rejected), not handled
	ev3, _ := cloudevent.Parse(l3)
	if ds, _ := r.d.Handle(l3); len(ds) != 0 || !r.sup.Seen(sid, "fix-core-report", ev3.ID) || r.sup.Handled(sid, "fix-core-report", ev3.ID) {
		t.Errorf("redelivery of a rejection: %s", actions(ds))
	}
	// loop: an event from a run of this rule
	ds, _ = r.d.Handle(filed(t, r, "r-4", "claude:"+first.Last8(), 0))
	if actions(ds) != "fix-core-report:rejected(loop)" {
		t.Errorf("loop: %s", actions(ds))
	}
	// loop through actorrun (02d): the run's line, stamped by anfsd, says another actor: still a loop
	stamped, _, err := cloudevent.StampLine(filed(t, r, "r-5", "shell:person", 0),
		cloudevent.Stamp{Actor: "shell:person", ActorSpace: sid, ActorRole: "maintainer", ActorRun: first.ID})
	if err != nil {
		t.Fatal(err)
	}
	ds, _ = r.d.Handle(stamped)
	if actions(ds) != "fix-core-report:rejected(loop)" {
		t.Errorf("loop by actorrun: %s", actions(ds))
	}
	// rate under overflow: reject: 2/h, two queued; the third is rejected
	r2 := newRig(t, strings.Replace(strings.Replace(spaceYML, "queue: 1", "queue: 5", 1), "    stale: 1h\n", "    stale: 1h\n    overflow: reject\n", 1))
	for i, want := range []string{"submitted", "submitted", "rejected(rate)"} {
		ds, _ := r2.d.Handle(filed(t, r2, "r-"+string(rune('a'+i)), "shell:me", 0))
		if actions(ds) != "fix-core-report:"+want {
			t.Errorf("rate %d: %s", i, actions(ds))
		}
	}
	// every rejection is a record and a run.rejected
	recs, _ := r.sup.Store.All()
	rej := 0
	for _, rc := range recs {
		if rc.State == supervise.Rejected {
			rej++
		}
	}
	if rej != 4 {
		t.Errorf("%d rejected records", rej)
	}
}

// overflow: delay (the default): a full queue or a spent rate holds the run, up to hold:; a rule
// that holds runs holds the next one too; a loop is never held
func TestHold(t *testing.T) {
	r := newRig(t, strings.Replace(spaceYML, "    stale: 1h\n", "    stale: 1h\n    hold: 2\n", 1))
	ds, _ := r.d.Handle(filed(t, r, "r-1", "shell:me", 0))
	first := ds[0].Record
	var got []string
	for _, s := range []string{"r-2", "r-3", "r-4"} {
		ds, _ := r.d.Handle(filed(t, r, s, "shell:me", 0))
		got = append(got, actions(ds)+" "+ds[0].Detail)
	}
	want := []string{
		"fix-core-report:held(queue) anfs-dev/maintainer's queue is full (queue: 1)",
		"fix-core-report:held(queue) anfs-dev/maintainer's queue is full (queue: 1)",
		"fix-core-report:rejected(queue) hold: 2 full (anfs-dev/maintainer's queue is full (queue: 1))",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got:\n%s", strings.Join(got, "\n"))
	}
	if n, why := r.sup.Held(sid, "fix-core-report"); n != 2 || why != "queue" {
		t.Errorf("held %d %s", n, why)
	}
	// a loop is rejected, not held
	ds, _ = r.d.Handle(filed(t, r, "r-5", "claude:"+first.Last8(), 0))
	if actions(ds) != "fix-core-report:rejected(loop)" {
		t.Errorf("loop: %s", actions(ds))
	}
	// the records: two held (submitted + held), run.submitted with held and detail
	recs, _ := r.sup.Store.All()
	h := 0
	for _, rc := range recs {
		if rc.State == supervise.Submitted && rc.Held == "queue" && rc.HeldSince != "" && rc.Queued == "" {
			h++
		}
	}
	if h != 2 {
		t.Errorf("%d held records", h)
	}
	b, _ := os.ReadFile(filepath.Join(r.sup.Events, sid, time.Now().UTC().Format("2006-01")+".jsonl"))
	if !strings.Contains(string(b), `"detail":"anfs-dev/maintainer's queue is full (queue: 1)","event":`) || !strings.Contains(string(b), `"held":"queue"`) {
		t.Errorf("run.submitted:\n%s", b)
	}
	// rate: 2/h with a roomy queue: the third is held for the rate, the fourth behind it
	r2 := newRig(t, strings.Replace(spaceYML, "queue: 1", "queue: 5", 1))
	got = nil
	for i := 0; i < 4; i++ {
		ds, _ := r2.d.Handle(filed(t, r2, "r-"+string(rune('a'+i)), "shell:me", 0))
		got = append(got, actions(ds))
	}
	if strings.Join(got, " ") != "fix-core-report:submitted fix-core-report:submitted fix-core-report:held(rate) fix-core-report:held(rate)" {
		t.Errorf("rate: %v", got)
	}
}

func TestDecideDry(t *testing.T) {
	r := newRig(t, spaceYML)
	sp := r.d.spaces[0]
	ev, _ := cloudevent.Parse(filed(t, r, "r-9", "shell:me", 0))
	var off *space.Rule
	for _, ru := range sp.Rules {
		if ru.Name == "off" {
			off = ru
		}
	}
	if d := r.d.Decide(off, ev, nil, true); !d.Match || d.Action != "disabled" {
		t.Errorf("%+v", d)
	}
	ev2, _ := cloudevent.Parse(event(t, sid, "report.filed", "r-9", "shell:me", r.now, map[string]string{"to": "file", "package": "stack/go"}))
	if d := r.d.Decide(sp.Rules[0], ev2, nil, true); d.Match || d.Matched != "data.package: stack/go is not core/*" {
		t.Errorf("%+v", d)
	}
	if recs, _ := r.sup.Store.All(); len(recs) != 0 {
		t.Errorf("Decide wrote %d records", len(recs))
	}
}

// The consumer on a real JetStream: its filter subjects, an event published is a run submitted
// once, and a source.updated reads the rules again
func TestConsumer(t *testing.T) {
	srv := testnats.Start(t, nil)
	b, err := bus.Connect(bus.Options{URL: srv.URL()}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.EnsureStream(ctx, bus.StreamSettings{MaxAge: time.Hour, MaxBytes: 1 << 20, DupWindow: time.Minute}); err != nil {
		t.Fatal(err)
	}
	r := newRig(t, spaceYML)
	r.now = time.Now()
	var loads atomic.Int32
	inner := r.d.Load
	r.d.Load = func(c context.Context) ([]*space.Space, error) { loads.Add(1); return inner(c) }
	r.d.Logf = t.Logf
	done := make(chan struct{})
	go func() { r.d.Run(ctx, b); close(done) }()
	js := b.JetStream()
	var info *consumerInfo
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := js.Consumer(ctx, bus.Stream, Consumer)
		if err == nil {
			ci, _ := c.Info(ctx)
			info = &consumerInfo{filters: ci.Config.FilterSubjects}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no consumer")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if strings.Join(info.filters, " ") != "anfs.abcde23456.report.filed anfs.host.source.updated" {
		t.Errorf("filters %v", info.filters)
	}
	l := filed(t, r, "r-77", "shell:me", 0)
	ev, _ := cloudevent.Parse(l)
	for i := 0; i < 2; i++ { // the same event twice (another msg id: past the duplicate window)
		if _, err := js.Publish(ctx, ev.NatsSubject(), l); err != nil {
			t.Fatal(err)
		}
	}
	src := event(t, "host", "source.updated", "core", "shell:me", time.Now(), nil)
	js.Publish(ctx, "anfs.host.source.updated", src)
	deadline = time.Now().Add(5 * time.Second)
	for {
		recs, _ := r.sup.Store.All()
		if len(recs) == 1 && loads.Load() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d records, %d loads", len(recs), loads.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
}

type consumerInfo struct{ filters []string }

func request(t *testing.T, dir string, line []byte, rule string, again bool) {
	t.Helper()
	os.MkdirAll(dir, 0o700)
	b, _ := json.Marshal(Request{Event: strings.TrimSpace(string(line)), Rule: rule, By: "shell:person", Again: again})
	if err := os.WriteFile(filepath.Join(dir, supervise.NewID()+".json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// anfs agents redispatch's requests: a run that skips stale, rate and loop (it can be held for
// the queue); refused for a run not ended, a handled event without again, a disabled rule; the
// files removed
func TestRedispatch(t *testing.T) {
	r := newRig(t, strings.Replace(spaceYML, "    stale: 1h\n", "    stale: 1h\n    overflow: reject\n", 1))
	r.d.RedispatchDir = filepath.Join(t.TempDir(), "redispatch")
	a := filed(t, r, "r-1", "shell:me", 0)
	b := filed(t, r, "r-2", "shell:me", 0)
	c := filed(t, r, "r-3", "shell:me", 2*time.Hour)
	ds, _ := r.d.Handle(a)
	first := ds[0].Record
	d := filed(t, r, "r-4", "claude:"+first.Last8(), 0)
	for _, l := range [][]byte{b, c, d} {
		r.d.Handle(l)
	}
	request(t, r.d.RedispatchDir, b, "", false)
	got := r.d.Redispatch()
	if len(got) == 0 || !strings.HasPrefix(got[0], "anfs-dev/fix-core-report: submitted run ") || !strings.HasSuffix(got[0], "held for the queue") {
		t.Errorf("rejected for the queue: %v", got)
	}
	for _, c := range []struct {
		line  []byte
		rule  string
		again bool
		want  string
	}{
		{a, "", false, "anfs-dev/fix-core-report: refused: it has a run for this event that has not ended"},
		{b, "", false, "anfs-dev/fix-core-report: refused: it has a run for this event that has not ended"},
		{c, "fix-core-report", false, "anfs-dev/fix-core-report: refused: it already handled this event (--again for another run)"},
		{d, "", true, "anfs-dev/fix-core-report: submitted run "},
		{a, "off", false, "anfs-dev/off: refused: the rule is disabled (enabled: false)"},
		{a, "nonesuch", false, "no rule nonesuch"},
		{event(t, sid, "report.closed", "r-9", "shell:me", r.now, nil), "", false, "no rule matches report.closed r-9"},
	} {
		request(t, r.d.RedispatchDir, c.line, c.rule, c.again)
		got := r.d.Redispatch()
		if len(got) == 0 || !strings.HasPrefix(got[0], c.want) {
			t.Errorf("%s: %v", c.want, got)
		}
	}
	// stale skipped: c, stale and rejected, is handled (a stale rejection is final); with again it
	// is submitted though 2h old
	request(t, r.d.RedispatchDir, c, "", true)
	if got := r.d.Redispatch(); len(got) == 0 || !strings.Contains(got[0], "submitted run ") {
		t.Errorf("stale with again: %v", got)
	}
	if ents, _ := os.ReadDir(r.d.RedispatchDir); len(ents) != 0 {
		t.Errorf("%d requests left", len(ents))
	}
	recs, _ := r.sup.Store.All()
	n := 0
	for _, rc := range recs {
		if rc.RedispatchBy == "shell:person" {
			n++
		}
	}
	if n != 3 {
		t.Errorf("%d redispatched records", n)
	}
}

type cancelLog struct {
	*supervise.Supervisor
	calls []string
}

func (c *cancelLog) CancelUnstarted(spaceID, by string) int {
	c.calls = append(c.calls, spaceID+" "+by)
	return 0
}

// dispatcher: none (02d): the space's rules are not loaded nor in the filters, a matching event
// starts nothing, and what is not started of it is canceled at the reload
func TestDispatcherNone(t *testing.T) {
	r := newRig(t, "dispatcher: none\n"+spaceYML)
	c := &cancelLog{Supervisor: r.sup}
	r.d.Sup = c
	r.d.Reload(context.Background())
	if len(r.d.Rules()) != 0 || strings.Join(r.d.Filters(), " ") != SourceUpdated {
		t.Errorf("rules %v filters %v", r.d.Rules(), r.d.Filters())
	}
	if ds, _ := r.d.Handle(filed(t, r, "r-1", "shell:me", 0)); len(ds) != 0 {
		t.Errorf("%s", actions(ds))
	}
	if strings.Join(c.calls, ",") != sid+" dispatcher:none" {
		t.Errorf("cancel calls %v", c.calls)
	}
	// redispatch refuses it
	r.d.RedispatchDir = filepath.Join(t.TempDir(), "rd")
	request(t, r.d.RedispatchDir, filed(t, r, "r-2", "shell:me", 0), "fix-core-report", false)
	if got := r.d.Redispatch(); len(got) != 1 || !strings.Contains(got[0], "refused: the space's dispatcher is none") {
		t.Errorf("%v", got)
	}
}
