package supervise

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/testfake"
)

const sid = "abcde23456"

type rig struct {
	t      *testing.T
	f      *testfake.Fakes
	sup    *Supervisor
	events string
	cancel context.CancelFunc
	logs   *logbuf
}

type logbuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logbuf) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(strings.TrimSpace(sprintf(f, a...)) + "\n")
}

func (l *logbuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newRig(t *testing.T, maxRuns int) *rig {
	f := testfake.New(t)
	root := t.TempDir()
	r := &rig{t: t, f: f, events: filepath.Join(root, "events"), logs: &logbuf{}}
	r.sup = r.newSup(filepath.Join(root, "runs"), maxRuns)
	return r
}

func (r *rig) newSup(dir string, maxRuns int) *Supervisor {
	return &Supervisor{
		Store: Store{Dir: dir}, Events: r.events, MaxRuns: maxRuns, Anfs: r.f.Anfs,
		Grace: 500 * time.Millisecond, Check: 100 * time.Millisecond,
		RetryDelay: func(int) time.Duration { return 50 * time.Millisecond },
		Env:        r.f.Env(), Logf: r.logs.logf,
	}
}

func (r *rig) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if err := r.sup.Start(ctx); err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { cancel(); r.sup.Wait() })
}

// a record as the dispatcher makes it
func rec(rule, role, h, job string, repos ...string) *Record {
	return &Record{ID: NewID(), Rule: rule, Space: "anfs-dev", SpaceID: sid, SpaceDir: "/tmp", Event: NewID(),
		EventType: "report.filed", Subject: "r-1", Actor: "shell:t", Role: role, Address: "anfs-dev/" + role,
		Harness: h, Job: job, Repos: repos, Concurrency: 1, Timeout: "1m"}
}

const line = `{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-000000000001","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.filed","subject":"r-1","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"shell:t","anfsv":1,"data":{}}`

func (r *rig) submit(rc *Record) {
	r.t.Helper()
	if err := r.sup.Submit(rc, []byte(line), "Fix "+rc.Subject+"."); err != nil {
		r.t.Fatal(err)
	}
}

// wait until the record is in a state (or fail)
func (r *rig) waitState(id, state string, within time.Duration) *Record {
	r.t.Helper()
	deadline := time.Now().Add(within)
	for {
		rc, err := r.sup.Store.Load(id)
		if err == nil && rc.State == state {
			return rc
		}
		if time.Now().After(deadline) {
			got := "none"
			if rc != nil {
				got = rc.State + " " + rc.Reason + " " + rc.Detail
			}
			r.t.Fatalf("run %s: %s, wanted %s\nlog:\n%s", id, got, state, r.logs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// the run.* events, in order, as "<type> <subject>" plus the data asked for; every line valid
func (r *rig) events_(keys ...string) []string {
	r.t.Helper()
	var out []string
	files, _ := filepath.Glob(filepath.Join(r.events, "*", "*.jsonl"))
	for _, f := range files {
		fh, _ := os.Open(f)
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			e, err := cloudevent.Parse(sc.Bytes())
			if err != nil {
				r.t.Fatalf("not a valid line: %v: %s", err, sc.Text())
			}
			if e.Actor != "anfsd" || e.Scope != sid || e.SpacePath != "anfs-dev" {
				r.t.Errorf("event %s: actor %s scope %s", e.Short(), e.Actor, e.Scope)
			}
			s := e.Short()
			for _, k := range keys {
				if v, ok := e.Data[k]; ok {
					s += " " + k + "=" + v
				}
			}
			out = append(out, s)
		}
		fh.Close()
	}
	return out
}

func TestLifecycleWithJob(t *testing.T) {
	r := newRig(t, 2)
	r.f.Touch("commit", "")
	r.start()
	rc := rec("fix-core-report", "maintainer", "claude", "fix-r-1", "core", "stack")
	r.submit(rc)
	done := r.waitState(rc.ID, Completed, 10*time.Second)
	// anfs work new with the rendered name and the repos; path and env read
	calls := strings.Join(r.f.AnfsCalls(), "\n")
	if !strings.Contains(calls, "work new fix-r-1 core stack") || !strings.Contains(calls, "work env fix-r-1") {
		t.Errorf("anfs calls:\n%s", calls)
	}
	starts := r.f.Starts()
	if len(starts) != 1 || starts[0].Name != "claude" {
		t.Fatalf("starts %+v", starts)
	}
	s := starts[0]
	job := filepath.Join(r.f.Work, "fix-r-1")
	if s.Cwd != job && !strings.HasSuffix(s.Cwd, "/work/fix-r-1") {
		t.Errorf("cwd %s", s.Cwd)
	}
	if s.Stdin != "" {
		t.Errorf("stdin %q", s.Stdin)
	}
	runDir := r.sup.Store.RunDir(rc.ID)
	for k, v := range map[string]string{
		"ANFS_ACTOR": "claude:" + rc.Last8(), "ANFS_AS": "anfs-dev/maintainer", "ANFS_RUN": rc.ID,
		"ANFS_EVENT": filepath.Join(runDir, "event.json"), "ANFS_WORK_JOB": "fix-r-1", "ANFS_WORK_NUMBER": "7",
		"TMPDIR": job + "/.tmp", "ROUNDTRIP_SRC_ROOT": "/a path", "FAKE_CTL": r.f.Ctl,
	} {
		if s.Env[k] != v {
			t.Errorf("env %s=%q, want %q", k, s.Env[k], v)
		}
	}
	if _, ok := s.Env["ANFS_WORK"]; ok {
		t.Error("ANFS_WORK reached the run")
	}
	// the argv: claude's, the settings file written, the prompt with the footer last
	a := s.Argv
	if a[0] != "-p" || a[len(a)-2] != "--" || !strings.HasPrefix(a[len(a)-1], "Fix r-1.\n\n---\n\nYou are anfs-dev/maintainer") ||
		!strings.Contains(a[len(a)-1], `"id":"01a169de-e9bb-7000-8000-000000000001"`) || !strings.Contains(a[len(a)-1], "Your job is "+job) {
		t.Errorf("argv %q", a)
	}
	if _, err := os.Stat(filepath.Join(runDir, "claude.json")); err != nil {
		t.Error(err)
	}
	if b, _ := os.ReadFile(filepath.Join(runDir, "event.json")); strings.TrimSpace(string(b)) != line {
		t.Errorf("event.json %s", b)
	}
	// the record and the events
	if done.Summary != "The cause was X. Fixed with a test. Left for the person: review and merge." ||
		done.Commits["core"] != 1 || done.Commits["stack"] != 1 || *done.Exit != 0 || !done.JobMade || done.Pid == 0 {
		t.Errorf("record %+v", done)
	}
	got := strings.Join(r.events_("job", "branch", "commits"), "\n")
	want := "run.submitted job=fix-r-1\nrun.working job=fix-r-1 branch=fix-r-1\nrun.completed job=fix-r-1 commits=core:1 stack:1"
	if got != want {
		t.Errorf("events:\n%s\nwant:\n%s", got, want)
	}
}

func TestJobNoneAndHarnesses(t *testing.T) {
	r := newRig(t, 4)
	r.start()
	var ids []string
	for _, h := range []string{"opencode", "pi", "antigravity"} {
		rc := rec("r-"+h, h, h, "")
		rc.SpaceDir = r.f.Home
		r.submit(rc)
		ids = append(ids, rc.ID)
	}
	for _, id := range ids {
		r.waitState(id, Completed, 10*time.Second)
	}
	names := map[string]testfake.Started{}
	for _, s := range r.f.Starts() {
		names[s.Name] = s
	}
	if names["opencode"].Env["OPENCODE_CONFIG_CONTENT"] == "" || names["opencode"].Argv[0] != "run" {
		t.Errorf("opencode %+v", names["opencode"].Argv)
	}
	if names["pi"].Argv[0] != "-p" || names["agy"].Argv[0] != "-p" {
		t.Errorf("pi %v agy %v", names["pi"].Argv, names["agy"].Argv)
	}
	for _, s := range names {
		if s.Cwd != r.f.Home && !strings.HasSuffix(s.Cwd, "/home") {
			t.Errorf("%s cwd %s", s.Name, s.Cwd)
		}
		if !strings.HasSuffix(s.Env["TMPDIR"], "/tmp") || !strings.Contains(s.Env["TMPDIR"], "/runs/") {
			t.Errorf("%s TMPDIR %s", s.Name, s.Env["TMPDIR"])
		}
	}
	if calls := r.f.AnfsCalls(); len(calls) != 0 {
		t.Errorf("job: none called anfs: %v", calls)
	}
}

func TestRetryReusesTheJob(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("fail-once")
	r.start()
	rc := rec("fix", "maintainer", "claude", "fix-r-2", "core")
	rc.Retries = 1
	r.submit(rc)
	done := r.waitState(rc.ID, Completed, 10*time.Second)
	if done.Attempt != 2 {
		t.Errorf("attempt %d", done.Attempt)
	}
	n := 0
	for _, c := range r.f.AnfsCalls() {
		if strings.HasPrefix(c, "work new") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("work new %d times: %v", n, r.f.AnfsCalls())
	}
	got := strings.Join(r.events_("reason", "retry"), "\n")
	if got != "run.submitted\nrun.working\nrun.failed reason=exit retry=1 of 1\nrun.working\nrun.completed" {
		t.Errorf("events:\n%s", got)
	}
}

func TestForeignJobFails(t *testing.T) {
	r := newRig(t, 2)
	os.MkdirAll(filepath.Join(r.f.Work, "fix-r-3"), 0o755)
	r.start()
	rc := rec("fix", "maintainer", "claude", "fix-r-3", "core")
	rc.Retries = 2
	r.submit(rc)
	done := r.waitState(rc.ID, Failed, 5*time.Second)
	if done.Reason != "job" || !strings.Contains(done.Detail, "exists that this run did not make") || len(r.f.Starts()) != 0 {
		t.Errorf("%+v", done)
	}
	// anfs work new failing is a job failure too
	r.f.Touch("x", "")
	rc2 := rec("fix", "maintainer", "claude", "bad", "core")
	os.WriteFile(filepath.Join(r.f.Work, "bad"), []byte("a file in the way"), 0o644)
	r.submit(rc2)
	if d := r.waitState(rc2.ID, Failed, 5*time.Second); d.Reason != "job" {
		t.Errorf("%+v", d)
	}
}

func TestSpawnFailure(t *testing.T) {
	r := newRig(t, 2)
	r.start()
	rc := rec("r", "m", "nonesuch", "")
	r.submit(rc)
	if d := r.waitState(rc.ID, Failed, 5*time.Second); d.Reason != "spawn" {
		t.Errorf("%+v", d)
	}
}

func working(r *rig) (int, map[string]int) {
	recs, _ := r.sup.Store.All()
	n, per := 0, map[string]int{}
	for _, rc := range recs {
		if rc.State == Working {
			n++
			per[rc.Role]++
		}
	}
	return n, per
}

func TestConcurrencyPerRoleAndHost(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("sleep 1")
	r.start()
	var all []*Record
	for i := 0; i < 3; i++ {
		rc := rec("a", "solo", "claude", "")
		all = append(all, rc)
		r.submit(rc)
	}
	r.sup.SetConcurrency(sid, "duo", 2)
	for i := 0; i < 3; i++ {
		rc := rec("b", "duo", "claude", "")
		rc.Concurrency = 2
		all = append(all, rc)
		r.submit(rc)
	}
	maxTotal, maxSolo := 0, 0
	deadline := time.Now().Add(20 * time.Second)
	for {
		n, per := working(r)
		maxTotal, maxSolo = max(maxTotal, n), max(maxSolo, per["solo"])
		ended := 0
		recs, _ := r.sup.Store.All()
		for _, rc := range recs {
			if Ended(rc.State) {
				ended++
			}
		}
		if ended == len(all) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all ended:\n%s", r.logs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if maxTotal != 2 || maxSolo != 1 {
		t.Errorf("at most %d working (host 2), %d of solo (1)", maxTotal, maxSolo)
	}
	if w, q := r.sup.Counts(); w != 0 || q != 0 {
		t.Errorf("counts %d %d", w, q)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func pidOf(t *testing.T, f *testfake.Fakes, file string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, s := range f.Starts() {
			if b, err := os.ReadFile(filepath.Join(s.Dir, file)); err == nil && len(strings.TrimSpace(string(b))) > 0 {
				n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
				return n
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s recorded", file)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTimeoutKillsTheGroup(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("child")
	r.start()
	rc := rec("r", "m", "claude", "")
	rc.Timeout = "1s"
	r.submit(rc)
	child := pidOf(t, r.f, "child")
	fake := pidOf(t, r.f, "pid")
	d := r.waitState(rc.ID, Failed, 10*time.Second)
	if d.Reason != "timeout" {
		t.Errorf("%+v", d)
	}
	deadline := time.Now().Add(3 * time.Second)
	for alive(child) || alive(fake) {
		if time.Now().After(deadline) {
			t.Fatalf("left alive: child %v, fake %v", alive(child), alive(fake))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// anfs agents stop: the marker, then SIGTERM to the group; anfsd says canceled
func TestCancelMarker(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("child")
	r.start()
	rc := rec("r", "m", "claude", "")
	r.submit(rc)
	w := r.waitState(rc.ID, Working, 5*time.Second)
	child := pidOf(t, r.f, "child")
	os.WriteFile(filepath.Join(r.sup.Store.RunDir(rc.ID), "cancel"), []byte("shell:person\n"), 0o600)
	syscall.Kill(-w.Pgid, syscall.SIGTERM)
	d := r.waitState(rc.ID, Canceled, 5*time.Second)
	if d.By != "shell:person" || alive(child) && func() bool { time.Sleep(time.Second); return alive(child) }() {
		t.Errorf("%+v child alive %v", d, alive(child))
	}
	if got := strings.Join(r.events_("by"), "\n"); got != "run.submitted\nrun.working\nrun.canceled by=shell:person" {
		t.Errorf("events:\n%s", got)
	}
	// a queued run with a marker is canceled before it starts
	r.f.Mode("ok")
	rc2 := rec("r", "m", "claude", "")
	os.MkdirAll(r.sup.Store.RunDir(rc2.ID), 0o700)
	os.WriteFile(filepath.Join(r.sup.Store.RunDir(rc2.ID), "cancel"), []byte("x\n"), 0o600)
	r.submit(rc2)
	if d := r.waitState(rc2.ID, Canceled, 5*time.Second); d.Started != "" {
		t.Errorf("started %+v", d)
	}
}

// anfsd restarted: a live run adopted (and finished from its exit file when it ends), a dead one
// orphaned, a queued one started
func TestRestartAdoptsAndOrphans(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("sleep 1")
	r.start()
	rc := rec("r", "m", "claude", "")
	r.submit(rc)
	r.waitState(rc.ID, Working, 5*time.Second)
	r.cancel() // anfsd stops: the run goes on
	r.sup.Wait()
	// a dead working run, and a queued one, as a crash would leave them
	dead := rec("r", "m", "claude", "")
	dead.State, dead.Pid, dead.Started, dead.Submitted = Working, 999999, now(), now()
	r.sup.Store.Save(dead)
	queued := rec("r", "m", "claude", "")
	queued.State, queued.Submitted, queued.Attempt = Submitted, now(), 1
	r.sup.Store.Save(queued)
	os.WriteFile(filepath.Join(r.sup.Store.RunDir(queued.ID), "event.json"), []byte(line+"\n"), 0o600)

	r.sup = r.newSup(r.sup.Store.Dir, 2)
	r.start()
	if !strings.Contains(r.logs.String(), "run "+rc.ID+" (r) adopted") {
		t.Errorf("not adopted:\n%s", r.logs)
	}
	if d := r.waitState(dead.ID, Failed, 5*time.Second); d.Reason != "orphaned" {
		t.Errorf("%+v", d)
	}
	if d := r.waitState(rc.ID, Completed, 10*time.Second); *d.Exit != 0 {
		t.Errorf("%+v", d)
	}
	r.waitState(queued.ID, Completed, 10*time.Second)
}

// an adopted run past its timeout: its group is ended, the run failed (timeout)
func TestAdoptedTimeout(t *testing.T) {
	r := newRig(t, 2)
	r.f.Mode("child")
	r.start()
	rc := rec("r", "m", "claude", "")
	rc.Timeout = "1s"
	r.submit(rc)
	r.waitState(rc.ID, Working, 5*time.Second)
	child := pidOf(t, r.f, "child")
	r.cancel()
	r.sup.Wait()
	r.sup = r.newSup(r.sup.Store.Dir, 2)
	r.start()
	if d := r.waitState(rc.ID, Failed, 10*time.Second); d.Reason != "timeout" {
		t.Errorf("%+v", d)
	}
	time.Sleep(200 * time.Millisecond)
	if alive(child) {
		t.Error("child alive")
	}
}

func TestGuardsBookkeeping(t *testing.T) {
	r := newRig(t, 1)
	r.f.Mode("sleep 2")
	r.start()
	a := rec("r", "m", "claude", "")
	r.submit(a)
	if !r.sup.Has(sid, "r", a.Event) || r.sup.Has(sid, "other", a.Event) {
		t.Error("Has")
	}
	if n := r.sup.Starts(sid, "r", time.Now().Add(-time.Minute)); n != 1 {
		t.Errorf("starts %d", n)
	}
	if !r.sup.IsRunOf(sid, "r", "claude:"+a.Last8()) || r.sup.IsRunOf(sid, "r", "shell:me") || r.sup.IsRunOf(sid, "x", "claude:"+a.Last8()) {
		t.Error("IsRunOf")
	}
	r.waitState(a.ID, Working, 5*time.Second)
	// one working (host max 1), none waiting: the next waits (queue 1 allows it)
	if r.sup.Full(sid, "m", 1, 1) {
		t.Error("full with an empty queue")
	}
	b := rec("r", "m", "claude", "")
	r.submit(b)
	if !r.sup.Full(sid, "m", 1, 1) || r.sup.Full(sid, "m", 1, 2) {
		t.Error("queue of 1 with one waiting")
	}
	rej := rec("r", "m", "claude", "")
	if err := r.sup.Reject(rej, []byte(line), "queue", "full"); err != nil {
		t.Fatal(err)
	}
	if d, _ := r.sup.Store.Load(rej.ID); d.State != Rejected || d.Reason != "queue" {
		t.Errorf("%+v", d)
	}
	if n := r.sup.Starts(sid, "r", time.Now().Add(-time.Minute)); n != 2 {
		t.Errorf("a rejection counted against rate: %d", n)
	}
	// both runs end before the temp dirs go: a run outlives its supervisor, and its wrapper would
	// write the exit status into a directory being removed (a flaky cleanup)
	r.waitState(a.ID, Completed, 10*time.Second)
	r.waitState(b.ID, Completed, 10*time.Second)
}

func TestParseEnv(t *testing.T) {
	v, err := ParseEnv("unset ANFS_WORK ANFS_WORK_SAID\nexport A=plain\nexport B=/a\\ b\\'c\nexport C=$'x\\ny'\n")
	if err != nil || v["A"] != "plain" || v["B"] != "/a b'c" || v["C"] != "x\ny" {
		t.Errorf("%v %q", err, v)
	}
	if _, err := ParseEnv("rm -rf /\n"); err == nil {
		t.Error("not an export line accepted")
	}
}

func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }
