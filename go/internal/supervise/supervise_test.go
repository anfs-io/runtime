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

// Ended_ is waitState's "any final state"
const Ended_ = "(ended)"

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
		if err == nil && (rc.State == state || state == Ended_ && Ended(rc.State)) {
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
		"TMPDIR": job + "/.tmp", "ROUNDTRIP_SRC_ROOT": "/a path",
	} {
		if s.Env[k] != v {
			t.Errorf("env %s=%q, want %q", k, s.Env[k], v)
		}
	}
	for _, k := range []string{"ANFS_WORK", "FAKE_CTL", "GIT_CONFIG_GLOBAL"} {
		if _, ok := s.Env[k]; ok {
			t.Errorf("%s reached the run (not in the allow-list)", k)
		}
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

// The run's environment is the allow-list plus anfsd's names (02d, F7): no SSH agent, no tokens,
// no ANFSD_*; claude alone gets its credential; git asks nobody and uses no helper
func TestRunEnvAllowList(t *testing.T) {
	r := newRig(t, 4)
	// a gitconfig in HOME whose helper would hand out a password, as osxkeychain would
	os.WriteFile(filepath.Join(r.f.Home, ".gitconfig"), []byte("[credential]\n\thelper = \"!f() { echo username=u; echo password=leaked; }; f\"\n"), 0o644)
	r.f.Touch("script", `printf 'protocol=https\nhost=example.com\n\n' | git credential fill > "$FAKE_LOG/fill.$$" 2>&1
echo "fill exit $?" >> "$FAKE_LOG/fill.$$"
`)
	r.f.Mode("script")
	r.sup.Env = append(r.sup.Env,
		"SSH_AUTH_SOCK=/tmp/agent.sock", "GH_TOKEN=gh1", "GITHUB_TOKEN=gh2", "AWS_SECRET_ACCESS_KEY=aws",
		"ANTHROPIC_API_KEY=sk-ant", "CLAUDE_CODE_OAUTH_TOKEN=oat", "ANFSD_MAX_RUNS=9", "ANFS_SOURCES_HOME=/srcs",
		"ANFS_WORK=job", "ANFS_EVENTS_SPOOL=/elsewhere", "GIT_ASKPASS=/bin/askpass", "LC_ALL=C", "TMPDIR=/anfsd/tmp",
		"FAKE_LOG="+r.f.Log)
	r.start()
	c := rec("r", "m", "claude", "")
	c.SpaceDir = r.f.Home
	o := rec("o", "o", "opencode", "fix-env", "core")
	r.submit(c)
	r.submit(o)
	for _, id := range []string{c.ID, o.ID} {
		if d := r.waitState(id, Ended_, 10*time.Second); d.State != Completed {
			out, _ := os.ReadFile(filepath.Join(r.sup.Store.RunDir(id), "out.log"))
			so, _ := filepath.Glob(filepath.Join(r.f.Log, "*", "script.out"))
			var sb []byte
			for _, f := range so {
				b, _ := os.ReadFile(f)
				sb = append(sb, b...)
			}
			t.Fatalf("%s: %s %v\n%s\n%s", d.Harness, d.State, d.Exit, out, sb)
		}
	}
	byName := map[string]testfake.Started{}
	for _, s := range r.f.Starts() {
		byName[s.Name] = s
	}
	allowed := map[string]bool{"PATH": true, "HOME": true, "LC_ALL": true, "TMPDIR": true, "ANFS_SOURCES_HOME": true,
		"ANFS_EVENTS": true, "ANFS_LOG": true, "ANFS_ACTOR": true, "ANFS_AS": true, "ANFS_RUN": true, "ANFS_EVENT": true,
		"ANFS_EVENTS_SPOOL": true, "GIT_TERMINAL_PROMPT": true, "GIT_CONFIG_COUNT": true, "GIT_CONFIG_KEY_0": true,
		"GIT_CONFIG_VALUE_0": true, "GIT_CONFIG_KEY_1": true, "GIT_CONFIG_VALUE_1": true,
		// set by the shell the wrapper and the fake run in, or by the job (anfs work env)
		"PWD": true, "SHLVL": true, "OLDPWD": true, "_": true,
		"ANFS_WORK_JOB": true, "ANFS_WORK_NUMBER": true, "ROUNDTRIP_SRC_ROOT": true,
		// the harness's own
		"OPENCODE_CONFIG_CONTENT": true, "ANTHROPIC_API_KEY": true, "CLAUDE_CODE_OAUTH_TOKEN": true,
		"CLAUDE_CODE_SUBPROCESS_ENV_SCRUB": true, "OPENCODE_DISABLE_PROJECT_CONFIG": true, "OPENCODE_DISABLE_CLAUDE_CODE": true,
		"OPENCODE_DISABLE_EXTERNAL_SKILLS": true, "OPENCODE_PURE": true,
	}
	for name, s := range byName {
		for k := range s.Env {
			if !allowed[k] {
				t.Errorf("%s: %s reached the run", name, k)
			}
		}
		if s.Env["ANFS_SOURCES_HOME"] != "/srcs" || s.Env["LC_ALL"] != "C" || s.Env["GIT_TERMINAL_PROMPT"] != "0" {
			t.Errorf("%s: %v", name, s.Env)
		}
		if s.Env["TMPDIR"] == "/anfsd/tmp" {
			t.Errorf("%s: anfsd's TMPDIR", name)
		}
	}
	cl, oc := byName["claude"].Env, byName["opencode"].Env
	if cl["ANTHROPIC_API_KEY"] != "sk-ant" || cl["CLAUDE_CODE_OAUTH_TOKEN"] != "oat" {
		t.Errorf("claude lacks its credential: %v", cl)
	}
	if _, ok := oc["ANTHROPIC_API_KEY"]; ok {
		t.Error("opencode got claude's credential")
	}
	if cl["GIT_CONFIG_COUNT"] != "1" || cl["GIT_CONFIG_KEY_0"] != "credential.helper" {
		t.Errorf("git: %v", cl)
	}
	// git asked nobody and no helper answered, in both
	fills, _ := filepath.Glob(filepath.Join(r.f.Log, "fill.*"))
	if len(fills) != 2 {
		t.Fatalf("%d fills", len(fills))
	}
	for _, f := range fills {
		b, _ := os.ReadFile(f)
		if strings.Contains(string(b), "leaked") || strings.Contains(string(b), "fill exit 0") {
			t.Errorf("a credential came back: %s", b)
		}
	}
	// anfsd's own anfs calls get the same list
	if b, err := os.ReadFile(filepath.Join(r.f.Log, "anfs.env")); err == nil {
		for _, bad := range []string{"SSH_AUTH_SOCK", "GH_TOKEN", "ANFSD_MAX_RUNS", "ANTHROPIC_API_KEY"} {
			if strings.Contains(string(b), bad+"=") {
				t.Errorf("anfs got %s", bad)
			}
		}
		if !strings.Contains(string(b), "ANFS_SOURCES_HOME=/srcs") || !strings.Contains(string(b), "GIT_TERMINAL_PROMPT=0") {
			t.Errorf("anfs env:\n%s", b)
		}
	} else {
		t.Error(err)
	}
}

// the job's GIT_CONFIG_COUNT is kept: the reset goes after it
func TestGitNoCredentialsAfterTheJobs(t *testing.T) {
	env := gitNoCredentials([]string{"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.hooksPath", "GIT_CONFIG_VALUE_0=/h"})
	want := "GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/h GIT_TERMINAL_PROMPT=0 GIT_CONFIG_KEY_1=credential.helper GIT_CONFIG_VALUE_1="
	if strings.Join(env, " ") != want {
		t.Errorf("%v", env)
	}
}
