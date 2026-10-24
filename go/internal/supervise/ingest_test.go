package supervise

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
)

// a line of the profile in scope (a space id or host), with the given id's last digits and actor
// attributes, at now
func spoolLine(scope string, n int, typ, actorAttrs string) string {
	src := "/anfs/host"
	if scope != "host" {
		src = "/anfs/spaces/" + scope
	}
	t := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	return fmt.Sprintf(`{"specversion":"1.0","id":"%s","source":"%s","type":"io.anfs.%s","subject":"r-1","time":"%s","datacontenttype":"application/json",%s,"anfsv":1,"data":{"n":"%d"}}`,
		lineID(n), src, typ, t, actorAttrs, n)
}

// a UUIDv7 of now's milliseconds ending in n
func lineID(n int) string {
	ms := time.Now().UnixMilli()
	return fmt.Sprintf("%08x-%04x-7000-8000-%012x", ms>>16, ms&0xffff, n)
}

func month() string { return time.Now().UTC().Format("2006-01") }

// the space's log, in order: every line parsed (each must be valid)
func (r *rig) logEvents() []cloudevent.Event {
	r.t.Helper()
	var out []cloudevent.Event
	fh, err := os.Open(filepath.Join(r.events, sid, month()+".jsonl"))
	if err != nil {
		return nil
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		e, err := cloudevent.Parse(sc.Bytes())
		if err != nil {
			r.t.Fatalf("not a valid line: %v: %s", err, sc.Text())
		}
		out = append(out, e)
	}
	return out
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	os.MkdirAll(filepath.Dir(path), 0o700)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(s)
	f.Close()
}

// A run's three lines: honest, forged, run.completed. In the log: the first stamped (actorrun, no
// claim), the second stamped with its claim, the third refused and counted; ids kept, data last;
// the run's own events before its run.completed
func TestRunSpoolStamped(t *testing.T) {
	r := newRig(t, 2)
	honest := spoolLine(sid, 1, "report.closed", `"actor":"claude:ACTOR8","actorspace":"`+sid+`","actorrole":"m","spacepath":"anfs-dev"`)
	forged := spoolLine(sid, 2, "report.closed", `"actor":"shell:person","actorrole":"person"`)
	runDone := spoolLine(sid, 3, "run.completed", `"actor":"claude:ACTOR8"`)
	r.f.Touch("script", `mkdir -p "$ANFS_EVENTS_SPOOL/`+sid+`" && sed "s/ACTOR8/${ANFS_ACTOR#claude:}/" "$FAKE_CTL/lines" >> "$ANFS_EVENTS_SPOOL/`+sid+`/`+month()+`.jsonl"`)
	r.f.Touch("lines", honest+"\n"+forged+"\n"+runDone+"\n")
	r.f.Mode("script")
	r.start()
	rc := rec("r", "m", "claude", "")
	rc.SpaceDir = r.f.Home
	r.submit(rc)
	done := r.waitState(rc.ID, Completed, 10*time.Second)
	if done.EventsIngested != 2 || done.EventsRefused != 1 {
		t.Errorf("ingested %d refused %d", done.EventsIngested, done.EventsRefused)
	}
	// the harness got its spool
	s := r.f.Starts()[0]
	if s.Env["ANFS_EVENTS_SPOOL"] != r.sup.Store.SpoolDir(rc.ID) || !strings.Contains(s.Argv[len(s.Argv)-1], "Events you emit go through `$ANFS_EVENTS_SPOOL`") {
		t.Errorf("env %s", s.Env["ANFS_EVENTS_SPOOL"])
	}
	if st, _ := os.Stat(r.sup.Store.SpoolDir(rc.ID)); st.Mode().Perm() != 0o700 {
		t.Errorf("spool mode %v", st.Mode())
	}
	var got []string
	for _, e := range r.logEvents() {
		got = append(got, fmt.Sprintf("%s %s run=%v claimed=%q", e.Short(), e.Actor, e.ActorRun == rc.ID, e.ActorClaimed))
	}
	actor := "claude:" + rc.Last8()
	want := []string{
		"run.submitted anfsd run=false claimed=\"\"",
		"run.working anfsd run=false claimed=\"\"",
		"report.closed " + actor + " run=true claimed=\"\"",
		"report.closed " + actor + " run=true claimed=\"shell:person as /person\"",
		"run.completed anfsd run=false claimed=\"\"",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("log:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// byte for byte: the honest line with actorrun after actorrole, the rest kept
	b, _ := os.ReadFile(filepath.Join(r.events, sid, month()+".jsonl"))
	wantLine := strings.Replace(strings.Replace(honest, "ACTOR8", rc.Last8(), 1), `"actorrole":"m",`, `"actorrole":"m","actorrun":"`+rc.ID+`",`, 1)
	if !strings.Contains(string(b), wantLine+"\n") {
		t.Errorf("no %s in\n%s", wantLine, b)
	}
}

// Refused: a scope directory that is not the line's source, a symlinked file, past the maximum,
// over 4096 once stamped, not an event; a partial line waits for its newline
func TestRunSpoolRefusals(t *testing.T) {
	r := newRig(t, 2)
	r.sup.RunEventsMax = 3
	if err := r.sup.Open(); err != nil {
		t.Fatal(err)
	}
	rc := rec("r", "m", "claude", "")
	r.sup.Store.Save(rc)
	sp, err := r.sup.makeSpool(rc)
	if err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(sp, sid, month()+".jsonl")
	a := `"actor":"x"`
	appendTo(t, f, spoolLine(sid, 1, "a.b", a)+"\n"+spoolLine(sid, 2, "a.b", a))
	r.sup.drain(rc)
	if ev := r.logEvents(); len(ev) != 1 {
		t.Fatalf("a partial line went in: %d", len(ev))
	}
	appendTo(t, f, "\n")
	// in host/: a line whose source is the space; a symlinked month file; not an event
	appendTo(t, filepath.Join(sp, "host", month()+".jsonl"), spoolLine(sid, 3, "a.b", a)+"\nnot an event\n")
	other := filepath.Join(t.TempDir(), "x.jsonl")
	appendTo(t, other, spoolLine(sid, 4, "a.b", a)+"\n")
	os.Symlink(other, filepath.Join(sp, sid, "2020-01.jsonl"))
	// over 4096 once stamped (valid as written: 4095 bytes)
	big := spoolLine(sid, 5, "a.b", a)
	big = strings.Replace(big, `"n":"5"`, `"n":"5`+strings.Repeat("x", cloudevent.Max-len(big)-1)+`"`, 1)
	if _, err := cloudevent.Parse([]byte(big)); err != nil {
		t.Fatal(err)
	}
	appendTo(t, f, big+"\n")
	// two more: the third ingested reaches the maximum, the fourth is past it
	appendTo(t, f, spoolLine(sid, 6, "a.b", a)+"\n"+spoolLine(sid, 7, "a.b", a)+"\n")
	r.sup.drainFinal(rc)
	var ids []string
	for _, e := range r.logEvents() {
		ids = append(ids, e.ID[len(e.ID)-1:])
	}
	if strings.Join(ids, " ") != "1 2 6" {
		t.Errorf("in the log: %v", ids)
	}
	if rc.EventsIngested != 3 || rc.EventsRefused != 4 {
		t.Errorf("ingested %d refused %d\n%s", rc.EventsIngested, rc.EventsRefused, r.logs)
	}
	for _, w := range []string{"its source is /anfs/spaces/" + sid + ", in the directory host", "not an event", "once stamped", "past ANFSD_RUN_EVENTS_MAX (3)"} {
		if !strings.Contains(r.logs.String(), w) {
			t.Errorf("log lacks %q:\n%s", w, r.logs)
		}
	}
	if n := r.sup.Store.Pending(rc.ID); n != 0 {
		t.Errorf("%d bytes pending", n)
	}
}

// A crash between the append and the cursor's rename: the cursors put back, a new supervisor
// drains again, and each id is in the log once (the tail check)
func TestRunSpoolCrashRepeat(t *testing.T) {
	r := newRig(t, 2)
	r.sup.Open()
	rc := rec("r", "m", "claude", "")
	rc.State, rc.Pid, rc.Ended = Completed, 0, now()
	r.sup.Store.Save(rc)
	sp, _ := r.sup.makeSpool(rc)
	for i := 1; i <= 5; i++ {
		appendTo(t, filepath.Join(sp, sid, month()+".jsonl"), spoolLine(sid, i, "a.b", `"actor":"x"`)+"\n")
	}
	cursors := filepath.Join(r.sup.Store.RunDir(rc.ID), "cursors.json")
	r.sup.drain(rc)
	os.Remove(cursors) // the crash: the lines appended, the cursors not written
	if n := r.sup.Store.Pending(rc.ID); n == 0 {
		t.Fatal("nothing pending after the crash")
	}
	// anfsd starts again: the run ended a moment ago, its spool is past its cursors
	r.sup = r.newSup(r.sup.Store.Dir, 2)
	r.start()
	seen := map[string]int{}
	for _, e := range r.logEvents() {
		seen[e.ID]++
	}
	if len(seen) != 5 {
		t.Errorf("%d ids: %v", len(seen), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s %d times", id, n)
		}
	}
	if d, _ := r.sup.Store.Load(rc.ID); d.EventsIngested != 0 || d.EventsRefused != 0 {
		// the repeats are neither ingested again nor refused
		t.Errorf("%+v", d)
	}
}

// finishDead: the run left its exit status and a line while anfsd was down: the line is in the
// log before its run.completed
func TestRunSpoolFinishDead(t *testing.T) {
	r := newRig(t, 2)
	rc := rec("r", "m", "claude", "")
	rc.State, rc.Pid, rc.Started, rc.Submitted, rc.Attempt = Working, 999999, now(), now(), 1
	r.sup.Store.Save(rc)
	os.WriteFile(filepath.Join(r.sup.Store.RunDir(rc.ID), "exit"), []byte("0\n"), 0o600)
	sp := r.sup.Store.SpoolDir(rc.ID)
	appendTo(t, filepath.Join(sp, sid, month()+".jsonl"), spoolLine(sid, 1, "report.closed", `"actor":"x"`)+"\n")
	r.start()
	d := r.waitState(rc.ID, Completed, 5*time.Second)
	var got []string
	for _, e := range r.logEvents() {
		got = append(got, e.Short())
	}
	if strings.Join(got, " ") != "report.closed run.completed" || d.EventsIngested != 1 {
		t.Errorf("%v %+v", got, d)
	}
}

// adopted: anfsd restarted mid-run; the run then writes one more line and ends; both lines are
// in the log once, before its run.completed
func TestRunSpoolAdopted(t *testing.T) {
	r := newRig(t, 2)
	r.f.Touch("script", `f="$ANFS_EVENTS_SPOOL/`+sid+`/`+month()+`.jsonl"
mkdir -p "${f%/*}"
sed -n 1p "$FAKE_CTL/lines" >> "$f"
while [ ! -f "$FAKE_CTL/go" ]; do sleep 0.1; done
sed -n 2p "$FAKE_CTL/lines" >> "$f"`)
	r.f.Touch("lines", spoolLine(sid, 1, "a.b", `"actor":"x"`)+"\n"+spoolLine(sid, 2, "a.c", `"actor":"x"`)+"\n")
	r.f.Mode("script")
	r.start()
	rc := rec("r", "m", "claude", "")
	rc.SpaceDir = r.f.Home
	r.submit(rc)
	r.waitState(rc.ID, Working, 5*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for len(r.logEvents()) < 3 { // submitted, working, a.b (the watcher)
		if time.Now().After(deadline) {
			t.Fatalf("the first line not ingested while working:\n%s", r.logs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.cancel()
	r.sup.Wait()
	r.sup = r.newSup(r.sup.Store.Dir, 2)
	r.start()
	r.f.Touch("go", "")
	d := r.waitState(rc.ID, Completed, 10*time.Second)
	var got []string
	for _, e := range r.logEvents() {
		got = append(got, e.Short())
	}
	if strings.Join(got, " ") != "run.submitted run.working a.b a.c run.completed" || d.EventsIngested != 2 {
		t.Errorf("%v %+v\n%s", got, d, r.logs)
	}
}
