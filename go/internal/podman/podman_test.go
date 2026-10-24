package podman

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/testfake"
)

// testdata/events.jsonl: `podman events --format json` recorded on this Mac (podman 6.1.1, a
// podman machine), 2026-10-23: a busybox container with exec noise that died 137, and pcm's
// glitchtip coming up and going down in its pod (health checks, exec, sync, cleanup). Home paths
// in the labels are /Users/person.
func fixture(t *testing.T) []byte {
	b, err := os.ReadFile("testdata/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// every event in the host scope, in order
func spooled(t *testing.T, events string) []cloudevent.Event {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(events, "host", "*.jsonl"))
	var out []cloudevent.Event
	for _, f := range files {
		b, _ := os.ReadFile(f)
		sc := bufio.NewScanner(bytes.NewReader(b))
		for sc.Scan() {
			e, err := cloudevent.Parse(sc.Bytes())
			if err != nil {
				t.Fatalf("an invalid line in the spool: %v: %s", err, sc.Text())
			}
			out = append(out, e)
		}
	}
	return out
}

type logs struct {
	mu sync.Mutex
	l  []string
}

func (l *logs) f(format string, a ...any) {
	l.mu.Lock()
	l.l = append(l.l, fmt.Sprintf(format, a...))
	l.mu.Unlock()
}

func (l *logs) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.l...)
}

func watcher(t *testing.T) (*Watcher, *logs) {
	lg := &logs{}
	w := &Watcher{Events: filepath.Join(t.TempDir(), "events"), StateFile: filepath.Join(t.TempDir(), "podman.json"), Logf: lg.f}
	w.defaults()
	return w, lg
}

func brief(e cloudevent.Event) string {
	keys := []string{"id", "image", "pod", "exit", "health", "pcm", "error"}
	var d []string
	for _, k := range keys {
		if v, ok := e.Data[k]; ok {
			d = append(d, k+"="+v)
		}
	}
	return e.Short() + " " + e.Subject + " " + strings.Join(d, " ")
}

// Each kept status is one valid line of the host scope with its type, subject and data; the
// rest (exec, exec_died, attach, init, sync, cleanup) none; a health check only when it changes
func TestRecordedLines(t *testing.T) {
	w, _ := watcher(t)
	for _, l := range bytes.Split(fixture(t), []byte("\n")) {
		if err := w.Handle(l); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for _, e := range spooled(t, w.Events) {
		if e.Scope != "host" || e.Actor != "podman" || e.Source != "/anfs/host" {
			t.Errorf("event %+v", e)
		}
		got = append(got, brief(e))
	}
	const img = "image=docker.io/glitchtip/glitchtip:6 pod=7980cbfa1207"
	want := []string{
		"container.created gifted_kirch id=8b4335b2bbe9 image=docker.io/library/busybox:1.37",
		"container.started gifted_kirch id=8b4335b2bbe9 image=docker.io/library/busybox:1.37",
		"container.died gifted_kirch id=8b4335b2bbe9 image=docker.io/library/busybox:1.37 exit=137",
		"container.removed gifted_kirch id=8b4335b2bbe9 image=docker.io/library/busybox:1.37",
		"pod.created pod_glitchtip id=b6d0d1b8a6a6",
		"container.created glitchtip_web_1 id=2f820df40bb2 " + img + " pcm=glitchtip",
		"container.started glitchtip_web_1 id=2f820df40bb2 " + img + " pcm=glitchtip",
		"container.health glitchtip_web_1 id=2f820df40bb2 " + img + " health=starting pcm=glitchtip",
		"container.health glitchtip_web_1 id=2f820df40bb2 " + img + " health=healthy pcm=glitchtip",
		"container.died glitchtip_web_1 id=2f820df40bb2 image=docker.io/glitchtip/glitchtip:6 exit=0 pcm=glitchtip",
		"container.removed glitchtip_web_1 id=2f820df40bb2 " + img + " pcm=glitchtip",
		"pod.removed pod_glitchtip id=b6d0d1b8a6a6",
	}
	// the pod's id is the recording's
	for i := range want {
		if strings.HasPrefix(want[i], "pod.") && i < len(got) && strings.HasPrefix(got[i], "pod.") {
			want[i] = want[i][:strings.Index(want[i], "id=")] + got[i][strings.Index(got[i], "id="):]
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// base: the lines' seconds count from 1000 s ago, so their events are of now's month
var base = time.Now().Unix() - 1000

func line(id, name, status string, sec int64) []byte {
	b, _ := json.Marshal(map[string]any{"ID": id, "Name": name, "Status": status, "Type": "container", "time": base + sec,
		"Image": "img", "ContainerExitCode": 0, "Attributes": map[string]string{}})
	return b
}

// Statuses that are noise, and other types, write nothing; a line that is not JSON is an error
func TestDropped(t *testing.T) {
	w, _ := watcher(t)
	for _, s := range []string{"exec", "exec_died", "attach", "init", "sync", "cleanup", "mount", "unmount", "checkpoint", "rename"} {
		w.Handle(line("aaaaaaaaaaaaaaaa", "c", s, 1))
	}
	w.Handle([]byte(`{"ID":"x","Name":"img","Status":"pull","Type":"image","time":1}`))
	w.Handle([]byte(`{"ID":"x","network":"n","Status":"create","Type":"network","time":1}`))
	w.Handle([]byte(`{"ID":"x","Name":"p","Status":"pause","Type":"pod","time":1}`))
	if ev := spooled(t, w.Events); len(ev) != 0 {
		t.Errorf("%d written", len(ev))
	}
	if err := w.Handle([]byte("Error: not json")); err == nil {
		t.Error("no error for a line that is not JSON")
	}
}

// The same container, status and second are one event; a burst over the rate is cut at it and
// the rest counted, logged once
func TestCoalesceAndCap(t *testing.T) {
	w, lg := watcher(t)
	w.DropReport = 300 * time.Millisecond
	for i := 0; i < 50; i++ {
		w.Handle(line("aaaaaaaaaaaaaaaa", "loop", "start", 100))
	}
	if n := len(spooled(t, w.Events)); n != 1 {
		t.Fatalf("50 starts in a second: %d events", n)
	}
	// the next second is another event
	w.Handle(line("aaaaaaaaaaaaaaaa", "loop", "start", 101))
	// a restart loop of many containers: at most the rate's burst (20) goes through
	for i := 0; i < 200; i++ {
		w.Handle(line(fmt.Sprintf("%016d", i), "c", "died", 102))
	}
	n := len(spooled(t, w.Events))
	// the rate's burst is 20 in all: the two before took two of it
	if n < 19 || n > 22 {
		t.Errorf("a burst of 200: %d events in all (want about 20)", n)
	}
	time.Sleep(600 * time.Millisecond)
	var drops []string
	for _, l := range lg.all() {
		if strings.Contains(l, "not recorded in the last") {
			drops = append(drops, l)
		}
	}
	if len(drops) != 1 || !strings.Contains(drops[0], fmt.Sprintf("podman: %d events over 20 a second", 200-(n-2))) {
		t.Errorf("drop log: %q", drops)
	}
}

func TestBackoff(t *testing.T) {
	want := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute, time.Minute, time.Minute}
	for i, d := range want {
		if got := Backoff(i + 1); got != d {
			t.Errorf("after %d failed: %v, want %v", i+1, got, d)
		}
	}
}

func readState(t *testing.T, w *Watcher) State {
	var s State
	b, _ := os.ReadFile(w.StateFile)
	json.Unmarshal(b, &s)
	return s
}

func waitFor(t *testing.T, what string, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func types(evs []cloudevent.Event) string {
	var s []string
	for _, e := range evs {
		s = append(s, e.Short())
	}
	return strings.Join(s, " ")
}

// The fake podman prints recorded lines, stays a moment, exits: podman.connected, the events,
// podman.disconnected; then after the backoff it is started again and connects again
func TestChildExitsAndComesBack(t *testing.T) {
	f := testfake.New(t)
	f.Setenv(t)
	lines := string(line("bbbbbbbbbbbbbbbb", "web", "start", 200)) + "\n"
	f.Podman("run 1", lines)
	w, lg := watcher(t)
	w.Bin = filepath.Join(f.Bin, "podman")
	w.Settle = 200 * time.Millisecond
	var tries []int
	var mu sync.Mutex
	w.Backoff = func(n int) time.Duration {
		mu.Lock()
		tries = append(tries, n)
		mu.Unlock()
		return 300 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, "two exits", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(tries) >= 2
	})
	cancel()
	<-done
	got := types(spooled(t, w.Events))
	// a second "start" of the same container in the same second, from the restarted child, is
	// coalesced too
	if !strings.HasPrefix(got, "podman.connected container.started podman.disconnected podman.connected") {
		t.Errorf("events: %s", got)
	}
	for _, e := range spooled(t, w.Events) {
		if e.Short() == "podman.disconnected" && e.Data["error"] != "Error: unexpected EOF" {
			t.Errorf("disconnected's error %q", e.Data["error"])
		}
	}
	// a connected child resets nothing until it has been up a minute: the tries count on
	mu.Lock()
	if len(tries) < 2 || tries[0] != 1 || tries[1] != 2 {
		t.Errorf("backoff asked for %v", tries)
	}
	mu.Unlock()
	if s := strings.Join(lg.all(), "\n"); !strings.Contains(s, "podman connected: ") || !strings.Contains(s, "podman away: Error: unexpected EOF") {
		t.Errorf("log:\n%s", s)
	}
}

// Podman away (the machine stopped): never connected, no events, the backoff climbs, one log
// line however many tries, the state says the error and the next try; killed cleanly
func TestAway(t *testing.T) {
	f := testfake.New(t)
	f.Setenv(t)
	w, lg := watcher(t)
	w.Bin = filepath.Join(f.Bin, "podman")
	w.Backoff = func(n int) time.Duration { return 100 * time.Millisecond }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, "five tries", 10*time.Second, func() bool { return f.PodmanStarts() >= 5 })
	s := readState(t, w)
	cancel()
	<-done
	if len(spooled(t, w.Events)) != 0 {
		t.Errorf("events while away: %s", types(spooled(t, w.Events)))
	}
	if s.State != "away" || !strings.Contains(s.Error, "Cannot connect to Podman") || s.Next == "" {
		t.Errorf("state %+v", s)
	}
	if l := lg.all(); len(l) != 1 || !strings.Contains(l[0], "podman away: Error: Cannot connect to Podman") {
		t.Errorf("log: %q", l)
	}
}

// Not installed: no child at all, the state says so
func TestNotInstalled(t *testing.T) {
	w, lg := watcher(t)
	w.Bin = filepath.Join(t.TempDir(), "podman")
	w.Backoff = func(int) time.Duration { return time.Hour }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, "away", 5*time.Second, func() bool { return readState(t, w).State == "away" })
	cancel()
	<-done
	if s := readState(t, w); !strings.Contains(s.Error, "not found on PATH") {
		t.Errorf("state %+v", s)
	}
	if l := lg.all(); len(l) != 1 {
		t.Errorf("log %q", l)
	}
}

// Off: the state says off, nothing runs
func TestOff(t *testing.T) {
	w, _ := watcher(t)
	w.Off()
	if s := readState(t, w); s.State != "off" {
		t.Errorf("state %+v", s)
	}
}

// An event's time is podman's (timeNano, else time), not when anfsd read the line
func TestPodmansTime(t *testing.T) {
	w, _ := watcher(t)
	w.Handle([]byte(`{"ID":"8b4335b2bbe937af42fd11842715f2ecd8edfee0ea34817b6b792070d6ccf080","Image":"busybox","Name":"k","Status":"start","time":1792703550,"timeNano":1792703550431703474,"Type":"container","Attributes":{}}`))
	w.Handle([]byte(`{"ID":"9b4335b2bbe937af42fd11842715f2ecd8edfee0ea34817b6b792070d6ccf080","Image":"busybox","Name":"j","Status":"start","time":1792703551,"Type":"container","Attributes":{}}`))
	evs := spooled(t, w.Events)
	if len(evs) != 2 || evs[0].Time != "2026-10-22T21:12:30.431Z" || evs[1].Time != "2026-10-22T21:12:31.000Z" {
		t.Errorf("%+v", evs)
	}
}
