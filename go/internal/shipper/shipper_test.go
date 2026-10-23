package shipper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/spool"
	"github.com/anfs-io/runtime/go/internal/testnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const space = "abcde23456"

type logs struct {
	mu    sync.Mutex
	lines []string
	t     *testing.T
}

func (l *logs) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := fmt.Sprintf(f, a...)
	l.lines = append(l.lines, s)
	l.t.Log(s)
}

func (l *logs) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

type rig struct {
	t      *testing.T
	srv    *testnats.Server
	events string
	state  string
	bus    *bus.Bus
	sh     *Shipper
	log    *logs
}

func fast(int) time.Duration { return 100 * time.Millisecond }

func newRig(t *testing.T) *rig {
	r := &rig{t: t, srv: testnats.Start(t, nil), events: t.TempDir(), state: t.TempDir(), log: &logs{t: t}}
	r.open()
	return r
}

// open makes the spool, the bus and the shipper again (a restart of anfsd)
func (r *rig) open() {
	if r.bus != nil {
		r.bus.Close()
	}
	sp, err := spool.Open(r.events, r.state, r.log.logf)
	if err != nil {
		r.t.Fatal(err)
	}
	b, err := bus.Connect(bus.Options{URL: r.srv.URL(), ReconnectDelay: fast}, r.log.logf)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(b.Close)
	r.bus = b
	r.sh = &Shipper{Spool: sp, Bus: b, Stream: bus.StreamSettings{MaxAge: time.Hour, MaxBytes: 64 << 20, DupWindow: time.Hour},
		Rate: 10000, Burst: 10000, Window: 256, Logf: r.log.logf, Backoff: fast}
}

func (r *rig) emit(scope string, n int) []string {
	var ids []string
	for i := 0; i < n; i++ {
		ev, err := cloudevent.Append(r.events, cloudevent.New{Scope: scope, Type: "report.filed", Subject: fmt.Sprintf("r-%d", i), Actor: "t"})
		if err != nil {
			r.t.Fatal(err)
		}
		ids = append(ids, ev.ID)
	}
	return ids
}

func (r *rig) msgs() uint64 {
	r.t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.bus.WaitConnected(c); err != nil {
		r.t.Fatal(err)
	}
	info, err := r.bus.StreamInfo(c)
	if err != nil {
		return 0
	}
	return info.State.Msgs
}

func (r *rig) drain() int {
	r.t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.bus.WaitConnected(c); err != nil {
		r.t.Fatal(err)
	}
	n, err := r.sh.Drain(c)
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// N events are N messages, on anfs.<scope>.<type>, each with its id as Nats-Msg-Id
func TestShipsEach(t *testing.T) {
	r := newRig(t)
	hostIDs := r.emit("host", 300)
	spaceIDs := r.emit(space, 5)
	if n := r.drain(); n != 305 {
		t.Fatalf("shipped %d", n)
	}
	if m := r.msgs(); m != 305 {
		t.Fatalf("%d messages", m)
	}
	c := context.Background()
	st, _ := r.bus.JetStream().Stream(c, bus.Stream)
	got := map[string]string{}
	for seq := uint64(1); seq <= 305; seq++ {
		m, err := st.GetMsg(c, seq)
		if err != nil {
			t.Fatal(err)
		}
		got[m.Header.Get(nats.MsgIdHdr)] = m.Subject
	}
	for _, id := range hostIDs {
		if got[id] != "anfs.host.report.filed" {
			t.Fatalf("%s on %q", id, got[id])
		}
	}
	for _, id := range spaceIDs {
		if got[id] != "anfs."+space+".report.filed" {
			t.Fatalf("%s on %q", id, got[id])
		}
	}
	// the cursors are at the files' ends, on disk
	b, _ := os.ReadFile(filepath.Join(r.state, "cursors.json"))
	for _, f := range mustFiles(t, r) {
		fi, _ := os.Stat(f.Path)
		if !strings.Contains(string(b), fmt.Sprintf("%q:%d", f.Key, fi.Size())) {
			t.Errorf("cursor of %s not at %d: %s", f.Key, fi.Size(), b)
		}
	}
	// nothing more to ship
	if n := r.drain(); n != 0 {
		t.Errorf("shipped %d again", n)
	}
}

func mustFiles(t *testing.T, r *rig) []spool.File {
	fs, err := r.sh.Spool.Files()
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

// Acked but the cursors not written (a crash between): shipped again, still N in the stream
func TestCrashBeforeCursorWrite(t *testing.T) {
	r := newRig(t)
	r.emit("host", 20)
	r.drain()
	os.Remove(filepath.Join(r.state, "cursors.json"))
	r.open()
	if n := r.drain(); n != 20 {
		t.Fatalf("resent %d", n)
	}
	if m := r.msgs(); m != 20 {
		t.Fatalf("%d messages: the duplicate window did not drop them", m)
	}
}

// NATS down at the start, then up: the backlog ships
func TestNatsDownAtStart(t *testing.T) {
	r := newRig(t)
	r.srv.Stop()
	r.open()
	r.emit("host", 30)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	wake := make(chan struct{}, 1)
	go func() { done <- r.sh.Run(ctx, wake) }()
	time.Sleep(500 * time.Millisecond)
	if r.sh.Shipped() != 0 {
		t.Fatal("shipped with NATS down")
	}
	r.srv.Restart()
	eventually(t, "the backlog shipped", func() bool { return r.sh.Shipped() == 30 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m := r.msgs(); m != 30 {
		t.Fatalf("%d messages", m)
	}
	if r.log.count("shipping waits for NATS") != 1 {
		t.Errorf("the wait was not said once: %v", r.log.lines)
	}
}

// NATS restarted while events keep coming: nothing lost, nothing twice
func TestNatsRestartMidStream(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w, err := spool.Watch(ctx, r.events, time.Second, r.log.logf)
	if err != nil {
		t.Fatal(err)
	}
	go func() { done <- r.sh.Run(ctx, w.C) }()
	total := 0
	for round := 0; round < 6; round++ {
		r.emit("host", 25)
		r.emit(space, 5)
		total += 30
		if round == 2 {
			r.srv.Restart()
		}
		time.Sleep(100 * time.Millisecond)
	}
	eventually(t, "everything in the stream", func() bool {
		c, cl := context.WithTimeout(context.Background(), time.Second)
		defer cl()
		if !r.bus.Connected() {
			return false
		}
		info, err := r.bus.StreamInfo(c)
		return err == nil && info.State.Msgs == uint64(total)
	})
	// and stays so: nothing sent twice past the window
	time.Sleep(300 * time.Millisecond)
	if m := r.msgs(); m != uint64(total) {
		t.Fatalf("%d messages, %d events", m, total)
	}
	cancel()
	<-done
}

// The token bucket per scope holds a flood back
func TestRateLimit(t *testing.T) {
	r := newRig(t)
	r.sh.Rate, r.sh.Burst = 20, 5
	r.emit("host", 25)
	start := time.Now()
	if n := r.drain(); n != 25 {
		t.Fatalf("shipped %d", n)
	}
	// 5 at once, 20 more at 20 a second
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("25 events in %v: the limit did not hold", d)
	}
}

// A line that is not an event is not sent: logged once, the cursor past it
func TestInvalidLine(t *testing.T) {
	r := newRig(t)
	r.emit("host", 2)
	f := filepath.Join(r.events, "host", time.Now().UTC().Format("2006-01")+".jsonl")
	fh, _ := os.OpenFile(f, os.O_WRONLY|os.O_APPEND, 0)
	fh.WriteString("not an event\n" + `{"specversion":"1.0","anfsv":2}` + "\n")
	fh.Close()
	r.emit("host", 2)
	if n := r.drain(); n != 4 {
		t.Fatalf("shipped %d", n)
	}
	if c := r.log.count("not shipped, not an event: host/"); c != 2 {
		t.Fatalf("logged %d times: %v", c, r.log.lines)
	}
	r.emit("host", 1)
	r.drain()
	r.open()
	r.drain()
	if c := r.log.count("not shipped, not an event"); c != 2 {
		t.Fatalf("logged again: %v", r.log.lines)
	}
	if m := r.msgs(); m != 5 {
		t.Fatalf("%d messages", m)
	}
}

// With NATS gone the cursor stays where the acks stopped
func TestCursorNeverAheadOfAck(t *testing.T) {
	r := newRig(t)
	r.emit("host", 3)
	r.drain()
	key := mustFiles(t, r)[0].Key
	before := r.sh.Spool.Cursor(key)
	r.srv.Stop()
	r.emit("host", 3)
	if _, err := r.sh.Drain(context.Background()); err == nil {
		t.Fatal("drained with NATS down")
	}
	if r.sh.Spool.Cursor(key) != before {
		t.Fatalf("cursor moved from %d to %d with no ack", before, r.sh.Spool.Cursor(key))
	}
	r.srv.Restart()
	r.drain()
	if m := r.msgs(); m != 6 {
		t.Fatalf("%d messages", m)
	}
}

// A foreign stream on anfs.>: nothing shipped, said once, retried
func TestForeignStreamStops(t *testing.T) {
	r := newRig(t)
	c := context.Background()
	if err := r.bus.WaitConnected(c); err != nil {
		t.Fatal(err)
	}
	if _, err := r.bus.JetStream().CreateStream(c, jetstream.StreamConfig{Name: "OTHER", Subjects: []string{"anfs.>"}}); err != nil {
		t.Fatal(err)
	}
	r.emit("host", 2)
	ctx, cancel := context.WithCancel(c)
	done := make(chan error, 1)
	go func() { done <- r.sh.Run(ctx, make(chan struct{})) }()
	time.Sleep(600 * time.Millisecond)
	cancel()
	<-done
	if r.sh.Shipped() != 0 {
		t.Fatal("shipped onto a foreign stream")
	}
	if n := r.log.count("another stream holds anfs.>: OTHER"); n != 1 {
		t.Fatalf("said %d times: %v", n, r.log.lines)
	}
}
