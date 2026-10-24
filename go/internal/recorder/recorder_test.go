package recorder

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
	"github.com/anfs-io/runtime/go/internal/testnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type logs struct {
	mu sync.Mutex
	l  []string
}

func (g *logs) logf(f string, a ...any) {
	g.mu.Lock()
	g.l = append(g.l, fmt.Sprintf(f, a...))
	g.mu.Unlock()
}

func (g *logs) count(sub string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, s := range g.l {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

func example(t *testing.T, f string) []byte {
	b, err := os.ReadFile(filepath.Join("..", "cloudevent", "testdata", "spec", "examples", f))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func read(t *testing.T, p string) string {
	b, err := os.ReadFile(p)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b)
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("waited for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The rules one message at a time: a direct publish is recorded once, canonical; what the spool
// holds is known; a second recorder (anfsd restarted after the append, before the ack) finds it;
// invalid and mismatched messages are terminated with one log line each
func TestRecord(t *testing.T) {
	dir := t.TempDir()
	g := &logs{}
	r := &Recorder{Events: dir, Logf: g.logf}
	nc := example(t, "valid/noncanonical.jsonl") // anfs.host.anfs.source.updated, in October 2026
	body := []byte(strings.TrimSuffix(string(nc), "\n"))
	if a, err := r.Record("anfs.host.anfs.source.updated", body); a != Recorded || err != nil {
		t.Fatalf("%s %v", a, err)
	}
	canon, _ := cloudevent.Canonical(nc)
	bus := filepath.Join(dir, "host", "2026-10.bus.jsonl")
	if got := read(t, bus); got != string(canon) {
		t.Errorf("recorded:\n%s", got)
	}
	if a, _ := r.Record("anfs.host.anfs.source.updated", body); a != Known {
		t.Errorf("again: %s", a)
	}
	if a, _ := (&Recorder{Events: dir}).Record("anfs.host.anfs.source.updated", body); a != Known || read(t, bus) != string(canon) {
		t.Errorf("after a restart: %s", a)
	}
	// the spool's line, as the shipper publishes it: known, nothing written; a cnfs one in its scope
	spool := filepath.Join(dir, "tkj76y3nfx", "2026-10.jsonl")
	os.MkdirAll(filepath.Dir(spool), 0o755)
	rc := example(t, "valid/report-closed.jsonl")
	os.WriteFile(spool, rc, 0o644)
	if a, _ := r.Record("anfs.tkj76y3nfx.anfs.report.closed", rc[:len(rc)-1]); a != Known {
		t.Errorf("a spool line: %s", a)
	}
	// a line appended to the spool after the month was first read is found too
	rf := example(t, "valid/as-role.jsonl")
	f, _ := os.OpenFile(spool, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write(rf)
	f.Close()
	if a, _ := r.Record("anfs.tkj76y3nfx.anfs.report.filed", rf); a != Known {
		t.Errorf("a spool line appended later: %s", a)
	}
	if _, err := os.Stat(filepath.Join(dir, "tkj76y3nfx", "2026-10.bus.jsonl")); err == nil {
		t.Error("a spool line recorded")
	}
	cn := example(t, "valid/cnfs-partitia-report-filed.jsonl")
	if a, _ := r.Record("anfs.m2vq7cfr4k.cnfs.partitia.report.filed", cn); a != Recorded || read(t, filepath.Join(dir, "m2vq7cfr4k", "2026-10.bus.jsonl")) != string(cn) {
		t.Errorf("cnfs: %s", a)
	}
	// invalid, and on another subject than its event's
	if a, _ := r.Record("anfs.tkj76y3nfx.anfs.report.filed", example(t, "invalid/anfs-data-nested.jsonl")); a != Terminated || g.count("anfs.data.flat") != 1 {
		t.Errorf("invalid: %s %v", a, g.l)
	}
	if a, _ := r.Record("anfs.m2vq7cfr4k.anfs.partitia.report.filed", cn); a != Terminated || g.count("its event's subject is anfs.m2vq7cfr4k.cnfs.partitia.report.filed (subject.mismatch)") != 1 {
		t.Errorf("mismatched: %s %v", a, g.l)
	}
	if a, _ := r.Record("anfs.host.anfs.x.y", []byte(strings.Repeat("x", 4096))); a != Terminated {
		t.Errorf("4096 bytes: %s", a)
	}
}

// On the bus: the consumer anfsd-recorder delivers everything from the start; a direct publish
// lands once in <scope>/<YYYY-MM>.bus.jsonl (also published again past the duplicate window, as
// another message), a spool-shipped one never; an invalid message is terminated, said once and
// never delivered again
func TestRecorderOnTheBus(t *testing.T) {
	srv := testnats.Start(t, nil)
	b, err := bus.Connect(bus.Options{URL: srv.URL()}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.EnsureStream(ctx, bus.StreamSettings{MaxAge: time.Hour, MaxBytes: 1 << 20, DupWindow: time.Minute}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	js := b.JetStream()
	pub := func(subject string, body []byte, id string) {
		m := nats.NewMsg(subject)
		m.Data = []byte(strings.TrimSuffix(string(body), "\n"))
		m.Header.Set(jetstream.MsgIDHeader, id)
		m.Header.Set("Content-Type", "application/cloudevents+json")
		if _, err := js.PublishMsg(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	// before the consumer exists: delivered all the same
	cn := example(t, "valid/cnfs-chatter-message-posted.jsonl")
	ev, _ := cloudevent.Parse(cn)
	pub(ev.NatsSubject(), cn, ev.ID)
	pub(ev.NatsSubject(), cn, ev.ID+"-again") // past the duplicate window: another message
	// a spool line the shipper published
	sp := example(t, "valid/report-closed.jsonl")
	os.MkdirAll(filepath.Join(dir, "tkj76y3nfx"), 0o755)
	os.WriteFile(filepath.Join(dir, "tkj76y3nfx", "2026-10.jsonl"), sp, 0o644)
	spe, _ := cloudevent.Parse(sp)
	pub(spe.NatsSubject(), sp, spe.ID)
	pub("anfs.host.anfs.report.filed", example(t, "invalid/time-not-id.jsonl"), "bad-1")

	g := &logs{}
	r := &Recorder{Events: dir, Logf: g.logf}
	go r.Run(ctx, b)
	var c jetstream.Consumer
	waitFor(t, "the consumer", func() bool {
		c, err = js.Consumer(ctx, bus.Stream, Consumer)
		return err == nil
	})
	waitFor(t, "all handled", func() bool {
		ci, err := c.Info(ctx)
		return err == nil && ci.NumPending == 0 && ci.NumAckPending == 0 && ci.AckFloor.Stream == 4
	})
	ci, _ := c.Info(ctx)
	if ci.Config.DeliverPolicy != jetstream.DeliverAllPolicy || ci.Config.FilterSubject != "anfs.>" || ci.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("config %+v", ci.Config)
	}
	recorded := read(t, filepath.Join(dir, "m2vq7cfr4k", "2026-10.bus.jsonl"))
	if recorded != string(cn) {
		t.Errorf("recorded: %q", recorded)
	}
	if _, err := os.Stat(filepath.Join(dir, "tkj76y3nfx", "2026-10.bus.jsonl")); err == nil {
		t.Error("the spool's line was recorded")
	}
	time.Sleep(300 * time.Millisecond)
	if n := g.count("recorder: anfs.host.anfs.report.filed: not recorded"); n != 1 || g.count("time.id") != 1 {
		t.Errorf("said %d times: %v", n, g.l)
	}
	// a direct publish while it runs
	cp := example(t, "valid/cnfs-host.jsonl")
	ce, _ := cloudevent.Parse(cp)
	pub(ce.NatsSubject(), cp, ce.ID)
	waitFor(t, "the host's record", func() bool { return read(t, filepath.Join(dir, "host", ce.Month()+".bus.jsonl")) == string(cp) })
}
