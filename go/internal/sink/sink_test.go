package sink

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
	"github.com/anfs-io/runtime/go/internal/testfake"
	"github.com/anfs-io/runtime/go/internal/testnats"
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

func (g *logs) all() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return strings.Join(g.l, "\n")
}

func sinkCalls(f *testfake.Fakes) []string {
	ents, _ := os.ReadDir(f.Log)
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "sink.") {
			b, _ := os.ReadFile(filepath.Join(f.Log, e.Name()))
			out = append(out, strings.TrimSpace(string(b)))
		}
	}
	return out
}

func setup(t *testing.T) (*testfake.Fakes, *bus.Bus, context.Context) {
	f := testfake.New(t)
	f.Setenv(t)
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
	return f, b, ctx
}

func publish(t *testing.T, ctx context.Context, b *bus.Bus, typ, subject string) []byte {
	l, ev, err := cloudevent.Line(cloudevent.New{Scope: "abcde23456", Type: typ, Subject: subject, Actor: "shell:t",
		Data: map[string]string{"to": "file"}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.JetStream().Publish(ctx, ev.NatsSubject(), l); err != nil {
		t.Fatal(err)
	}
	return l
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

// each report.* event on anfs report __sink's stdin, in order; other events never
func TestRendersEachReportEvent(t *testing.T) {
	f, b, ctx := setup(t)
	g := &logs{}
	s := &Sink{Anfs: f.Anfs, Logf: g.logf}
	go s.Run(ctx, b)
	waitFor(t, "the consumer", func() bool {
		_, err := b.JetStream().Consumer(ctx, bus.Stream, Consumer)
		return err == nil
	})
	l1 := publish(t, ctx, b, "report.filed", "r-1")
	publish(t, ctx, b, "source.updated", "core")
	l2 := publish(t, ctx, b, "report.closed", "r-1")
	waitFor(t, "two renders", func() bool { return len(sinkCalls(f)) == 2 })
	time.Sleep(200 * time.Millisecond)
	calls := sinkCalls(f)
	if len(calls) != 2 || !strings.Contains(strings.Join(calls, "\n"), strings.TrimSpace(string(l1))) || !strings.Contains(strings.Join(calls, "\n"), strings.TrimSpace(string(l2))) {
		t.Errorf("calls %v", calls)
	}
	c, _ := b.JetStream().Consumer(ctx, bus.Stream, Consumer)
	ci, _ := c.Info(ctx)
	if ci.Config.FilterSubject != Filter || ci.Config.MaxAckPending != 1 || ci.Config.MaxDeliver != MaxDeliver {
		t.Errorf("config %+v", ci.Config)
	}
}

// a failure is delivered again; after the 5th it is logged and dropped
func TestFailureRedeliveredThenLogged(t *testing.T) {
	f, b, ctx := setup(t)
	g := &logs{}
	f.Touch("sink-fail", "2") // the first two fail
	s := &Sink{Anfs: f.Anfs, Logf: g.logf, Delay: func(int) time.Duration { return 50 * time.Millisecond }}
	go s.Run(ctx, b)
	waitFor(t, "the consumer", func() bool {
		_, err := b.JetStream().Consumer(ctx, bus.Stream, Consumer)
		return err == nil
	})
	publish(t, ctx, b, "report.filed", "r-2")
	waitFor(t, "3 tries, the last acked", func() bool {
		c, err := b.JetStream().Consumer(ctx, bus.Stream, Consumer)
		if err != nil {
			return false
		}
		ci, err := c.Info(ctx)
		return err == nil && len(sinkCalls(f)) == 3 && ci.NumAckPending == 0 && ci.NumPending == 0 && ci.AckFloor.Consumer > 0
	})
	if !strings.Contains(g.all(), "render failed (as told)") || !strings.Contains(g.all(), "try 2 of 5") {
		t.Errorf("log:\n%s", g.all())
	}
	// always failing: 5 tries, then given up
	f.Touch("sink-fail", "1000")
	publish(t, ctx, b, "report.filed", "r-3")
	waitFor(t, "given up", func() bool { return strings.Contains(g.all(), "given up after 5 tries") })
	time.Sleep(300 * time.Millisecond)
	if n := len(sinkCalls(f)); n != 3+5 {
		t.Errorf("%d tries in all, want 8:\n%s\nlog:\n%s", n, strings.Join(sinkCalls(f), "\n"), g.all())
	}
}
