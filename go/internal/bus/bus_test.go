package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/testnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestBackoff(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := Backoff(i + 1); got != w {
			t.Errorf("attempt %d: %v", i+1, got)
		}
	}
}

func connect(t *testing.T, s *testnats.Server, user, pw string) *Bus {
	t.Helper()
	b, err := Connect(Options{URL: s.URL(), User: user, Password: pw, ReconnectDelay: func(int) time.Duration { return 100 * time.Millisecond }}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	if err := b.WaitConnected(ctx(t)); err != nil {
		t.Fatal(err)
	}
	return b
}

// ANFS is created with the settings, and its limits are updated when they change
func TestEnsureStream(t *testing.T) {
	s := testnats.Start(t, testnats.Users{"anfsd": "pw"})
	b := connect(t, s, "anfsd", "pw")
	if !b.NeedsEnsure() {
		t.Fatal("a new connection needs the stream made sure of")
	}
	set := StreamSettings{MaxAge: time.Hour, MaxBytes: 1 << 20, DupWindow: 10 * time.Minute}
	if err := b.EnsureStream(ctx(t), set); err != nil {
		t.Fatal(err)
	}
	if b.NeedsEnsure() {
		t.Error("still needs it")
	}
	info, err := b.StreamInfo(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	c := info.Config
	if c.Name != "ANFS" || len(c.Subjects) != 1 || c.Subjects[0] != "anfs.>" || c.Storage != jetstream.FileStorage ||
		c.Retention != jetstream.LimitsPolicy || c.Discard != jetstream.DiscardOld || c.MaxAge != time.Hour ||
		c.MaxBytes != 1<<20 || c.Duplicates != 10*time.Minute {
		t.Fatalf("config %+v", c)
	}
	set.MaxAge, set.MaxBytes = 2*time.Hour, 2<<20
	if err := b.EnsureStream(ctx(t), set); err != nil {
		t.Fatal(err)
	}
	info, _ = b.StreamInfo(ctx(t))
	if info.Config.MaxAge != 2*time.Hour || info.Config.MaxBytes != 2<<20 {
		t.Errorf("not updated: %+v", info.Config)
	}
}

// Another stream holding anfs.> is reported by name, and ANFS is not made
func TestForeignStream(t *testing.T) {
	s := testnats.Start(t, nil)
	b := connect(t, s, "", "")
	if _, err := b.JetStream().CreateStream(ctx(t), jetstream.StreamConfig{Name: "OTHER", Subjects: []string{"anfs.host.>"}}); err != nil {
		t.Fatal(err)
	}
	err := b.EnsureStream(ctx(t), StreamSettings{MaxAge: time.Hour, MaxBytes: 1 << 20, DupWindow: time.Minute})
	if !errors.Is(err, ErrForeignStream) || err.Error() != "another stream holds anfs.>: OTHER (remove it, or give it other subjects)" {
		t.Fatalf("err %v", err)
	}
	if _, err := b.StreamInfo(ctx(t)); err == nil {
		t.Error("ANFS was made")
	}
}

// A publish carries Nats-Msg-Id and the CloudEvents content type; the same id twice is stored once
func TestPublishDedup(t *testing.T) {
	s := testnats.Start(t, nil)
	b := connect(t, s, "", "")
	if err := b.EnsureStream(ctx(t), StreamSettings{MaxAge: time.Hour, MaxBytes: 1 << 20, DupWindow: time.Hour}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		f, err := b.Publish("anfs.host.anfs.x.y", []byte(`{"a":1}`), "id-1")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-f.Ok():
		case err := <-f.Err():
			t.Fatal(err)
		case <-time.After(5 * time.Second):
			t.Fatal("no ack")
		}
	}
	info, _ := b.StreamInfo(ctx(t))
	if info.State.Msgs != 1 {
		t.Fatalf("%d messages", info.State.Msgs)
	}
	st, _ := b.JetStream().Stream(ctx(t), "ANFS")
	m, err := st.GetMsg(ctx(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Get(nats.MsgIdHdr) != "id-1" || m.Header.Get("Content-Type") != "application/cloudevents+json" || m.Subject != "anfs.host.anfs.x.y" {
		t.Errorf("message %+v", m)
	}
}

// NATS down at the start: Connect returns, WaitConnected blocks until it is up
func TestConnectLater(t *testing.T) {
	s := testnats.Start(t, nil)
	s.Stop()
	b, err := Connect(Options{URL: s.URL(), ReconnectDelay: func(int) time.Duration { return 100 * time.Millisecond }}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Connected() {
		t.Fatal("connected to nothing")
	}
	short, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := b.WaitConnected(short); err == nil {
		t.Fatal("WaitConnected returned with NATS down")
	}
	s.Restart()
	if err := b.WaitConnected(ctx(t)); err != nil {
		t.Fatal(err)
	}
}

// A wrong password never connects
func TestAuth(t *testing.T) {
	s := testnats.Start(t, testnats.Users{"anfsd": "right"})
	b, err := Connect(Options{URL: s.URL(), User: "anfsd", Password: "wrong", ReconnectDelay: func(int) time.Duration { return 100 * time.Millisecond }}, func(string, ...any) {})
	if err != nil {
		return
	}
	defer b.Close()
	short, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if b.WaitConnected(short) == nil {
		t.Fatal("connected with a wrong password")
	}
}
