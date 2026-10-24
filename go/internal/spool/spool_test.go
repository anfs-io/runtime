package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	os.MkdirAll(filepath.Dir(path), 0o755)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(s)
	f.Close()
}

func readAll(t *testing.T, s *Spool) []string {
	t.Helper()
	files, err := s.Files()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range files {
		ls, err := s.Read(f, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range ls {
			out = append(out, f.Key+":"+string(l.Data))
		}
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReadPartialAndCursor(t *testing.T) {
	ev, st := t.TempDir(), t.TempDir()
	s, err := Open(ev, st, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(ev, "host", "2026-10.jsonl")
	appendTo(t, f, "one\ntw")
	got := readAll(t, s)
	if !eq(got, []string{"host/2026-10.jsonl:one"}) {
		t.Fatalf("partial: %v", got)
	}
	// nothing acknowledged: read again from the start
	if got := readAll(t, s); len(got) != 1 {
		t.Fatalf("again: %v", got)
	}
	s.Advance("host/2026-10.jsonl", 4)
	if got := readAll(t, s); len(got) != 0 {
		t.Fatalf("past the cursor, a partial line only: %v", got)
	}
	appendTo(t, f, "o\nthree\n")
	if got := readAll(t, s); !eq(got, []string{"host/2026-10.jsonl:two", "host/2026-10.jsonl:three"}) {
		t.Fatalf("the newline came: %v", got)
	}
	// never backwards
	s.Advance("host/2026-10.jsonl", 2)
	if s.Cursor("host/2026-10.jsonl") != 4 {
		t.Fatal("moved back")
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	// the cursors survive a restart
	s2, err := Open(ev, st, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if s2.Cursor("host/2026-10.jsonl") != 4 {
		t.Fatalf("after a restart: %d", s2.Cursor("host/2026-10.jsonl"))
	}
	b, _ := os.ReadFile(filepath.Join(st, "cursors.json"))
	if string(b) != "{\"host/2026-10.jsonl\":4}\n" {
		t.Errorf("cursors.json: %s", b)
	}
}

func TestMonthsScopesLateLines(t *testing.T) {
	ev, st := t.TempDir(), t.TempDir()
	s, _ := Open(ev, st, t.Logf)
	appendTo(t, filepath.Join(ev, "host", "2026-09.jsonl"), "sep\n")
	appendTo(t, filepath.Join(ev, "host", "2026-10.jsonl"), "oct\n")
	appendTo(t, filepath.Join(ev, "abcde23456", "2026-10.jsonl"), "space\n")
	appendTo(t, filepath.Join(ev, "host", "notes.txt"), "not a month\n")
	got := readAll(t, s)
	want := []string{"abcde23456/2026-10.jsonl:space", "host/2026-09.jsonl:sep", "host/2026-10.jsonl:oct"}
	if !eq(got, want) {
		t.Fatalf("%v", got)
	}
	files, _ := s.Files()
	for _, f := range files {
		ls, _ := s.Read(f, 100)
		for _, l := range ls {
			s.Advance(f.Key, l.End)
		}
	}
	// a late line in last month's file is found
	appendTo(t, filepath.Join(ev, "host", "2026-09.jsonl"), "late\n")
	if got := readAll(t, s); !eq(got, []string{"host/2026-09.jsonl:late"}) {
		t.Fatalf("late: %v", got)
	}
}

func TestShorterFileReadAgain(t *testing.T) {
	ev, st := t.TempDir(), t.TempDir()
	s, _ := Open(ev, st, t.Logf)
	p := filepath.Join(ev, "host", "2026-10.jsonl")
	appendTo(t, p, "a\nb\n")
	s.Advance("host/2026-10.jsonl", 4)
	os.WriteFile(p, []byte("c\n"), 0o644)
	if got := readAll(t, s); !eq(got, []string{"host/2026-10.jsonl:c"}) {
		t.Fatalf("%v", got)
	}
}

func TestReadMax(t *testing.T) {
	ev, st := t.TempDir(), t.TempDir()
	s, _ := Open(ev, st, t.Logf)
	p := filepath.Join(ev, "host", "2026-10.jsonl")
	appendTo(t, p, "a\nb\nc\n")
	files, _ := s.Files()
	ls, _ := s.Read(files[0], 2)
	if len(ls) != 2 || ls[1].Start != 2 || ls[1].End != 4 {
		t.Fatalf("%+v", ls)
	}
}

// The watcher wakes once at its start, on an append to an existing file and on a new scope; a burst is one wake-up
func TestWatch(t *testing.T) {
	ev := t.TempDir()
	appendTo(t, filepath.Join(ev, "host", "2026-10.jsonl"), "a\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := Watch(ctx, ev, time.Hour, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	wait := func(what string) {
		t.Helper()
		select {
		case <-w.C:
		case <-time.After(5 * time.Second):
			t.Fatalf("no wake-up for %s", what)
		}
	}
	drain := func() {
		time.Sleep(200 * time.Millisecond)
		for {
			select {
			case <-w.C:
			default:
				return
			}
		}
	}
	wait("what was there before the watch (a run's first line, written before its spool is watched)")
	drain()
	appendTo(t, filepath.Join(ev, "host", "2026-10.jsonl"), "b\n")
	wait("an append")
	drain()
	appendTo(t, filepath.Join(ev, "abcde23456", "2026-10.jsonl"), "c\n")
	wait("a new scope")
	drain()
	// the new scope is watched: an append to it wakes too
	appendTo(t, filepath.Join(ev, "abcde23456", "2026-10.jsonl"), "d\n")
	wait("an append in the new scope")
	drain()
	appendTo(t, filepath.Join(ev, "abcde23456", "2026-11.jsonl"), "e\n")
	wait("a new month")
}
