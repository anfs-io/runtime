// Package spool is the event log as anfsd reads it: $XDG_STATE_HOME/anfs/events/<scope>/<YYYY-MM>.jsonl,
// written by core's lib/anfs/events.sh (and by anfsd itself, through cloudevent.Append), one event
// per line, never rewritten. The spool finds the complete lines past each month file's cursor, and
// keeps the cursors: $XDG_STATE_HOME/anfsd/cursors.json, {"<scope>/<YYYY-MM>.jsonl": <byte offset>},
// rewritten (a temp file, fsync, rename) only when the shipper says how far NATS acknowledged.
//
// Every month file has its own cursor, so a line written to last month's file after this month's
// appeared is found too. A file with no cursor is read from its start. A partial line (no newline
// yet) waits for its newline.
//
// Watch tells the shipper when to look: fsnotify (kqueue on macOS, inotify on Linux) on the events
// directory and each scope directory, plus a reconcile every 30 s; no other timer and no loop.
package spool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
)

// ReadMax is the most bytes read from one file at a time; a line is at most 4096 bytes.
const ReadMax = 1 << 20

var monthRE = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}\.jsonl$`)

// File is one month file of one scope.
type File struct {
	Key   string // <scope>/<YYYY-MM>.jsonl, the cursor's key
	Scope string // the directory's name: a space id, or host
	Path  string
}

// Line is one complete line of a file: its bytes without the newline, where it starts, and the
// offset just past its newline (what the cursor becomes once it is acknowledged).
type Line struct {
	File  File
	Data  []byte
	Start int64
	End   int64
}

// Spool is the events directory and the cursors.
type Spool struct {
	Dir         string
	CursorsPath string
	logf        func(string, ...any)

	mu      sync.Mutex
	cursors map[string]int64
}

// Open reads the cursors (none yet is every file from its start).
func Open(eventsDir, stateDir string, logf func(string, ...any)) (*Spool, error) {
	s := &Spool{Dir: eventsDir, CursorsPath: filepath.Join(stateDir, "cursors.json"), logf: logf, cursors: map[string]int64{}}
	b, err := os.ReadFile(s.CursorsPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(b, &s.cursors); err != nil {
			return nil, fmt.Errorf("%s: %w", s.CursorsPath, err)
		}
	}
	return s, nil
}

// Files are the month files, by scope, each scope's oldest first.
func (s *Spool) Files() ([]File, error) {
	scopes, err := os.ReadDir(s.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []File
	for _, d := range scopes {
		if !d.IsDir() {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(s.Dir, d.Name()))
		if err != nil {
			continue
		}
		var names []string
		for _, e := range ents {
			if e.Type().IsRegular() && monthRE.MatchString(e.Name()) {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, n := range names {
			out = append(out, File{Key: d.Name() + "/" + n, Scope: d.Name(), Path: filepath.Join(s.Dir, d.Name(), n)})
		}
	}
	return out, nil
}

// Cursor is how far a file has been shipped.
func (s *Spool) Cursor(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[key]
}

// Cursors is a copy of them all.
func (s *Spool) Cursors() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.cursors))
	for k, v := range s.cursors {
		out[k] = v
	}
	return out
}

// Advance moves a file's cursor (in memory; Save writes them). Never backwards.
func (s *Spool) Advance(key string, off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off > s.cursors[key] {
		s.cursors[key] = off
	}
}

// Save writes the cursors: a temp file beside them, fsync, rename.
func (s *Spool) Save() error {
	s.mu.Lock()
	b, err := json.Marshal(s.cursors)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.CursorsPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".cursors.json.")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.CursorsPath)
}

// Read returns up to max complete lines of f past its cursor. A file shorter than its cursor
// (rewritten by hand: the tools never do) is said once and read again from its start.
func (s *Spool) Read(f File, max int) ([]Line, error) {
	if max <= 0 {
		return nil, nil
	}
	fh, err := os.Open(f.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	cur := s.Cursor(f.Key)
	if st.Size() < cur {
		s.logf("%s is shorter (%d bytes) than its cursor (%d): read again from its start", f.Key, st.Size(), cur)
		s.mu.Lock()
		s.cursors[f.Key] = 0
		s.mu.Unlock()
		cur = 0
	}
	if st.Size() == cur {
		return nil, nil
	}
	n := st.Size() - cur
	if n > ReadMax {
		n = ReadMax
	}
	buf := make([]byte, n)
	got, err := fh.ReadAt(buf, cur)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:got]
	var out []Line
	off := cur
	for len(out) < max {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		out = append(out, Line{File: f, Data: buf[:i:i], Start: off, End: off + int64(i) + 1})
		off += int64(i) + 1
		buf = buf[i+1:]
	}
	// A whole read with no newline is no line the tools write (they write at most 4096 bytes):
	// handed over as one line, which the shipper refuses and steps past
	if len(out) == 0 && int64(got) == ReadMax {
		out = append(out, Line{File: f, Data: buf, Start: cur, End: cur + int64(got)})
	}
	return out, nil
}
