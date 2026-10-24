package supervise

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/spool"
)

// A run's events (agents/02d, F1 and F6). A run writes its events into its own spool,
// <run dir>/events/<scope>/<YYYY-MM>.jsonl ($ANFS_EVENTS_SPOOL, the only directory of the run's
// that it may write), never into the shared log. anfsd moves each line into the log under the
// run's identity: actor, actorspace, actorrole and actorrun stamped, a differing claim kept in
// actorclaimed. Refused (logged, counted as events_refused): a line that is not an event, run.*
// (anfsd's own), a line whose scope directory is not its source, anything past
// ANFSD_RUN_EVENTS_MAX, and a line over 4096 bytes once stamped. The run's cursors are in
// <run dir>/cursors.json (written after the lines, temp + fsync + rename), with the two counts.
//
// It drains while the run works (a spool.Watch on the directory: fsnotify and the 30 s
// reconcile, only while the run works), at its end before the final run.* (end), when it is
// finished dead (finishDead, through end), when adopted, and at start for the runs that ended in
// the last day. The first batch per run after anfsd starts skips the ids already in the last
// MiB of the month file it appends to: a crash between an append and the cursor's rename repeats
// at most one batch.

// Defaults of the ingestion.
const (
	DefaultRunEventsMax = 1000
	ingestBatch         = 256
	tailCheck           = 1 << 20
	refusalsLogged      = 10
	recentEnded         = 24 * time.Hour
)

// keys in a run's cursors.json beside the files' cursors
const (
	countIngested = "#ingested"
	countRefused  = "#refused"
)

type ingester struct {
	mu      sync.Mutex
	sp      *spool.Spool
	checked map[string]bool // spool files whose first batch since anfsd started looked for repeats
	logged  int
	stop    context.CancelFunc
	done    chan struct{}
}

// SpoolDir is a run's spool: ANFS_EVENTS_SPOOL.
func (s Store) SpoolDir(id string) string { return filepath.Join(s.RunDir(id), "events") }

func (s *Supervisor) ingesterOf(r *Record) (*ingester, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g := s.ingest[r.ID]; g != nil {
		return g, nil
	}
	sp, err := spool.Open(s.Store.SpoolDir(r.ID), s.Store.RunDir(r.ID), s.Logf)
	if err != nil {
		return nil, err
	}
	sp.NoFollow = true
	g := &ingester{sp: sp, checked: map[string]bool{}}
	s.ingest[r.ID] = g
	return g, nil
}

// makeSpool makes the run's spool directory (0700) before its harness starts
func (s *Supervisor) makeSpool(r *Record) (string, error) {
	d := s.Store.SpoolDir(r.ID)
	if err := os.MkdirAll(d, 0o700); err != nil {
		return "", err
	}
	return d, os.Chmod(d, 0o700)
}

// watchSpool drains the run's spool on every change while it works; stopped by drainFinal
func (s *Supervisor) watchSpool(r *Record) {
	g, err := s.ingesterOf(r)
	if err != nil {
		s.Logf("run %s: its spool: %v", r.ID, err)
		return
	}
	g.mu.Lock()
	if g.stop != nil {
		g.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	g.stop, g.done = cancel, make(chan struct{})
	done := g.done
	g.mu.Unlock()
	w, err := spool.Watch(ctx, s.Store.SpoolDir(r.ID), spool.Reconcile, s.Logf)
	if err != nil {
		s.Logf("run %s: watching its spool: %v (drained at its end)", r.ID, err)
		cancel()
		close(done)
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.C:
				s.drain(r)
			}
		}
	}()
}

// drainFinal stops the watcher, drains what is left, forgets the ingester and puts the counts in
// the record (the caller saves it)
func (s *Supervisor) drainFinal(r *Record) {
	if _, err := os.Stat(s.Store.SpoolDir(r.ID)); err != nil {
		return // a run that never started, or a record from before 02d
	}
	g, err := s.ingesterOf(r)
	if err != nil {
		s.Logf("run %s: its spool: %v", r.ID, err)
		return
	}
	g.mu.Lock()
	stop, done := g.stop, g.done
	g.stop, g.done = nil, nil
	g.mu.Unlock()
	if stop != nil {
		stop()
		<-done
	}
	s.drain(r)
	s.mu.Lock()
	delete(s.ingest, r.ID)
	s.mu.Unlock()
	r.EventsIngested, r.EventsRefused = int(g.sp.Cursor(countIngested)), int(g.sp.Cursor(countRefused))
}

// drain moves every complete line past the cursors into the log, batch by batch
func (s *Supervisor) drain(r *Record) {
	g, err := s.ingesterOf(r)
	if err != nil {
		s.Logf("run %s: its spool: %v", r.ID, err)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	max := s.RunEventsMax
	if max <= 0 {
		max = DefaultRunEventsMax
	}
	st := cloudevent.Stamp{Actor: r.Harness + ":" + r.Last8(), ActorSpace: r.SpaceID, ActorRole: r.Role, ActorRun: r.ID}
	for {
		files, err := g.sp.Files()
		if err != nil {
			s.Logf("run %s: its spool: %v", r.ID, err)
			return
		}
		moved := false
		for _, f := range files {
			lines, err := g.sp.Read(f, ingestBatch)
			if err != nil {
				g.refuseLog(s, r, f.Key, 0, err.Error())
				continue
			}
			if len(lines) == 0 {
				continue
			}
			var seen map[string]map[string]bool // the first batch: ids already in the target files
			if !g.checked[f.Key] {
				seen = map[string]map[string]bool{}
			}
			failed := false
			for _, l := range lines {
				res, why := s.ingestLine(r, g, st, f, l, max, seen)
				switch res {
				case appended:
					g.sp.Advance(countIngested, g.sp.Cursor(countIngested)+1)
				case refused:
					g.sp.Advance(countRefused, g.sp.Cursor(countRefused)+1)
					g.refuseLog(s, r, f.Key, l.Start, why)
				case failedAppend:
					s.Logf("run %s: %s at byte %d: %s (tried again at the next drain)", r.ID, f.Key, l.Start, why)
					failed = true
				}
				if failed {
					break
				}
				g.sp.Advance(f.Key, l.End)
			}
			g.checked[f.Key] = true
			if err := g.sp.Save(); err != nil {
				s.Logf("run %s: writing its cursors: %v", r.ID, err)
				return
			}
			if failed {
				return
			}
			moved = true
		}
		if !moved {
			return
		}
	}
}

// what became of a line
const (
	appended = iota
	refused
	repeated     // already in the log: a batch repeated after a crash
	failedAppend // the log could not be written: the cursor stays, tried again later
)

// ingestLine stamps one line and appends it to the log
func (s *Supervisor) ingestLine(r *Record, g *ingester, st cloudevent.Stamp, f spool.File, l spool.Line, max int,
	seen map[string]map[string]bool) (int, string) {
	ev, err := cloudevent.Parse(l.Data)
	switch {
	case err != nil:
		return refused, "not an event: " + err.Error()
	case strings.HasPrefix(ev.Short(), "run."):
		return refused, ev.Short() + " is anfsd's own"
	case ev.Scope != f.Scope:
		return refused, "its source is " + ev.Source + ", in the directory " + f.Scope
	case int(g.sp.Cursor(countIngested)) >= max:
		return refused, "past ANFSD_RUN_EVENTS_MAX (" + strconv.Itoa(max) + ")"
	}
	line, out, err := cloudevent.StampLine(l.Data, st)
	if err != nil {
		return refused, err.Error()
	}
	month := out.Month()
	if seen != nil {
		target := filepath.Join(s.Events, out.Scope, month+".jsonl")
		if seen[target] == nil {
			seen[target] = tailIDs(target)
		}
		if seen[target][out.ID] {
			return repeated, ""
		}
	}
	if err := cloudevent.AppendLine(s.Events, out.Scope, month, line); err != nil {
		return failedAppend, "not appended: " + err.Error()
	}
	return appended, ""
}

func (g *ingester) refuseLog(s *Supervisor, r *Record, key string, off int64, why string) {
	g.logged++
	switch {
	case g.logged <= refusalsLogged:
		s.Logf("run %s: event refused (%s at byte %d): %s", r.ID, key, off, why)
	case g.logged == refusalsLogged+1:
		s.Logf("run %s: more events refused; counted in run.json (events_refused), not logged", r.ID)
	}
}

// tailIDs are the ids of the lines in a file's last MiB
func tailIDs(path string) map[string]bool {
	ids := map[string]bool{}
	f, err := os.Open(path)
	if err != nil {
		return ids
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() > tailCheck {
		f.Seek(st.Size()-tailCheck, io.SeekStart)
	}
	b, err := io.ReadAll(f)
	if err != nil && !errors.Is(err, io.EOF) {
		return ids
	}
	for _, l := range bytes.Split(b, []byte("\n")) {
		if i := bytes.Index(l, []byte(`"id":"`)); i >= 0 && len(l) >= i+6+36 {
			ids[string(l[i+6:i+6+36])] = true
		}
	}
	return ids
}

// Pending is how many bytes of a run's spool are past its cursors (anfs agents show says them)
func (s Store) Pending(id string) int64 {
	sp, err := spool.Open(s.SpoolDir(id), s.RunDir(id), func(string, ...any) {})
	if err != nil {
		return 0
	}
	files, _ := sp.Files()
	var n int64
	for _, f := range files {
		if st, err := os.Lstat(f.Path); err == nil && st.Size() > sp.Cursor(f.Key) {
			n += st.Size() - sp.Cursor(f.Key)
		}
	}
	return n
}

// drainRecent: at start, the runs that ended in the last day with lines past their cursors (written
// between their end and a crash, or by a straggling child) go into the log too
func (s *Supervisor) drainRecent(recs []*Record) {
	for _, r := range recs {
		if !Ended(r.State) || r.Ended == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, r.Ended)
		if err != nil || time.Since(t) > recentEnded || s.Store.Pending(r.ID) == 0 {
			continue
		}
		s.drainFinal(r)
		s.save(r)
		s.Logf("run %s (%s): %d events from its spool after its end", r.ID, r.Rule, r.EventsIngested)
	}
}
