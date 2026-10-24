package spool

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Reconcile is how often the watcher looks without being told: new scope directories that were
// missed, and anything fsnotify did not report. The only timer.
const Reconcile = 30 * time.Second

// Watcher sends on C (buffered, one slot: a burst is one wake-up) when the events directory may
// have new lines.
type Watcher struct {
	C       chan struct{}
	dir     string
	every   time.Duration
	logf    func(string, ...any)
	w       *fsnotify.Watcher
	watched map[string]bool
}

// Watch starts watching dir (which may not exist yet: it is looked for at each reconcile). It
// stops when ctx is done.
func Watch(ctx context.Context, dir string, every time.Duration, logf func(string, ...any)) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{C: make(chan struct{}, 1), dir: dir, every: every, logf: logf, w: fw, watched: map[string]bool{}}
	w.add()
	// a look at once: what was written before the watches were in place raises no event (a run's
	// harness may write its first line before its spool is watched)
	w.Kick()
	go w.loop(ctx)
	return w, nil
}

// Kick asks for a look now.
func (w *Watcher) Kick() {
	select {
	case w.C <- struct{}{}:
	default:
	}
}

// add watches the events directory and each scope directory not watched yet
func (w *Watcher) add() {
	try := func(p string) {
		if w.watched[p] {
			return
		}
		if err := w.w.Add(p); err == nil {
			w.watched[p] = true
		}
	}
	if _, err := os.Stat(w.dir); err != nil {
		return
	}
	try(w.dir)
	ents, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if e.IsDir() {
			try(filepath.Join(w.dir, e.Name()))
		}
	}
}

func (w *Watcher) loop(ctx context.Context) {
	defer w.w.Close()
	t := time.NewTicker(w.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.w.Events:
			if !ok {
				return
			}
			if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
				delete(w.watched, ev.Name)
			}
			if ev.Has(fsnotify.Create) && filepath.Dir(ev.Name) == w.dir {
				w.add()
			}
			w.Kick()
		case err, ok := <-w.w.Errors:
			if !ok {
				return
			}
			w.logf("watching %s: %v (the 30 s reconcile still looks)", w.dir, err)
		case <-t.C:
			w.add()
			w.Kick()
		}
	}
}
