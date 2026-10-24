// Package shipper sends the spool to the bus: each complete line of each month file
// (<scope>/<YYYY-MM>.jsonl; never the recorder's <YYYY-MM>.bus.jsonl), as it is, on
// anfs.<scope>.<type without io.> (anfs-io/events SPEC.md §5: the scope its source names) with
// Nats-Msg-Id set to its CloudEvent id. Any layer's event is shipped (io.cnfs, io.lgat: any JSON
// data); a line in another scope's directory is said once per file and shipped by its source. At most 256 publishes await
// their acks (one batch); a token bucket per scope (ANFSD_SHIP_RATE a second, ANFSD_SHIP_BURST)
// holds a flood back, and the files hold the rest. A file's cursor moves only over the contiguous
// prefix of its lines NATS acknowledged, and the cursors are written after each batch; a line sent
// again after a crash between the ack and that write is dropped by the stream's duplicate window.
//
// A line that is not an event of the profile is not sent: it is logged once, with its file and
// offset, and the cursor steps past it.
//
// When NATS is down the shipper blocks until the connection is back (the bus's backoff); when a
// batch fails while connected it waits 1, 2, 5, 10, then 30 s. Nothing here spins.
package shipper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/spool"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"
)

// AckTimeout is how long a batch waits for its acks (and what SIGTERM waits for at most).
const AckTimeout = 5 * time.Second

// Bus is what the shipper needs of the connection.
type Bus interface {
	Connected() bool
	WaitConnected(ctx context.Context) error
	NeedsEnsure() bool
	EnsureStream(ctx context.Context, s bus.StreamSettings) error
	Publish(subject string, data []byte, id string) (jetstream.PubAckFuture, error)
}

// Shipper is one spool going to one bus.
type Shipper struct {
	Spool  *spool.Spool
	Bus    Bus
	Stream bus.StreamSettings
	Rate   float64
	Burst  int
	Window int // the batch: publishes awaiting acks (bus.MaxPending)
	Logf   func(string, ...any)
	// Backoff replaces bus.Backoff between failed batches (tests)
	Backoff func(attempt int) time.Duration

	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	state    string
	shipped  int64
	// misfiledSaid: the files a line of another scope was said of
	misfiledSaid map[string]bool
}

// Shipped is how many lines NATS has acknowledged since the shipper started.
func (s *Shipper) Shipped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shipped
}

func (s *Shipper) limiter(scope string) *rate.Limiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limiters == nil {
		s.limiters = map[string]*rate.Limiter{}
	}
	l := s.limiters[scope]
	if l == nil {
		r, b := s.Rate, s.Burst
		if r <= 0 {
			r = 100
		}
		if b <= 0 {
			b = 1000
		}
		l = rate.NewLimiter(rate.Limit(r), b)
		s.limiters[scope] = l
	}
	return l
}

// say logs a state the first time it is entered
func (s *Shipper) say(state string) {
	s.mu.Lock()
	changed := state != s.state
	s.state = state
	s.mu.Unlock()
	if changed && state != "" {
		s.Logf("%s", state)
	}
}

// Run ships until ctx is done: everything pending, then again at each wake-up (the watcher's
// channel). On the way out the batch in flight finishes (its acks, at most AckTimeout) and the
// cursors are written.
func (s *Shipper) Run(ctx context.Context, wake <-chan struct{}) error {
	attempt := 0
	for {
		_, err := s.Drain(ctx)
		if ctx.Err() != nil {
			return s.Spool.Save()
		}
		if err != nil {
			if !s.Bus.Connected() {
				s.say("shipping waits for NATS")
				if s.Bus.WaitConnected(ctx) != nil {
					return s.Spool.Save()
				}
				attempt = 0
				continue
			}
			attempt++
			s.say("shipping failed: " + err.Error() + " (retrying)")
			back := bus.Backoff
			if s.Backoff != nil {
				back = s.Backoff
			}
			t := time.NewTimer(back(attempt))
			select {
			case <-ctx.Done():
				t.Stop()
				return s.Spool.Save()
			case <-t.C:
			}
			continue
		}
		s.say("shipping")
		attempt = 0
		select {
		case <-ctx.Done():
			return s.Spool.Save()
		case <-wake:
		}
	}
}

// Drain ships batches until nothing is pending or one fails; it returns how many lines were
// acknowledged (invalid lines stepped past are not counted).
func (s *Shipper) Drain(ctx context.Context) (int, error) {
	total := 0
	for {
		if !s.Bus.Connected() {
			return total, errors.New("not connected")
		}
		if s.Bus.NeedsEnsure() {
			ectx, cancel := context.WithTimeout(context.Background(), AckTimeout)
			err := s.Bus.EnsureStream(ectx, s.Stream)
			cancel()
			if err != nil {
				return total, err
			}
		}
		lines, err := s.collect()
		if err != nil {
			return total, err
		}
		if len(lines) == 0 {
			return total, nil
		}
		n, err := s.batch(ctx, lines)
		total += n
		if serr := s.Spool.Save(); serr != nil {
			s.Logf("writing the cursors: %v", serr)
		}
		if err != nil {
			return total, err
		}
		if ctx.Err() != nil {
			return total, nil
		}
	}
}

// collect takes up to Window lines: every file in order, each file from its cursor
func (s *Shipper) collect() ([]spool.Line, error) {
	window := s.Window
	if window <= 0 {
		window = bus.MaxPending
	}
	files, err := s.Spool.Files()
	if err != nil {
		return nil, err
	}
	var out []spool.Line
	for _, f := range files {
		ls, err := s.Spool.Read(f, window-len(out))
		if err != nil {
			s.Logf("reading %s: %v", f.Key, err)
			continue
		}
		out = append(out, ls...)
		if len(out) >= window {
			break
		}
	}
	return out, nil
}

// misfiled says once per file that its lines are another scope's (their source): shipped by
// their source all the same, the subject being the event's
func (s *Shipper) misfiled(key, scope string) {
	s.mu.Lock()
	if s.misfiledSaid == nil {
		s.misfiledSaid = map[string]bool{}
	}
	said := s.misfiledSaid[key]
	s.misfiledSaid[key] = true
	s.mu.Unlock()
	if !said {
		s.Logf("%s holds an event of scope %s (its source): shipped by its source; the file is not its scope's", key, scope)
	}
}

type sent struct {
	line spool.Line
	fut  jetstream.PubAckFuture // nil: not sent (invalid: done; or not reached: not done)
	done bool
}

// batch publishes the lines (each through its scope's token bucket), waits for the acks, and
// moves each file's cursor over the prefix that is done
func (s *Shipper) batch(ctx context.Context, lines []spool.Line) (int, error) {
	items := make([]sent, len(lines))
	var firstErr error
	stopped := false
	for i, l := range lines {
		items[i].line = l
		if stopped {
			continue
		}
		ev, err := cloudevent.Parse(l.Data)
		if err != nil {
			s.Logf("not shipped, not an event: %s at byte %d: %v", l.File.Key, l.Start, err)
			items[i].done = true
			continue
		}
		if ev.Scope != l.File.Scope {
			s.misfiled(l.File.Key, ev.Scope)
		}
		if err := s.limiter(l.File.Scope).Wait(ctx); err != nil {
			stopped = true // shutting down: what was sent is waited for, the rest stays
			continue
		}
		fut, err := s.Bus.Publish(ev.NatsSubject(), l.Data, ev.ID)
		if err != nil {
			firstErr = fmt.Errorf("publish: %w", err)
			stopped = true
			continue
		}
		items[i].fut = fut
	}

	deadline := time.NewTimer(AckTimeout)
	defer deadline.Stop()
	timedOut := false
	acked := 0
	for i := range items {
		f := items[i].fut
		if f == nil || timedOut {
			continue
		}
		select {
		case <-f.Ok():
			items[i].done = true
			acked++
		case err := <-f.Err():
			if firstErr == nil {
				firstErr = fmt.Errorf("ack: %w", err)
			}
		case <-deadline.C:
			timedOut = true
			if firstErr == nil {
				firstErr = errors.New("no ack within 5 s")
			}
		}
	}

	// each file's cursor over its contiguous done prefix
	blocked := map[string]bool{}
	for _, it := range items {
		k := it.line.File.Key
		if blocked[k] {
			continue
		}
		if !it.done {
			blocked[k] = true
			continue
		}
		s.Spool.Advance(k, it.line.End)
	}
	s.mu.Lock()
	s.shipped += int64(acked)
	s.mu.Unlock()
	return acked, firstErr
}
