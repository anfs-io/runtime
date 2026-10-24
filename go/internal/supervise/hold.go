package supervise

import (
	"fmt"
	"sort"
	"time"
)

// Held runs (agents/02d, F5). A full queue or a spent rate holds a run instead of rejecting it
// (a rule's overflow: delay, the default; hold: caps how many): the run is submitted with held
// rate|queue, and leaves hold, oldest first per rule, when its rule's rate has a start free and its
// role's queue has room. A rule's held runs are released on every schedule() (a run starting or
// ending, a submission) and, while the rate is what holds them, by one timer per rule set to the
// oldest counted start plus the window: no polling. A held run is a record (submitted + held), so
// a restart puts it back in hold in id order.

// releaseHeld moves held runs into their queue (s.mu held)
func (s *Supervisor) releaseHeld() {
	keys := make([]string, 0, len(s.held))
	for k := range s.held {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	now := time.Now()
	for _, k := range keys {
		list := s.held[k]
		// a held run canceled (anfs agents stop) leaves at once, wherever it is
		kept := list[:0]
		for _, r := range list {
			if by, ok := s.Store.Canceled(r.ID); ok {
				s.endLater(r, Canceled, "", "", by)
				continue
			}
			kept = append(kept, r)
		}
		list = kept
		// oldest first; once the rate holds one, the rule's later runs wait behind it, but a
		// redispatched run (the rate skipped) may still go by when the queue has room
		var wait time.Time
		rateHeld := false
		kept = list[:0:0]
		for i, r := range list {
			if s.full(r.roleKey(), s.concurrency(r), r.Queue) {
				kept = append(kept, list[i:]...)
				break
			}
			if r.RedispatchBy == "" {
				if !rateHeld {
					if next, spent := s.rateSpent(k, r, now); spent {
						wait, rateHeld = next, true
					}
				}
				if rateHeld {
					kept = append(kept, r)
					continue
				}
			}
			r.Held, r.Queued = "", now.UTC().Format("2006-01-02T15:04:05.000Z")
			s.starts[k] = append(s.starts[k], now)
			s.queue = append(s.queue, r)
			s.save(r)
			s.Logf("run %s (%s): no longer held, queued", r.ID, r.Rule)
		}
		list = kept
		if len(list) == 0 {
			delete(s.held, k)
		} else {
			s.held[k] = list
		}
		s.armRate(k, wait)
	}
	for k := range s.timers {
		if len(s.held[k]) == 0 {
			s.armRate(k, time.Time{})
		}
	}
}

// rateSpent: the rule's starts in its window are at its rate; next is when the oldest counted
// one leaves the window
func (s *Supervisor) rateSpent(k string, r *Record, now time.Time) (time.Time, bool) {
	w := r.RateWin()
	if r.RateN <= 0 || w <= 0 {
		return time.Time{}, false
	}
	var in []time.Time
	for _, t := range s.starts[k] {
		if !t.Before(now.Add(-w)) {
			in = append(in, t)
		}
	}
	if len(in) < r.RateN {
		return time.Time{}, false
	}
	sort.Slice(in, func(i, j int) bool { return in[i].Before(in[j]) })
	return in[len(in)-r.RateN].Add(w), true
}

// armRate sets the rule's rate timer to at (the zero time: none); it wakes the scheduler
func (s *Supervisor) armRate(k string, at time.Time) {
	if t := s.timers[k]; t != nil {
		t.Stop()
		delete(s.timers, k)
	}
	if at.IsZero() {
		return
	}
	d := time.Until(at) + 10*time.Millisecond
	if d < 10*time.Millisecond {
		d = 10 * time.Millisecond
	}
	s.timers[k] = time.AfterFunc(d, s.signal)
}

// tooOld: a run whose trigger is older than its rule's stale, unless it was redispatched
func (s *Supervisor) tooOld(r *Record) (string, bool) {
	st := r.StaleDur()
	if st <= 0 || r.RedispatchBy != "" || r.EventTime == "" {
		return "", false
	}
	t, err := time.Parse(time.RFC3339, r.EventTime)
	if err != nil {
		return "", false
	}
	if age := time.Since(t); age > st {
		return fmt.Sprintf("the event is %s old when the run would start (stale: %s)", age.Round(time.Second), r.Stale), true
	}
	return "", false
}

// endLater ends a run that never started, outside the lock (s.mu held by the caller)
func (s *Supervisor) endLater(r *Record, state, reason, detail, by string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.end(r, state, reason, detail, nil, by)
	}()
}
