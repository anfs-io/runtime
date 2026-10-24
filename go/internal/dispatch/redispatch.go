package dispatch

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/space"
)

// Request is one anfs agents redispatch request: <state>/anfsd/redispatch/<uuid>.json.
type Request struct {
	Event string `json:"event"` // the event's line, as in the log
	Rule  string `json:"rule"`  // one rule's name, or "" for every rule that matches
	By    string `json:"by"`    // who asked (their actor)
	Again bool   `json:"again"` // also when the rule already handled the event (a failed run's second try)
}

// Redispatch handles every request in RedispatchDir, oldest first, and removes it: for each rule
// that matches (or the one named), a run is submitted that skips stale, rate and loop (the person
// asked for it); it may still be held for its role's queue. Refused, and logged: a rule that is
// disabled or invalid, one that already has a run not ended, and one that handled the event
// unless again is set. Returns what it did, one line per rule.
func (d *Dispatcher) Redispatch() []string {
	if d.RedispatchDir == "" {
		return nil
	}
	ents, err := os.ReadDir(d.RedispatchDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			d.logf("redispatch: %v", err)
		}
		return nil
	}
	var names []string
	for _, e := range ents {
		if e.Type().IsRegular() && strings.HasSuffix(e.Name(), ".json") && !strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	var out []string
	for _, n := range names {
		p := filepath.Join(d.RedispatchDir, n)
		for _, l := range d.redispatchFile(p) {
			d.logf("redispatch %s: %s", strings.TrimSuffix(n, ".json"), l)
			out = append(out, l)
		}
		if err := os.Remove(p); err != nil {
			d.logf("redispatch: removing %s: %v", n, err)
		}
	}
	return out
}

func (d *Dispatcher) redispatchFile(p string) []string {
	b, err := os.ReadFile(p)
	if err != nil {
		return []string{"not read: " + err.Error()}
	}
	var q Request
	if err := json.Unmarshal(b, &q); err != nil {
		return []string{"not a request: " + err.Error()}
	}
	line := []byte(strings.TrimSpace(q.Event))
	ev, err := cloudevent.Parse(line)
	if err != nil {
		return []string{"not an event: " + err.Error()}
	}
	if q.By == "" {
		q.By = "unknown"
	}
	d.mu.Lock()
	spaces := d.spaces
	d.mu.Unlock()
	var out []string
	named := false
	for _, s := range spaces {
		for _, r := range s.Rules {
			if q.Rule != "" && r.Name != q.Rule {
				continue
			}
			named = true
			if r.Status == space.StatusInvalid {
				if q.Rule != "" {
					out = append(out, fmt.Sprintf("%s/%s: refused: invalid (%s)", s.Path, r.Name, r.Reason))
				}
				continue
			}
			if !r.Matches(ev, d.SpacePath(ev.Scope)) {
				if q.Rule != "" {
					out = append(out, fmt.Sprintf("%s/%s: does not match: %s", s.Path, r.Name, whyNot(r, ev, d.SpacePath(ev.Scope))))
				}
				continue
			}
			out = append(out, fmt.Sprintf("%s/%s: %s", s.Path, r.Name, d.redispatchRule(r, ev, line, q)))
		}
	}
	if q.Rule != "" && !named {
		out = append(out, "no rule "+q.Rule)
	}
	if len(out) == 0 {
		out = append(out, "no rule matches "+ev.Short()+" "+ev.Subject)
	}
	return out
}

func (d *Dispatcher) redispatchRule(r *space.Rule, ev cloudevent.Event, line []byte, q Request) string {
	sid := r.Space.ID
	switch {
	case r.Status == space.StatusDisabled:
		return "refused: the rule is disabled (enabled: false)"
	case !r.Loaded():
		return "refused: the rule is " + r.Status
	case d.Sup.Unended(sid, r.Name, ev.ID):
		return "refused: it has a run for this event that has not ended"
	case d.Sup.Handled(sid, r.Name, ev.ID) && !q.Again:
		return "refused: it already handled this event (--again for another run)"
	}
	var dec Decision
	d.record(&dec, r, ev)
	rec := dec.Record
	rec.RedispatchBy = q.By
	if r.Job != nil && !validJob(rec.Job) {
		return fmt.Sprintf("refused: job.name renders to %q, not a job name", rec.Job)
	}
	detail := ""
	held, _ := d.Sup.Held(sid, r.Name)
	if d.Sup.Full(sid, r.As, r.Role.Concurrency, r.Role.Queue) {
		if held >= r.Hold {
			return fmt.Sprintf("refused: hold: %d full", r.Hold)
		}
		rec.Held = "queue"
		detail = fmt.Sprintf("%s's queue is full (queue: %d)", r.Address(), r.Role.Queue)
	}
	if err := d.Sup.Submit(rec, line, dec.Prompt, detail); err != nil {
		return "not recorded: " + err.Error()
	}
	if rec.Held != "" {
		return "submitted run " + rec.ID + ", held for the queue"
	}
	return "submitted run " + rec.ID
}
