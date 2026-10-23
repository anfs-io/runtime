// Package dispatch is anfsd's dispatcher: the rules of every installed space (internal/space),
// the durable consumer anfsd-dispatch on the stream ANFS (its filters each rule's
// anfs.<space id>.<type prefix>; the globs matched here), and for each event a rule matches, the
// guards (stale, rate, queue, loop: each a run.rejected) and then a run submitted to the
// supervisor. One run per rule and event id: a redelivery finds the record and is acked. The
// message is acked only once the record is written.
//
// The rules are read at start, on SIGHUP (anfs bus reload) and on a source.updated event.
package dispatch

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/space"
	"github.com/anfs-io/runtime/go/internal/supervise"
	"github.com/nats-io/nats.go/jetstream"
)

// Consumer is the durable consumer's name.
const Consumer = "anfsd-dispatch"

// SourceUpdated is always in the filter: a source that moved may bring new rules.
const SourceUpdated = "anfs.host.source.updated"

// Submitter is what the dispatcher needs of the supervisor.
type Submitter interface {
	Has(spaceID, rule, eventID string) bool
	Starts(spaceID, rule string, since time.Time) int
	IsRunOf(spaceID, rule, actor string) bool
	Full(spaceID, role string, concurrency, queue int) bool
	SetConcurrency(spaceID, role string, n int)
	Submit(r *supervise.Record, eventLine []byte, prompt string) error
	Reject(r *supervise.Record, eventLine []byte, reason, detail string) error
}

// Dispatcher matches events to rules. Fill the fields, Reload, then Handle each event (Run does
// it from the consumer).
type Dispatcher struct {
	Sup  Submitter
	Load func(ctx context.Context) ([]*space.Space, error)
	Logf func(string, ...any)
	Now  func() time.Time

	mu     sync.Mutex
	spaces []*space.Space
	rules  []*space.Rule // loaded
	paths  map[string]string
	reload chan struct{}
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Dispatcher) logf(f string, a ...any) {
	if d.Logf != nil {
		d.Logf(f, a...)
	}
}

// Reload reads the rules again; one log line per invalid rule and a count.
func (d *Dispatcher) Reload(ctx context.Context) error {
	spaces, err := d.Load(ctx)
	if err != nil {
		d.logf("rules not read: %v (the last ones stay)", err)
		return err
	}
	var loaded []*space.Rule
	paths := map[string]string{}
	invalid, disabled := 0, 0
	for _, s := range spaces {
		if s.ID != "" {
			paths[s.ID] = s.Path
		}
		for _, r := range s.Rules {
			switch {
			case r.Loaded():
				loaded = append(loaded, r)
				for _, w := range r.Warnings {
					d.logf("rule %s/%s: %s", s.Path, r.Name, w)
				}
			case r.Status == space.StatusDisabled:
				disabled++
			default:
				invalid++
				d.logf("rule %s/%s invalid, not loaded: %s", s.Path, r.Name, r.Reason)
			}
		}
		for name, role := range s.Roles {
			if s.ID != "" {
				d.Sup.SetConcurrency(s.ID, name, role.Concurrency)
			}
		}
	}
	d.mu.Lock()
	d.spaces, d.rules, d.paths = spaces, loaded, paths
	d.mu.Unlock()
	d.logf("rules: %d loaded, %d disabled, %d invalid, from %d spaces", len(loaded), disabled, invalid, len(spaces))
	return nil
}

// Rules are the loaded rules.
func (d *Dispatcher) Rules() []*space.Rule {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*space.Rule(nil), d.rules...)
}

// Filters are the consumer's filter subjects: each rule's, and source.updated; one covered by
// another is left out, and filters that overlap otherwise become anfs.> (NATS refuses them).
func (d *Dispatcher) Filters() []string { return Filters(d.Rules()) }

// Filters (see Dispatcher.Filters).
func Filters(rules []*space.Rule) []string {
	set := map[string]bool{SourceUpdated: true}
	for _, r := range rules {
		set[r.FilterSubject()] = true
	}
	var all []string
	for s := range set {
		all = append(all, s)
	}
	sort.Strings(all)
	var kept []string
	for i, a := range all {
		covered := false
		for j, b := range all {
			if i != j && covers(b, a) && (!covers(a, b) || j < i) {
				covered = true
				break
			}
		}
		if !covered {
			kept = append(kept, a)
		}
	}
	for i := range kept {
		for j := i + 1; j < len(kept); j++ {
			if collide(kept[i], kept[j]) {
				return []string{"anfs.>"}
			}
		}
	}
	return kept
}

// covers: every subject b matches, a matches too
func covers(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i, t := range at {
		if t == ">" {
			return i < len(bt)
		}
		if i >= len(bt) {
			return false
		}
		switch {
		case bt[i] == ">":
			return false
		case t == "*":
		case t != bt[i] || bt[i] == "*":
			return false
		}
	}
	return len(at) == len(bt)
}

// collide: some subject matches both
func collide(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(at) && i < len(bt); i++ {
		if at[i] == ">" || bt[i] == ">" {
			return true
		}
		if at[i] != "*" && bt[i] != "*" && at[i] != bt[i] {
			return false
		}
	}
	return len(at) == len(bt)
}

// Decision is what one rule does with one event (Handle, and anfsd match without starting).
type Decision struct {
	Rule    *space.Rule
	Match   bool
	Action  string // submitted, rejected, duplicate, disabled (match only)
	Reason  string // a rejection's
	Detail  string
	Record  *supervise.Record
	Prompt  string // the rule's prompt, rendered (anfsd adds the footer when it starts)
	Matched string // why it did not match (match only)
}

// SpacePath is the path of a space id, from the spaces loaded.
func (d *Dispatcher) SpacePath(id string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == "host" {
		return "host"
	}
	return d.paths[id]
}

// Handle acts on one event's line: for each loaded rule that matches, a run is submitted or
// rejected (or it is a redelivery). A line that is not an event is an error (the caller acks it
// and logs it: it will never be one).
func (d *Dispatcher) Handle(line []byte) ([]Decision, error) {
	ev, err := cloudevent.Parse(line)
	if err != nil {
		return nil, err
	}
	if ev.NatsSubject() == SourceUpdated {
		d.askReload()
	}
	var out []Decision
	for _, r := range d.Rules() {
		dec := d.Decide(r, ev, line, false)
		if !dec.Match || dec.Action == "duplicate" {
			continue
		}
		var err error
		if dec.Action == "rejected" {
			err = d.Sup.Reject(dec.Record, line, dec.Reason, dec.Detail)
		} else {
			err = d.Sup.Submit(dec.Record, line, dec.Prompt)
		}
		if err != nil {
			return out, fmt.Errorf("rule %s: recording the run: %w", r.Key(), err)
		}
		d.logf("event %s %s: rule %s/%s %s run %s%s", ev.Short(), ev.Subject, r.Space.Path, r.Name, dec.Action, dec.Record.ID,
			map[bool]string{true: " (" + dec.Reason + ")", false: ""}[dec.Reason != ""])
		out = append(out, dec)
	}
	return out, nil
}

// Decide is what a rule would do with an event, without recording anything. dry also decides for
// a rule that is not loaded (disabled), for anfsd match.
func (d *Dispatcher) Decide(r *space.Rule, ev cloudevent.Event, line []byte, dry bool) Decision {
	dec := Decision{Rule: r}
	if !r.Matches(ev, d.SpacePath(ev.Scope)) {
		dec.Matched = whyNot(r, ev, d.SpacePath(ev.Scope))
		return dec
	}
	dec.Match = true
	sid := r.Space.ID
	if d.Sup.Has(sid, r.Name, ev.ID) {
		dec.Action = "duplicate"
		return dec
	}
	id := supervise.NewID()
	vars := space.Vars{Event: ev, Space: r.Space.Path, Run: id}
	rec := &supervise.Record{
		ID: id, Rule: r.Name, Space: r.Space.Path, SpaceID: sid, SpaceDir: r.Space.Dir,
		Event: ev.ID, EventType: ev.Short(), Subject: ev.Subject, Actor: ev.Actor,
		Role: r.As, Address: r.Address(), Harness: r.Harness, Model: r.Model, BudgetUSD: r.BudgetUSD,
		Args: r.Args, Concurrency: r.Role.Concurrency, Timeout: r.Timeout.String(), Retries: r.Retries,
	}
	for _, a := range r.Space.Access {
		if a.Mode == "write" {
			rec.Write = append(rec.Write, a.Path)
		} else {
			rec.Read = append(rec.Read, a.Path)
		}
	}
	if r.Job != nil {
		rec.Job = space.JobName(r.Job.Name, vars)
		rec.Repos = r.Job.Repos
		vars.Job = rec.Job
	}
	dec.Record = rec
	dec.Prompt = space.Render(r.Prompt, vars)
	reject := func(reason, detail string) Decision {
		dec.Action, dec.Reason, dec.Detail = "rejected", reason, detail
		return dec
	}
	if r.Job != nil && !validJob(rec.Job) {
		return reject("invalid", fmt.Sprintf("job.name renders to %q, not a job name", rec.Job))
	}
	if t, err := time.Parse(time.RFC3339, ev.Time); err == nil && d.now().Sub(t) > r.Stale {
		return reject("stale", fmt.Sprintf("the event is %s old (stale: %s)", d.now().Sub(t).Round(time.Second), r.Stale))
	}
	if n := d.Sup.Starts(sid, r.Name, d.now().Add(-r.Rate.Window)); n >= r.Rate.N {
		return reject("rate", fmt.Sprintf("%d runs in the last %s (rate: %s)", n, r.Rate.Window, r.Rate))
	}
	if d.Sup.IsRunOf(sid, r.Name, ev.Actor) {
		return reject("loop", fmt.Sprintf("the event is from %s, a run of this rule", ev.Actor))
	}
	if d.Sup.Full(sid, r.As, r.Role.Concurrency, r.Role.Queue) {
		return reject("queue", fmt.Sprintf("%s's queue is full (queue: %d)", r.Address(), r.Role.Queue))
	}
	dec.Action = "submitted"
	if dry && r.Status == space.StatusDisabled {
		dec.Action = "disabled"
	}
	return dec
}

func whyNot(r *space.Rule, ev cloudevent.Event, spacePath string) string {
	if !space.SpecMatches(r.On.Space, r.Space, ev.Scope, spacePath) {
		want := r.On.Space
		if want == "" {
			want = r.Space.Path + " (its own)"
		}
		return "space: the event is " + map[bool]string{true: spacePath, false: ev.Scope}[spacePath != ""] + ", the rule takes " + want
	}
	probe := *r
	probe.On.Subject, probe.On.Data = "", nil
	if !probe.Matches(ev, spacePath) {
		return "type: " + ev.Short() + " is not " + r.On.Type
	}
	probe.On.Subject = r.On.Subject
	if !probe.Matches(ev, spacePath) {
		return "subject: " + ev.Subject + " is not " + r.On.Subject
	}
	keys := make([]string, 0, len(r.On.Data))
	for k := range r.On.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, ok := ev.Data[k]
		if !ok {
			return "data." + k + ": the event has none"
		}
		probe.On.Data = map[string]string{k: r.On.Data[k]}
		if !probe.Matches(ev, spacePath) {
			return "data." + k + ": " + v + " is not " + r.On.Data[k]
		}
	}
	return ""
}

// a job name anfs work takes: a branch name with no /, not main
func validJob(n string) bool {
	if n == "" || n == "main" || n == "master" || strings.HasPrefix(n, "-") || strings.HasSuffix(n, "-") && strings.Trim(n, "-") == "" {
		return false
	}
	return !strings.ContainsAny(n, "/ ~^:?*[\\") && !strings.Contains(n, "..") && len(n) <= 100
}

func (d *Dispatcher) askReload() {
	d.mu.Lock()
	ch := d.reload
	d.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// AskReload is SIGHUP's: the rules are read again by Run's goroutine.
func (d *Dispatcher) AskReload() { d.askReload() }

// Consuming is what Run needs of the bus.
type Consuming interface {
	Consume(ctx context.Context, cfg jetstream.ConsumerConfig, h func(jetstream.Msg)) error
	UpdateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) error
}

// Config is the consumer's configuration for the filters.
func Config(filters []string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:        Consumer,
		Description:    "anfsd's dispatcher: the events dispatch rules act on",
		FilterSubjects: filters,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        time.Minute,
		MaxAckPending:  64,
	}
}

// Run consumes until ctx is done; the rules are read again on AskReload and on source.updated
// (then the consumer's filters are updated). Reload before Run.
func (d *Dispatcher) Run(ctx context.Context, b Consuming) error {
	d.mu.Lock()
	d.reload = make(chan struct{}, 1)
	ch := d.reload
	d.mu.Unlock()
	filters := d.Filters()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			}
			if d.Reload(ctx) != nil {
				continue
			}
			nf := d.Filters()
			if strings.Join(nf, " ") == strings.Join(filters, " ") {
				continue
			}
			if err := b.UpdateConsumer(ctx, Config(nf)); err != nil {
				d.logf("consumer %s: filters not updated: %v", Consumer, err)
				continue
			}
			filters = nf
			d.logf("consumer %s: filters %s", Consumer, strings.Join(nf, " "))
		}
	}()
	d.logf("consumer %s: filters %s", Consumer, strings.Join(filters, " "))
	return b.Consume(ctx, Config(filters), func(m jetstream.Msg) {
		if _, err := d.Handle(m.Data()); err != nil {
			if strings.HasPrefix(err.Error(), "rule ") {
				// the record could not be written: let NATS deliver it again
				d.logf("%s: %v (redelivered)", m.Subject(), err)
				m.NakWithDelay(30 * time.Second)
				return
			}
			d.logf("%s: not an event: %v", m.Subject(), err)
		}
		m.Ack()
	})
}
