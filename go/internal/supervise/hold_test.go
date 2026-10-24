package supervise

import (
	"strings"
	"testing"
	"time"
)

func heldRec(rule, held string) *Record {
	r := rec(rule, "m", "claude", "")
	r.Held, r.Queue = held, 0
	return r
}

func (r *rig) heldCount() int {
	_, _, h := r.sup.Counts()
	return h
}

// queue: held runs enter the queue as slots free, oldest first; every run completes
func TestHeldForTheQueue(t *testing.T) {
	r := newRig(t, 4)
	r.f.Mode("sleep 0.3")
	r.start()
	a := rec("r", "m", "claude", "")
	a.SpaceDir = r.f.Home
	r.submit(a)
	var held []*Record
	for i := 0; i < 3; i++ {
		h := heldRec("r", "queue")
		h.SpaceDir = r.f.Home
		r.submit(h)
		held = append(held, h)
	}
	r.waitState(a.ID, Completed, 10*time.Second)
	var prev time.Time
	for _, h := range held {
		d := r.waitState(h.ID, Completed, 10*time.Second)
		st, _ := time.Parse(time.RFC3339, d.Started)
		if st.Before(prev) || d.Held != "" || d.Queued == "" || d.HeldSince == "" {
			t.Errorf("out of order or still held: %+v", d)
		}
		prev = st
	}
	if n := r.heldCount(); n != 0 {
		t.Errorf("%d still held", n)
	}
	got := strings.Join(r.events_("held"), "\n")
	if strings.Count(got, "run.submitted held=queue") != 3 || strings.Count(got, "run.completed") != 4 {
		t.Errorf("events:\n%s", got)
	}
}

// rate: a run held for its rule's rate enters the queue when the window frees a start, by a timer
// that exists only while the rule holds runs; rate counts runs entering the queue
func TestHeldForTheRate(t *testing.T) {
	r := newRig(t, 4)
	r.start()
	a := rec("r", "m", "claude", "")
	a.SpaceDir, a.RateN, a.RateWindow, a.Queue = r.f.Home, 1, "1s", 10
	r.submit(a)
	b := heldRec("r", "rate")
	b.SpaceDir, b.RateN, b.RateWindow, b.Queue = r.f.Home, 1, "1s", 10
	r.submit(b)
	if n := r.sup.Starts(sid, "r", time.Now().Add(-time.Minute)); n != 1 {
		t.Errorf("a held run counted against the rate: %d", n)
	}
	timers := 0
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline) && timers == 0; time.Sleep(10 * time.Millisecond) {
		r.sup.mu.Lock()
		timers = len(r.sup.timers)
		r.sup.mu.Unlock()
	}
	if timers != 1 {
		t.Errorf("%d timers while holding", timers)
	}
	da := r.waitState(a.ID, Completed, 5*time.Second)
	db := r.waitState(b.ID, Completed, 5*time.Second)
	qa, _ := time.Parse(time.RFC3339, da.Queued)
	qb, _ := time.Parse(time.RFC3339, db.Queued)
	if qb.Sub(qa) < 900*time.Millisecond {
		t.Errorf("released after %s, the window is 1s", qb.Sub(qa))
	}
	r.sup.mu.Lock()
	timers = len(r.sup.timers)
	r.sup.mu.Unlock()
	if timers != 0 {
		t.Errorf("%d timers with nothing held", timers)
	}
}

// stale is checked when a run would start: one that waited past it is rejected (stale),
// terminal; a redispatched one is exempt
func TestStaleAtStart(t *testing.T) {
	r := newRig(t, 4)
	r.start()
	old := time.Now().Add(-2 * time.Hour).UTC().Format("2006-01-02T15:04:05.000Z")
	a := heldRec("r", "queue")
	a.SpaceDir, a.Stale, a.EventTime, a.Queue = r.f.Home, "1h0m0s", old, 10
	r.submit(a)
	d := r.waitState(a.ID, Rejected, 5*time.Second)
	if d.Reason != "stale" || !strings.Contains(d.Detail, "when the run would start (stale: 1h0m0s)") || d.Started != "" {
		t.Errorf("%+v", d)
	}
	b := rec("r2", "m", "claude", "")
	b.SpaceDir, b.Stale, b.EventTime, b.RedispatchBy = r.f.Home, "1h0m0s", old, "shell:person"
	r.submit(b)
	r.waitState(b.ID, Completed, 5*time.Second)
	if got := strings.Join(r.events_("reason", "redispatch"), " "); !strings.Contains(got, "run.rejected reason=stale") || !strings.Contains(got, "run.submitted redispatch=shell:person") {
		t.Errorf("%s", got)
	}
}

// held runs are records: a new supervisor puts them back in hold, in id order, and runs them
func TestHeldSurviveARestart(t *testing.T) {
	r := newRig(t, 1)
	if err := r.sup.Open(); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		h := heldRec("r", "queue")
		h.SpaceDir = r.f.Home
		if err := r.sup.Submit(h, []byte(line), "x", "full"); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, h.ID)
	}
	r.sup = r.newSup(r.sup.Store.Dir, 1)
	if err := r.sup.Open(); err != nil {
		t.Fatal(err)
	}
	if n := r.heldCount(); n != 3 {
		t.Fatalf("%d held after a restart", n)
	}
	r.sup = r.newSup(r.sup.Store.Dir, 1)
	r.start()
	var prev string
	for _, id := range ids {
		d := r.waitState(id, Completed, 10*time.Second)
		if d.Started < prev {
			t.Errorf("order: %s before %s", d.Started, prev)
		}
		prev = d.Started
	}
}

// a held run canceled (anfs agents stop) ends canceled without starting
func TestHeldCanceled(t *testing.T) {
	r := newRig(t, 1)
	r.f.Mode("sleep 1")
	r.start()
	a := rec("r", "m", "claude", "")
	a.SpaceDir = r.f.Home
	r.submit(a)
	h := heldRec("r", "queue")
	h.SpaceDir = r.f.Home
	r.submit(h)
	r.f.Touch("x", "")
	writeCancel(t, r, h.ID)
	r.sup.signal()
	d := r.waitState(h.ID, Canceled, 5*time.Second)
	if d.Started != "" || d.By != "shell:person" {
		t.Errorf("%+v", d)
	}
	r.waitState(a.ID, Completed, 5*time.Second)
}

// dispatcher: none at a reload: the space's queued and held runs end canceled
// (by=dispatcher:none), a working one goes on, another space's are untouched
func TestCancelUnstarted(t *testing.T) {
	r := newRig(t, 1)
	r.f.Mode("sleep 1")
	r.start()
	w := rec("r", "m", "claude", "")
	w.SpaceDir = r.f.Home
	r.submit(w)
	r.waitState(w.ID, Working, 5*time.Second)
	q := rec("r", "m", "claude", "")
	q.SpaceDir = r.f.Home
	r.submit(q)
	h := heldRec("r", "rate")
	h.SpaceDir = r.f.Home
	r.submit(h)
	o := rec("r", "m", "claude", "")
	o.SpaceID, o.SpaceDir = "zzzzz23456", r.f.Home
	r.submit(o)
	if n := r.sup.CancelUnstarted(sid, "dispatcher:none"); n != 2 {
		t.Errorf("%d marked", n)
	}
	for _, id := range []string{q.ID, h.ID} {
		if d := r.waitState(id, Canceled, 5*time.Second); d.By != "dispatcher:none" || d.Started != "" {
			t.Errorf("%+v", d)
		}
	}
	r.waitState(w.ID, Completed, 5*time.Second)
	r.waitState(o.ID, Completed, 10*time.Second)
}
