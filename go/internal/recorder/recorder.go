// Package recorder keeps "the files are the complete record" true for both ways in
// (anfs-io/events SPEC.md §7.3): anfsd's durable consumer anfsd-recorder reads every message of
// the stream ANFS (anfs.>, everything from the start when it is first made, explicit acks) and
// appends to <scope>/<YYYY-MM>.bus.jsonl each event that is not already in its month's files. So
// what a layer publishes to NATS directly (cnfs's gem, lgat's indexer, as their NATS user) is in
// the log beside what the spool shipped, and the spool's own lines, which the shipper published,
// are found and skipped: no header to trust or forge, only the id in the files.
//
// For each message: the event is checked against the profile and its subject against the event
// (anfs.<scope>.<type without io.>); invalid or mismatched, it is logged and terminated (it stays
// in the stream, never in the record). Its id in <scope>/<YYYY-MM>.jsonl or <YYYY-MM>.bus.jsonl
// (scope and month from the event): acked, nothing written; this is also how a redelivery after
// a crash between the append and the ack writes nothing. Otherwise its canonical form (the same
// members and values, whitespace out, data last) goes into <YYYY-MM>.bus.jsonl in one O_APPEND
// write, then the ack. The shipper never reads .bus.jsonl, so nothing recorded is published again.
//
// The ids of a month's two files are read once, when the month is first asked about, and then
// followed by offset: a lookup that misses reads what the files gained since.
package recorder

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/nats-io/nats.go/jetstream"
)

// Consumer is the durable consumer's name.
const Consumer = "anfsd-recorder"

// Filter is what it consumes: every layer, every scope.
const Filter = "anfs.>"

// Config is the consumer's.
func Config() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       Consumer,
		Description:   "anfsd's recorder: what is on the bus and not in the spool goes into <scope>/<YYYY-MM>.bus.jsonl",
		FilterSubject: Filter,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		MaxAckPending: 256,
	}
}

// Recorder appends what came on the bus to the events directory.
type Recorder struct {
	Events string // $XDG_STATE_HOME/anfs/events
	Logf   func(string, ...any)

	mu     sync.Mutex
	months map[string]*month // <scope>/<YYYY-MM>
}

// month is the ids in one month's two files and how far each file was read
type month struct {
	ids map[string]bool
	off map[string]int64 // file name: offset past its last complete line read
}

// Action is what Handle did with a message.
type Action string

const (
	Recorded   Action = "recorded"
	Known      Action = "known"      // its id is in the files: acked, nothing written
	Terminated Action = "terminated" // invalid or mismatched: never in the record
	Retry      Action = "retry"      // the append failed: delivered again
)

// Record is one message's subject and body; the caller acks by the action.
func (r *Recorder) Record(subject string, body []byte) (Action, error) {
	ev, err := cloudevent.Parse(body)
	if err != nil {
		r.logf("recorder: %s: not recorded: %v; terminated", subject, err)
		return Terminated, err
	}
	if want := ev.NatsSubject(); subject != want {
		err := errors.New("its event's subject is " + want)
		r.logf("recorder: %s: not recorded: %v (subject.mismatch); terminated", subject, err)
		return Terminated, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	mo := r.month(ev.Scope, ev.Month())
	if !mo.ids[ev.ID] {
		r.catchUp(ev.Scope, ev.Month(), mo)
	}
	if mo.ids[ev.ID] {
		return Known, nil
	}
	line, err := cloudevent.Canonical(body)
	if err != nil {
		return Terminated, err
	}
	if err := cloudevent.AppendRecorded(r.Events, ev.Scope, ev.Month(), line); err != nil {
		r.logf("recorder: %s: appending %s/%s.bus.jsonl: %v (delivered again)", subject, ev.Scope, ev.Month(), err)
		return Retry, err
	}
	mo.ids[ev.ID] = true
	return Recorded, nil
}

func (r *Recorder) month(scope, m string) *month {
	if r.months == nil {
		r.months = map[string]*month{}
	}
	k := scope + "/" + m
	mo := r.months[k]
	if mo == nil {
		mo = &month{ids: map[string]bool{}, off: map[string]int64{}}
		r.months[k] = mo
	}
	return mo
}

// catchUp reads the ids the month's files gained past their offsets (complete lines only)
func (r *Recorder) catchUp(scope, m string, mo *month) {
	for _, name := range []string{m + ".jsonl", m + ".bus.jsonl"} {
		f, err := os.Open(filepath.Join(r.Events, scope, name))
		if err != nil {
			continue
		}
		if _, err := f.Seek(mo.off[name], io.SeekStart); err != nil {
			f.Close()
			continue
		}
		br := bufio.NewReaderSize(f, 64<<10)
		off := mo.off[name]
		for {
			l, err := br.ReadBytes('\n')
			if err != nil {
				break // the end, or a line with no newline yet: read again next time
			}
			off += int64(len(l))
			var id struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(l, &id) == nil && id.ID != "" {
				mo.ids[id.ID] = true
			}
		}
		mo.off[name] = off
		f.Close()
	}
}

// Handle is one message: recorded or known acked, terminated, or delivered again in 30 s.
func (r *Recorder) Handle(m jetstream.Msg) {
	switch a, _ := r.Record(m.Subject(), m.Data()); a {
	case Recorded, Known:
		m.Ack()
	case Terminated:
		m.Term()
	default:
		m.NakWithDelay(30 * time.Second)
	}
}

func (r *Recorder) logf(f string, a ...any) {
	if r.Logf != nil {
		r.Logf(f, a...)
	}
}

// Consuming is what Run needs of the bus.
type Consuming interface {
	Consume(ctx context.Context, cfg jetstream.ConsumerConfig, h func(jetstream.Msg)) error
}

// Run consumes until ctx is done.
func (r *Recorder) Run(ctx context.Context, b Consuming) error {
	return b.Consume(ctx, Config(), r.Handle)
}
