// Package sink renders the report records into the report file (BACKLOG.md as a view): anfsd's
// durable consumer anfsd-sink-report (anfs.*.anfs.report.>) hands each event, one at a time, to
// `anfs report __sink` (core's plumbing) on stdin. anfsd knows nothing of the file: the render is
// core's, idempotent, under its own lock. A failure is delivered again, at most 5 times in all,
// then logged and dropped (the record is still there; the next report.* of it renders it, and so
// does `anfs report new` or `close` while anfsd is down).
package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Consumer is the durable consumer's name.
const Consumer = "anfsd-sink-report"

// Filter is what it consumes.
const Filter = "anfs.*.anfs.report.>"

// MaxDeliver is how often an event is tried in all.
const MaxDeliver = 5

// Sink runs the render.
type Sink struct {
	Anfs    string        // the anfs program
	Timeout time.Duration // per event (1 min)
	Logf    func(string, ...any)
	// Delay is the wait before delivery n+1 after n failed (5 s, 30 s, 2 min, 5 min)
	Delay func(n int) time.Duration
}

// Config is the consumer's.
func Config() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       Consumer,
		Description:   "anfsd's report sink: anfs report __sink renders each report.* into the report file",
		FilterSubject: Filter,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxDeliver:    MaxDeliver,
		MaxAckPending: 1, // one at a time
	}
}

func (s *Sink) delay(n int) time.Duration {
	if s.Delay != nil {
		return s.Delay(n)
	}
	return []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 5 * time.Minute, 5 * time.Minute}[min(max(n-1, 0), 4)]
}

// Render runs `anfs report __sink` with the event's line on stdin.
func (s *Sink) Render(ctx context.Context, line []byte) error {
	anfs := s.Anfs
	if anfs == "" {
		anfs = "anfs"
	}
	to := s.Timeout
	if to <= 0 {
		to = time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	c := exec.CommandContext(ctx, anfs, "report", "__sink")
	c.Stdin = bytes.NewReader(line)
	c.Env = append(c.Environ(), "ANFS_EVENTS=off")
	var errb bytes.Buffer
	c.Stderr = &errb
	if err := c.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = err.Error()
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return fmt.Errorf("anfs report __sink: exit %d: %s", ee.ExitCode(), msg)
		}
		return fmt.Errorf("anfs report __sink: %s", msg)
	}
	return nil
}

// Handle is one message: rendered and acked; a failure delivered again after a delay, or after
// the last try logged and dropped.
func (s *Sink) Handle(ctx context.Context, m jetstream.Msg) {
	err := s.Render(ctx, m.Data())
	if err == nil {
		m.Ack()
		return
	}
	n := uint64(1)
	if md, merr := m.Metadata(); merr == nil {
		n = md.NumDelivered
	}
	if n >= MaxDeliver {
		s.logf("%s: given up after %d tries: %v", m.Subject(), n, err)
		m.Term()
		return
	}
	d := s.delay(int(n))
	s.logf("%s: %v (try %d of %d; again in %s)", m.Subject(), err, n, MaxDeliver, d)
	m.NakWithDelay(d)
}

func (s *Sink) logf(f string, a ...any) {
	if s.Logf != nil {
		s.Logf(f, a...)
	}
}

// Consuming is what Run needs of the bus.
type Consuming interface {
	Consume(ctx context.Context, cfg jetstream.ConsumerConfig, h func(jetstream.Msg)) error
}

// Run consumes until ctx is done.
func (s *Sink) Run(ctx context.Context, b Consuming) error {
	return b.Consume(ctx, Config(), func(m jetstream.Msg) { s.Handle(ctx, m) })
}
