// Package bus is anfsd's connection to NATS: connect as the anfsd user with a backoff of 1, 2, 5,
// 10 and 30 s (then every 30 s) and never a tight loop, one log line per change of state, make
// sure of the stream ANFS (subjects anfs.>, file storage, limits retention, discard old, the age,
// size and duplicate window from anfsd.env), and publish with Nats-Msg-Id so the stream drops a
// line sent twice.
package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream is the one stream every event is on.
const Stream = "ANFS"

// Subjects are the stream's.
const Subjects = "anfs.>"

// MaxPending is the most publishes awaiting their acks.
const MaxPending = 256

// ErrForeignStream is another stream already holding anfs.>.
var ErrForeignStream = errors.New("another stream holds anfs.>")

// Backoff is the wait before the attempt after <attempt> failed ones (1, 2, 5, 10, then 30 s).
func Backoff(attempt int) time.Duration {
	steps := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}
	if attempt >= 1 && attempt <= len(steps) {
		return steps[attempt-1]
	}
	if attempt < 1 {
		return steps[0]
	}
	return 30 * time.Second
}

// StreamSettings are the stream's limits.
type StreamSettings struct {
	MaxAge    time.Duration
	MaxBytes  int64
	DupWindow time.Duration
	Replicas  int // 1 (tests may leave it 0)
}

// Options are how to reach NATS.
type Options struct {
	URL      string
	User     string
	Password string
	Name     string // the connection's name (anfsd)
	// ReconnectDelay replaces Backoff (tests)
	ReconnectDelay func(attempt int) time.Duration
}

// Bus is one connection.
type Bus struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	logf func(string, ...any)

	mu      sync.Mutex
	up      chan struct{} // closed while connected
	gen     int           // bumped at each (re)connect
	ensured int           // the gen the stream was last made sure of
}

// Connect starts connecting and returns at once: while NATS is not there the connection keeps
// trying (the backoff), and WaitConnected blocks until it is.
func Connect(o Options, logf func(string, ...any)) (*Bus, error) {
	b := &Bus{logf: logf, up: make(chan struct{})}
	delay := o.ReconnectDelay
	if delay == nil {
		delay = Backoff
	}
	name := o.Name
	if name == "" {
		name = "anfsd"
	}
	opts := []nats.Option{
		nats.Name(name),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.CustomReconnectDelay(delay),
		// a publish while disconnected fails at once instead of being buffered: the files are the
		// buffer, and the cursor stays where it is
		nats.ReconnectBufSize(-1),
		nats.ConnectHandler(func(*nats.Conn) { b.setUp(true, "connected to NATS") }),
		nats.ReconnectHandler(func(*nats.Conn) { b.setUp(true, "connected to NATS again") }),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			if c.IsClosed() {
				b.setUp(false, "")
				return
			}
			msg := "disconnected from NATS"
			if err != nil {
				msg += ": " + err.Error()
			}
			b.setUp(false, msg+" (retrying: 1, 2, 5, 10, then every 30 s)")
		}),
		nats.ClosedHandler(func(*nats.Conn) { b.setUp(false, "") }),
	}
	if o.User != "" {
		opts = append(opts, nats.UserInfo(o.User, o.Password))
	}
	nc, err := nats.Connect(o.URL, opts...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(MaxPending))
	if err != nil {
		nc.Close()
		return nil, err
	}
	b.nc, b.js = nc, js
	if nc.IsConnected() {
		b.setUp(true, "connected to NATS")
	} else {
		logf("NATS is not reachable at %s: retrying (1, 2, 5, 10, then every 30 s)", o.URL)
	}
	return b, nil
}

func (b *Bus) setUp(up bool, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	open := false
	select {
	case <-b.up:
		open = true
	default:
	}
	if up == open {
		return
	}
	if up {
		b.gen++
		close(b.up)
	} else {
		b.up = make(chan struct{})
	}
	if msg != "" {
		b.logf("%s", msg)
	}
}

// Connected says whether the connection is up now.
func (b *Bus) Connected() bool {
	b.mu.Lock()
	ch := b.up
	b.mu.Unlock()
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// WaitConnected blocks until the connection is up or ctx is done.
func (b *Bus) WaitConnected(ctx context.Context) error {
	b.mu.Lock()
	ch := b.up
	b.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NeedsEnsure says whether the stream has not been made sure of since the last (re)connect.
func (b *Bus) NeedsEnsure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ensured != b.gen
}

// EnsureStream creates ANFS, or updates its limits to these; ErrForeignStream (wrapped, naming it)
// when another stream already holds anfs.>.
func (b *Bus) EnsureStream(ctx context.Context, s StreamSettings) error {
	b.mu.Lock()
	gen := b.gen
	b.mu.Unlock()
	names := b.js.StreamNames(ctx, jetstream.WithStreamListSubject(Subjects))
	for n := range names.Name() {
		if n != Stream {
			return fmt.Errorf("%w: %s (remove it, or give it other subjects)", ErrForeignStream, n)
		}
	}
	if err := names.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return fmt.Errorf("listing streams: %w", err)
	}
	cfg := jetstream.StreamConfig{
		Name:       Stream,
		Subjects:   []string{Subjects},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Discard:    jetstream.DiscardOld,
		MaxAge:     s.MaxAge,
		MaxBytes:   s.MaxBytes,
		Duplicates: s.DupWindow,
		Replicas:   max(s.Replicas, 1),
	}
	if _, err := b.js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("stream %s: %w", Stream, err)
	}
	b.mu.Lock()
	b.ensured = gen
	b.mu.Unlock()
	return nil
}

// StreamInfo is the stream's current state (tests, status).
func (b *Bus) StreamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	st, err := b.js.Stream(ctx, Stream)
	if err != nil {
		return nil, err
	}
	return st.Info(ctx)
}

// Publish sends one event's line on subject with its id as Nats-Msg-Id; the future says whether
// the stream stored it (or dropped it as a duplicate, which is as good).
func (b *Bus) Publish(subject string, data []byte, id string) (jetstream.PubAckFuture, error) {
	m := nats.NewMsg(subject)
	m.Data = data
	m.Header.Set(jetstream.MsgIDHeader, id)
	m.Header.Set("Content-Type", "application/cloudevents+json")
	return b.js.PublishMsgAsync(m)
}

// Consume runs a durable pull consumer on ANFS until ctx is done: it is made (or its config
// updated) once NATS is up and the stream exists, retried with the backoff while it cannot be, and
// each message handed to h in turn (h acks). The client library keeps the subscription across a
// reconnect; a consumer that goes away (deleted by hand) is made again.
func (b *Bus) Consume(ctx context.Context, cfg jetstream.ConsumerConfig, h func(jetstream.Msg)) error {
	attempt := 0
	for {
		if err := b.WaitConnected(ctx); err != nil {
			return nil
		}
		cons, err := b.js.CreateOrUpdateConsumer(ctx, Stream, cfg)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			attempt++
			if attempt == 1 || attempt%10 == 0 {
				b.logf("consumer %s: %v (retrying: 1, 2, 5, 10, then every 30 s)", cfg.Durable, err)
			}
			t := time.NewTimer(Backoff(attempt))
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			}
			continue
		}
		if attempt > 0 {
			b.logf("consumer %s: ready", cfg.Durable)
		}
		attempt = 0
		gone := make(chan struct{}, 1)
		cc, err := cons.Consume(h, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
				select {
				case gone <- struct{}{}:
				default:
				}
			}
		}))
		if err != nil {
			attempt++
			t := time.NewTimer(Backoff(attempt))
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			cc.Drain()
			<-cc.Closed()
			return nil
		case <-gone:
			cc.Stop()
			b.logf("consumer %s: gone, making it again", cfg.Durable)
		}
	}
}

// UpdateConsumer changes a durable consumer's config (the dispatcher's filters after a reload).
func (b *Bus) UpdateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) error {
	_, err := b.js.CreateOrUpdateConsumer(ctx, Stream, cfg)
	return err
}

// JetStream is the context, for consumers (02b) and tests.
func (b *Bus) JetStream() jetstream.JetStream { return b.js }

// Close drains nothing: the shipper has waited for its acks.
func (b *Bus) Close() { b.nc.Close() }
