// Package podman follows `podman events` and writes what containers and pods do into the host
// scope of the event log (through cloudevent.Append, like every event of anfsd's), so the shipper
// puts them on the bus as anfs.host.anfs.container.<verb> and anfs.host.anfs.pod.<verb>.
//
// One child, `podman events --format json --filter type=container --filter type=pod` (no --since:
// from now), one JSON object a line. Kept: a container's create, start, stop, died, remove, pause,
// unpause, and health_status when the container's health changes; a pod's create, start, stop,
// remove. Everything else (exec, attach, cleanup, init, sync, ...) is dropped. The same container,
// status and second are one event; at most Rate events a second overall, the rest counted and
// logged once a minute.
//
// Podman away (not installed; on macOS the machine stopped): the child cannot start or exits. It
// is started again after 5, 10, 30, then every 60 s (Backoff), one log line per change. A child
// that stays up Settle is connected: podman.connected (host scope) once, so a subscriber lists
// once to reconcile, and podman.disconnected when it goes. Nothing polls `podman ps`.
//
// The state for `anfs bus status` is $ANFSD_STATE_HOME/podman.json (written on each change and
// each try): {"state": "connected"|"away"|"off"|"starting", "since", "error", "next"}.
package podman

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
)

// Args are podman's arguments.
var Args = []string{"events", "--format", "json", "--filter", "type=container", "--filter", "type=pod"}

// Backoff is the wait before the try after <failed> tries that did not connect: 5, 10, 30, then 60 s.
func Backoff(failed int) time.Duration {
	steps := []time.Duration{5 * time.Second, 10 * time.Second, 30 * time.Second}
	if failed < 1 {
		return 0
	}
	if failed <= len(steps) {
		return steps[failed-1]
	}
	return 60 * time.Second
}

// Watcher is the follower. The zero values are the daemon's: podman on PATH, Backoff, a 2 s
// settle, 20 events a second, a dropped count logged once a minute.
type Watcher struct {
	Bin       string // "podman" (looked up on PATH at each try)
	Events    string // the spool: $XDG_STATE_HOME/anfs/events
	StateFile string // $ANFSD_STATE_HOME/podman.json; "" writes none
	Logf      func(string, ...any)

	Backoff    func(failed int) time.Duration // tests: shorter
	Settle     time.Duration                  // up this long (or a line read) is connected
	Stable     time.Duration                  // connected this long resets the backoff (1 min)
	Rate       float64                        // events a second (20)
	DropReport time.Duration                  // how often dropped events are logged (1 min)

	mu      sync.Mutex
	dropped int
	dropT   *time.Timer
	health  map[string]string    // container id: its last health
	seen    map[string]time.Time // container|status|second: when, for coalescing
	limiter *rate.Limiter
	lastLog string
}

// State is podman.json.
type State struct {
	State string `json:"state"`
	Since string `json:"since,omitempty"`
	Error string `json:"error,omitempty"`
	Next  string `json:"next,omitempty"`
}

func (w *Watcher) defaults() {
	if w.Bin == "" {
		w.Bin = "podman"
	}
	if w.Backoff == nil {
		w.Backoff = Backoff
	}
	if w.Settle == 0 {
		w.Settle = 2 * time.Second
	}
	if w.Stable == 0 {
		w.Stable = time.Minute
	}
	if w.Rate == 0 {
		w.Rate = 20
	}
	if w.DropReport == 0 {
		w.DropReport = time.Minute
	}
	if w.Logf == nil {
		w.Logf = func(string, ...any) {}
	}
	w.health = map[string]string{}
	w.seen = map[string]time.Time{}
	w.limiter = rate.NewLimiter(rate.Limit(w.Rate), int(w.Rate))
}

// Off records that ANFSD_PODMAN=off: nothing is started.
func (w *Watcher) Off() {
	w.writeState(State{State: "off", Since: now()})
}

// Run follows podman until ctx ends. Every wait is a timer or the child: no loop spins.
func (w *Watcher) Run(ctx context.Context) {
	w.defaults()
	w.writeState(State{State: "starting", Since: now()})
	failed := 0
	connected := false // podman.connected written and not yet its disconnected
	for ctx.Err() == nil {
		up, ranFor, err := w.once(ctx, &connected)
		if ctx.Err() != nil {
			break
		}
		if connected {
			connected = false
			w.emit("podman.disconnected", "", map[string]string{"error": errText(err)})
		}
		if up && ranFor >= w.Stable {
			failed = 0
		}
		failed++
		d := w.Backoff(failed)
		w.say(fmt.Sprintf("podman away: %s; next try in %s", errText(err), d), "podman away: "+errText(err))
		w.writeState(State{State: "away", Since: now(), Error: errText(err), Next: time.Now().Add(d).UTC().Format(time.RFC3339)})
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
	w.mu.Lock()
	if w.dropT != nil {
		w.dropT.Stop()
	}
	w.mu.Unlock()
}

// say logs a line when key differs from the last one (one line per change of state)
func (w *Watcher) say(line, key string) {
	if key == w.lastLog {
		return
	}
	w.lastLog = key
	w.Logf("%s", line)
}

func errText(err error) string {
	if err == nil {
		return "podman events ended"
	}
	return err.Error()
}

// once starts one child and reads it until it ends: whether it was connected, for how long, and
// why it ended.
func (w *Watcher) once(ctx context.Context, connected *bool) (bool, time.Duration, error) {
	bin, err := exec.LookPath(w.Bin)
	if err != nil {
		return false, 0, fmt.Errorf("%s not found on PATH", w.Bin)
	}
	cmd := exec.CommandContext(ctx, bin, Args...)
	cmd.Stdin = nil
	var stderr tail
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.StdoutPipe()
	if err != nil {
		return false, 0, err
	}
	if err := cmd.Start(); err != nil {
		return false, 0, fmt.Errorf("starting %s: %w", bin, err)
	}
	start := time.Now()
	var once sync.Once
	connect := func() {
		once.Do(func() {
			*connected = true
			w.say("podman connected: "+bin+" events", "connected")
			w.writeState(State{State: "connected", Since: now()})
			w.emit("podman.connected", "", nil)
		})
	}
	settle := time.AfterFunc(w.Settle, connect)
	lines := bufio.NewScanner(out)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	bad := false
	for lines.Scan() {
		connect()
		if err := w.Handle(lines.Bytes()); err != nil && !bad {
			bad = true
			w.Logf("podman: a line not understood (%v): %.200s", err, lines.Text())
		}
	}
	settle.Stop()
	once.Do(func() {}) // waits for a connect the timer may be running; none after this
	werr := cmd.Wait()
	ran := time.Since(start)
	msg := stderr.last()
	switch {
	case msg != "":
		werr = errors.New(msg)
	case werr != nil:
		werr = fmt.Errorf("podman events: %v", werr)
	}
	return *connected, ran, werr
}

// Line is what is read of a podman event.
type Line struct {
	Type       string            `json:"Type"`
	Status     string            `json:"Status"`
	ID         string            `json:"ID"`
	Name       string            `json:"Name"`
	Image      string            `json:"Image"`
	PodID      string            `json:"PodID"`
	ExitCode   *int              `json:"ContainerExitCode"`
	Health     string            `json:"health_status"`
	Time       int64             `json:"time"`
	Attributes map[string]string `json:"Attributes"`
}

var verbs = map[string]map[string]string{
	"container": {"create": "created", "start": "started", "stop": "stopped", "died": "died", "remove": "removed",
		"pause": "paused", "unpause": "unpaused", "health_status": "health"},
	"pod": {"create": "created", "start": "started", "stop": "stopped", "remove": "removed"},
}

// Event is what a line becomes: the short type, subject and data; ok false when it is dropped.
func (w *Watcher) Event(l Line) (typ, subject string, data map[string]string, ok bool) {
	verb, ok := verbs[l.Type][l.Status]
	if !ok || l.Name == "" {
		return "", "", nil, false
	}
	data = map[string]string{"id": short(l.ID)}
	if l.Type == "container" {
		if l.Status == "health_status" {
			if l.Health == "" || w.health[l.ID] == l.Health {
				return "", "", nil, false
			}
			w.health[l.ID] = l.Health
			data["health"] = l.Health
		}
		if l.Status == "remove" {
			delete(w.health, l.ID)
		}
		if l.Image != "" {
			data["image"] = l.Image
		}
		if l.PodID != "" {
			data["pod"] = short(l.PodID)
		}
		if l.Status == "died" && l.ExitCode != nil {
			data["exit"] = strconv.Itoa(*l.ExitCode)
		}
		if p := l.Attributes["io.pcm.name"]; p != "" {
			data["pcm"] = p
		}
	}
	// the same container, status and second: one event
	key := l.ID + "|" + l.Status + "|" + strconv.FormatInt(l.Time, 10)
	if _, dup := w.seen[key]; dup {
		return "", "", nil, false
	}
	n := time.Now()
	for k, t := range w.seen {
		if n.Sub(t) > 5*time.Second {
			delete(w.seen, k)
		}
	}
	w.seen[key] = n
	return l.Type + "." + verb, l.Name, data, true
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// Handle takes one line of podman's output: kept, it is written (within the rate); dropped, not.
func (w *Watcher) Handle(b []byte) error {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	var l Line
	if err := json.Unmarshal(b, &l); err != nil {
		return err
	}
	typ, subject, data, ok := w.Event(l)
	if !ok {
		return nil
	}
	if !w.limiter.Allow() {
		w.drop()
		return nil
	}
	w.emit(typ, subject, data)
	return nil
}

// drop counts one over the rate; the count is logged once a DropReport, only while there are some
func (w *Watcher) drop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dropped++
	if w.dropT != nil {
		return
	}
	w.dropT = time.AfterFunc(w.DropReport, func() {
		w.mu.Lock()
		n := w.dropped
		w.dropped, w.dropT = 0, nil
		w.mu.Unlock()
		w.Logf("podman: %d events over %g a second not recorded in the last %s", n, w.Rate, w.DropReport)
	})
}

func (w *Watcher) emit(typ, subject string, data map[string]string) {
	if _, err := cloudevent.Append(w.Events, cloudevent.New{Scope: "host", Type: typ, Subject: subject, Actor: "podman", Data: data}); err != nil {
		w.Logf("podman: %s %s not recorded: %v", typ, subject, err)
	}
}

func (w *Watcher) writeState(s State) {
	if w.StateFile == "" {
		return
	}
	b, _ := json.Marshal(s)
	tmp := w.StateFile + ".tmp"
	if err := os.MkdirAll(filepath.Dir(w.StateFile), 0o700); err != nil {
		return
	}
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err == nil {
		os.Rename(tmp, w.StateFile)
	}
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

// tail keeps the last 4 KiB a child wrote to stderr; last is its last non-empty line.
type tail struct {
	mu sync.Mutex
	b  []byte
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if len(t.b) > 4096 {
		t.b = t.b[len(t.b)-4096:]
	}
	return len(p), nil
}

func (t *tail) last() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ls := bytes.Split(bytes.TrimSpace(t.b), []byte("\n"))
	s := string(bytes.TrimSpace(ls[len(ls)-1]))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

var _ io.Writer = (*tail)(nil)
