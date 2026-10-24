package supervise

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/harness"
	"github.com/google/uuid"
)

// Defaults.
const (
	DefaultMaxRuns = 2
	Grace          = 30 * time.Second // SIGTERM, then SIGKILL this much later
	AdoptCheck     = 10 * time.Second
)

// The wrapper every harness runs under: it records the harness's exit status in the run's
// directory, so a run that ends while anfsd is down (or adopted) still says how it ended.
const wrapper = `f=$1; shift; "$@"; s=$?; echo "$s" > "$f"; exit "$s"`

// Supervisor starts and watches runs. Fill the fields, then Start.
type Supervisor struct {
	Store   Store
	Events  string // the spool: run.* go there like every event
	MaxRuns int    // host-wide (ANFSD_MAX_RUNS)
	Anfs    string // the anfs program (anfs work new|path|env)
	Grace   time.Duration
	Check   time.Duration // the adopted runs' check
	// RetryDelay is the wait before a retry (attempt 1, 2): 30 s, then 2 min
	RetryDelay func(attempt int) time.Duration
	Env        []string // the environment runs start from (anfsd's)
	Logf       func(string, ...any)
	// RunEventsMax is the most lines a run's spool may put in the log (ANFSD_RUN_EVENTS_MAX)
	RunEventsMax int
	// ClaudeBare is ANFSD_CLAUDE_BARE=on: claude runs with --bare (an API key, no OAuth)
	ClaudeBare bool

	mu      sync.Mutex
	ctx     context.Context
	queue   []*Record
	running map[string]int // per role (<space id>/<role>), adopted runs included
	total   int
	conc    map[string]int // a role's concurrency, as the rules last loaded say
	adopted map[string]*adoptee
	working []*Record              // at Open: the records that say working
	byEvent map[string]string      // <rule key>|<event id> -> run id (any record: Seen)
	handled map[string]bool        // <rule key>|<event id> with a run not rejected for queue or rate
	unended map[string]bool        // <rule key>|<event id> with a run not ended
	starts  map[string][]time.Time // rule key -> runs entering the queue (rate:)
	held    map[string][]*Record   // rule key -> runs held for its rate or its role's queue, oldest first
	timers  map[string]*time.Timer // rule key -> the rate timer while it holds runs
	runsOf  map[string]map[string]bool
	ingest  map[string]*ingester // per working run: its spool's cursors (ingest.go)
	ended   []*Record            // at Open: the records that say ended (drainRecent)
	wake    chan struct{}
	wg      sync.WaitGroup
}

type adoptee struct {
	r        *Record
	termSent time.Time
}

// RoleKey is <space id>/<role>.
func RoleKey(spaceID, role string) string { return spaceID + "/" + role }

// RuleKey is <space id>/<rule>.
func RuleKey(spaceID, rule string) string { return spaceID + "/" + rule }

func (r *Record) roleKey() string { return RoleKey(r.SpaceID, r.Role) }
func (r *Record) ruleKey() string { return RuleKey(r.SpaceID, r.Rule) }

// NewID is a run id (UUIDv7).
func NewID() string { return uuid.Must(uuid.NewV7()).String() }

// Open reads the records: the indexes (one run per rule and event, rate:, loop), the queue (runs
// submitted and not started), and the working runs counted against their roles. Nothing starts:
// `anfsd match` decides on this alone; Start goes on from it.
func (s *Supervisor) Open() error {
	if s.MaxRuns <= 0 {
		s.MaxRuns = DefaultMaxRuns
	}
	if s.Grace <= 0 {
		s.Grace = Grace
	}
	if s.Check <= 0 {
		s.Check = AdoptCheck
	}
	if s.Anfs == "" {
		s.Anfs = "anfs"
	}
	if s.RetryDelay == nil {
		s.RetryDelay = func(a int) time.Duration {
			if a <= 1 {
				return 30 * time.Second
			}
			return 2 * time.Minute
		}
	}
	if s.Logf == nil {
		s.Logf = func(string, ...any) {}
	}
	s.running, s.conc, s.adopted = map[string]int{}, map[string]int{}, map[string]*adoptee{}
	s.byEvent, s.starts, s.runsOf = map[string]string{}, map[string][]time.Time{}, map[string]map[string]bool{}
	s.handled, s.unended, s.held, s.timers = map[string]bool{}, map[string]bool{}, map[string][]*Record{}, map[string]*time.Timer{}
	s.queue = nil
	s.wake = make(chan struct{}, 1)
	s.working, s.ended, s.ingest = nil, nil, map[string]*ingester{}
	recs, err := s.Store.All()
	if err != nil {
		return err
	}
	for _, r := range recs {
		s.index(r)
		switch r.State {
		case Submitted:
			if r.Held != "" {
				s.held[r.ruleKey()] = append(s.held[r.ruleKey()], r) // ids sort by time: the order holds
			} else {
				s.queue = append(s.queue, r)
			}
		case Working:
			s.working = append(s.working, r)
			s.running[r.roleKey()]++
			s.total++
		default:
			if Ended(r.State) {
				s.ended = append(s.ended, r)
			}
		}
	}
	return nil
}

// Start opens the records, adopts the working runs that are alive and ends the others, then
// schedules until ctx is done. Runs still working when it is done are left running.
func (s *Supervisor) Start(ctx context.Context) error {
	s.ctx = ctx
	if err := os.MkdirAll(s.Store.Dir, 0o700); err != nil {
		return err
	}
	if err := s.Open(); err != nil {
		return err
	}
	s.drainRecent(s.ended)
	s.ended = nil
	for _, r := range s.working {
		if s.alive(r) {
			s.adopted[r.ID] = &adoptee{r: r}
			s.Logf("run %s (%s) adopted: pid %d", r.ID, r.Rule, r.Pid)
			if _, err := os.Stat(s.Store.SpoolDir(r.ID)); err == nil {
				s.drain(r)
				s.watchSpool(r)
			}
			continue
		}
		s.running[r.roleKey()]--
		s.total--
		s.finishDead(r, false)
	}
	s.working = nil
	if len(s.adopted) > 0 {
		s.wg.Add(1)
		go s.watchAdopted()
	}
	s.wg.Add(1)
	go s.loop()
	s.signal()
	return nil
}

// Wait blocks until the scheduler and every run's goroutine have returned (after ctx is done).
func (s *Supervisor) Wait() { s.wg.Wait() }

func (s *Supervisor) index(r *Record) {
	k := r.ruleKey() + "|" + r.Event
	s.byEvent[k] = r.ID
	if !(r.State == Rejected && (r.Reason == "queue" || r.Reason == "rate")) {
		s.handled[k] = true
	}
	if !Ended(r.State) {
		s.unended[k] = true
	}
	// rate: counts a run when it entered the queue (a record from before 02d: when submitted)
	when := r.Queued
	if when == "" && r.Held == "" && r.State != Rejected {
		when = r.Submitted
	}
	if t, err := time.Parse(time.RFC3339, when); err == nil && when != "" {
		s.starts[r.ruleKey()] = append(s.starts[r.ruleKey()], t)
	}
	if s.runsOf[r.ruleKey()] == nil {
		s.runsOf[r.ruleKey()] = map[string]bool{}
	}
	s.runsOf[r.ruleKey()][r.Last8()] = true
}

// Seen says whether the rule already has a record for the event, a rejection included: a NATS
// redelivery is acked without a second run.rejected.
func (s *Supervisor) Seen(spaceID, rule, eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byEvent[RuleKey(spaceID, rule)+"|"+eventID]
	return ok
}

// Handled says whether the rule has a run for the event that is not rejected for queue or rate
// (02d): an unhandled event is what anfs agents redispatch takes.
func (s *Supervisor) Handled(spaceID, rule, eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handled[RuleKey(spaceID, rule)+"|"+eventID]
}

// Unended says whether the rule has a run for the event that has not ended (queued, held,
// working).
func (s *Supervisor) Unended(spaceID, rule, eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unended[RuleKey(spaceID, rule)+"|"+eventID]
}

// Held is how many runs the rule holds, and why the newest is held (rate or queue).
func (s *Supervisor) Held(spaceID, rule string) (int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.held[RuleKey(spaceID, rule)]
	if len(h) == 0 {
		return 0, ""
	}
	return len(h), h[len(h)-1].Held
}

// Starts is how many runs of the rule entered the queue since t (rate:).
func (s *Supervisor) Starts(spaceID, rule string, since time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.starts[RuleKey(spaceID, rule)] {
		if !t.Before(since) {
			n++
		}
	}
	return n
}

// IsRunOf says whether an actor (<harness>:<last 8 of a run id>) is a run of the rule (loop).
func (s *Supervisor) IsRunOf(spaceID, rule, actor string) bool {
	_, last8, ok := strings.Cut(actor, ":")
	if !ok || len(last8) != 8 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runsOf[RuleKey(spaceID, rule)][last8]
}

// Full says whether a run of the role would have to wait beyond its queue: no free slot (the
// role's or the host's), or others already waiting, and queue runs waiting.
func (s *Supervisor) Full(spaceID, role string, concurrency, queue int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.full(RoleKey(spaceID, role), concurrency, queue)
}

func (s *Supervisor) full(k string, concurrency, queue int) bool {
	w := 0
	for _, r := range s.queue {
		if r.roleKey() == k {
			w++
		}
	}
	if w == 0 && s.running[k] < concurrency && s.total < s.MaxRuns {
		return false
	}
	return w >= queue
}

// Counts is how many runs are working, waiting in a queue, and held (anfs bus status reads the
// records instead).
func (s *Supervisor) Counts() (working, queued, held int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.held {
		held += len(h)
	}
	return s.total, len(s.queue), held
}

// SetConcurrency records a role's concurrency from the rules just loaded.
func (s *Supervisor) SetConcurrency(spaceID, role string, n int) {
	s.mu.Lock()
	s.conc[RoleKey(spaceID, role)] = n
	s.mu.Unlock()
	s.signal()
}

// Submit records a run (run.json, the event's line, the prompt), emits run.submitted, and queues
// it, or holds it when r.Held says so (rate or queue; detail says why, in run.submitted's data).
// The dispatcher acks the event after this.
func (s *Supervisor) Submit(r *Record, eventLine []byte, prompt, detail string) error {
	r.State, r.Submitted, r.Attempt = Submitted, now(), 1
	if r.Held != "" {
		r.HeldSince = r.Submitted
	} else {
		r.Queued = r.Submitted
	}
	if err := s.write(r, eventLine, prompt); err != nil {
		return err
	}
	s.mu.Lock()
	s.index(r)
	if r.Held != "" {
		s.held[r.ruleKey()] = append(s.held[r.ruleKey()], r)
	} else {
		s.queue = append(s.queue, r)
	}
	s.mu.Unlock()
	data := map[string]string{"rule": r.Rule, "role": r.Role, "harness": r.Harness, "event": r.Event}
	if r.Job != "" {
		data["job"] = r.Job
	}
	if r.Held != "" {
		data["held"], data["detail"] = r.Held, detail
	}
	if r.RedispatchBy != "" {
		data["redispatch"] = r.RedispatchBy
	}
	s.emit(r, "run.submitted", data)
	s.signal()
	return nil
}

// Reject records a run that is not started (stale, rate, queue, loop) and emits run.rejected.
func (s *Supervisor) Reject(r *Record, eventLine []byte, reason, detail string) error {
	r.State, r.Reason, r.Detail = Rejected, reason, detail
	r.Submitted, r.Ended = now(), now()
	if err := s.write(r, eventLine, ""); err != nil {
		return err
	}
	s.mu.Lock()
	s.index(r)
	s.mu.Unlock()
	data := map[string]string{"rule": r.Rule, "reason": reason, "event": r.Event}
	if detail != "" {
		data["detail"] = harness.Cut(detail, 500)
	}
	s.emit(r, "run.rejected", data)
	return nil
}

func (s *Supervisor) write(r *Record, eventLine []byte, prompt string) error {
	d := s.Store.RunDir(r.ID)
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	line := bytes.TrimRight(eventLine, "\n")
	if err := os.WriteFile(filepath.Join(d, "event.json"), append(line, '\n'), 0o600); err != nil {
		return err
	}
	if prompt != "" {
		if err := os.WriteFile(filepath.Join(d, "prompt.md"), []byte(prompt), 0o600); err != nil {
			return err
		}
	}
	return s.Store.Save(r)
}

func (s *Supervisor) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Supervisor) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
			s.schedule()
		}
	}
}

func (s *Supervisor) concurrency(r *Record) int {
	if n, ok := s.conc[r.roleKey()]; ok && n > 0 {
		return n
	}
	if r.Concurrency > 0 {
		return r.Concurrency
	}
	return 1
}

// schedule moves held runs into their queue when their rule's rate and their role's queue let
// them, then starts what may start: oldest first, per role and host-wide
func (s *Supervisor) schedule() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	s.releaseHeld()
	for i := 0; i < len(s.queue); {
		r := s.queue[i]
		if by, ok := s.Store.Canceled(r.ID); ok {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			s.endLater(r, Canceled, "", "", by)
			continue
		}
		if s.total >= s.MaxRuns {
			return
		}
		if s.running[r.roleKey()] >= s.concurrency(r) {
			i++
			continue
		}
		// stale is checked again when a run would start: it may have waited past it (held, or a
		// long queue); a redispatched run is exempt
		if old, ok := s.tooOld(r); ok {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			s.endLater(r, Rejected, "stale", old, "")
			continue
		}
		s.queue = append(s.queue[:i], s.queue[i+1:]...)
		s.running[r.roleKey()]++
		s.total++
		s.wg.Add(1)
		go s.execute(r)
	}
}

func (s *Supervisor) release(r *Record) {
	s.mu.Lock()
	s.running[r.roleKey()]--
	s.total--
	s.mu.Unlock()
	s.signal()
}

// --- a run --------------------------------------------------------------------------------------

func (s *Supervisor) execute(r *Record) {
	defer s.wg.Done()
	defer s.release(r)
	if by, ok := s.Store.Canceled(r.ID); ok {
		s.end(r, Canceled, "", "", nil, by)
		return
	}
	env, cwd, tmp, reason, err := s.prepare(r)
	if err != nil {
		s.end(r, Failed, reason, err.Error(), nil, "")
		return
	}
	dir := s.Store.RunDir(r.ID)
	body, _ := os.ReadFile(filepath.Join(dir, "prompt.md"))
	eventLine, _ := os.ReadFile(filepath.Join(dir, "event.json"))
	write := append([]string{tmp}, r.Write...)
	read := append([]string{r.SpaceDir}, r.Read...)
	if r.Job != "" {
		write = append([]string{cwd}, write...)
	}
	spoolDir, err := s.makeSpool(r)
	if err != nil {
		s.end(r, Failed, "spawn", err.Error(), nil, "")
		return
	}
	spec := harness.Spec{
		Harness: r.Harness, Model: r.Model, BudgetUSD: r.BudgetUSD, Args: r.Args,
		Name: r.Rule + ":" + r.Last8(), Cwd: cwd, RunDir: dir, Timeout: r.TimeoutDur(),
		Write: write, Read: read, Spool: spoolDir, Bare: s.ClaudeBare,
		Home: getEnv(env, "HOME"), Secrets: harness.Secrets(getEnv(env, "HOME"), getEnv(env, "XDG_STATE_HOME")),
		Prompt: string(body) + Footer(r, cwd, bytes.TrimSpace(eventLine), write, read),
	}
	cmd, err := harness.Build(spec)
	if err != nil {
		s.end(r, Failed, "spawn", err.Error(), nil, "")
		return
	}
	for name, b := range cmd.Files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			s.end(r, Failed, "spawn", err.Error(), nil, "")
			return
		}
	}
	env = append(env, cmd.Env...)
	env = append(env,
		"ANFS_ACTOR="+r.Harness+":"+r.Last8(),
		"ANFS_AS="+r.Address,
		"ANFS_RUN="+r.ID,
		"ANFS_EVENT="+filepath.Join(dir, "event.json"),
		"ANFS_EVENTS_SPOOL="+spoolDir,
	)
	out, err := os.OpenFile(filepath.Join(dir, "out.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		s.end(r, Failed, "spawn", err.Error(), nil, "")
		return
	}
	os.Remove(filepath.Join(dir, "exit"))
	c := exec.Command("/bin/sh", append([]string{"-c", wrapper, "anfsd-run", filepath.Join(dir, "exit")}, cmd.Argv...)...)
	c.Dir, c.Env, c.Stdout, c.Stderr = cwd, env, out, out
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := c.Start(); err != nil {
		out.Close()
		s.end(r, Failed, "spawn", err.Error(), nil, "")
		return
	}
	out.Close()
	r.Argv, r.Cwd, r.Pid, r.Pgid = cmd.Argv, cwd, c.Process.Pid, c.Process.Pid
	r.PsStart = psStart(r.Pid)
	r.State, r.Started = Working, now()
	r.Exit, r.Reason, r.Detail = nil, "", ""
	s.save(r)
	s.watchSpool(r)
	data := map[string]string{"rule": r.Rule, "harness": r.Harness, "attempt": strconv.Itoa(r.Attempt)}
	if r.Job != "" {
		data["job"], data["branch"] = r.Job, r.Job
	}
	s.emit(r, "run.working", data)
	s.Logf("run %s (%s) working: pid %d in %s", r.ID, r.Rule, r.Pid, cwd)

	done := make(chan int, 1)
	go func() {
		c.Wait()
		done <- exitCode(c.ProcessState)
	}()
	deadline := time.NewTimer(r.TimeoutDur())
	defer deadline.Stop()
	var kill <-chan time.Time
	timedOut := false
	for {
		select {
		case code := <-done:
			if timedOut {
				s.killLater(r.Pgid)
			}
			s.finish(r, code, timedOut)
			return
		case <-deadline.C:
			timedOut = true
			s.Logf("run %s (%s): timeout after %s, SIGTERM to its group", r.ID, r.Rule, r.Timeout)
			syscall.Kill(-r.Pgid, syscall.SIGTERM)
			kill = time.After(s.Grace)
		case <-kill:
			syscall.Kill(-r.Pgid, syscall.SIGKILL)
		case <-s.ctx.Done():
			// anfsd stops: the run goes on in its own session, adopted at the next start
			return
		}
	}
}

// killLater: what is left of a group after its leader ended gets SIGKILL after the grace
func (s *Supervisor) killLater(pgid int) {
	if syscall.Kill(-pgid, 0) != nil {
		return
	}
	time.AfterFunc(s.Grace, func() { syscall.Kill(-pgid, syscall.SIGKILL) })
}

// prepare: the job (made by anfs work new, or this run's own from an earlier try), its variables
// and directory; with job: none the space's directory and a TMPDIR in the run's directory. The
// environment starts from the allow-list (runEnv), and git is told to ask nobody.
func (s *Supervisor) prepare(r *Record) (env []string, cwd, tmp, reason string, err error) {
	env = runEnv(s.Env, r.Harness)
	if r.Job == "" {
		tmp = filepath.Join(s.Store.RunDir(r.ID), "tmp")
		if err := os.MkdirAll(tmp, 0o700); err != nil {
			return nil, "", "", "spawn", err
		}
		return gitNoCredentials(append(env, "TMPDIR="+tmp)), r.SpaceDir, tmp, "", nil
	}
	path, rc, _ := s.anfs("work", "path", r.Job)
	if rc == 0 && !r.JobMade {
		return nil, "", "", "job", fmt.Errorf("a job %s exists that this run did not make (anfs work list)", r.Job)
	}
	if rc != 0 {
		_, rc, msg := s.anfs(append([]string{"work", "new", r.Job}, r.Repos...)...)
		if rc != 0 {
			return nil, "", "", "job", fmt.Errorf("anfs work new %s %s: %s", r.Job, strings.Join(r.Repos, " "), msg)
		}
		r.JobMade = true
		s.save(r)
		if path, rc, msg = s.anfs("work", "path", r.Job); rc != 0 {
			return nil, "", "", "job", fmt.Errorf("anfs work path %s: %s", r.Job, msg)
		}
	}
	cwd = strings.TrimSpace(path)
	out, rc, msg := s.anfs("work", "env", r.Job)
	if rc != 0 {
		return nil, "", "", "job", fmt.Errorf("anfs work env %s: %s", r.Job, msg)
	}
	vars, err := ParseEnv(out)
	if err != nil {
		return nil, "", "", "job", fmt.Errorf("anfs work env %s: %v", r.Job, err)
	}
	for k, v := range vars {
		env = setEnv(env, k, v)
		if k == "TMPDIR" {
			tmp = v
		}
	}
	if tmp == "" {
		tmp = filepath.Join(s.Store.RunDir(r.ID), "tmp")
		env = setEnv(env, "TMPDIR", tmp)
	}
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return nil, "", "", "job", err
	}
	return gitNoCredentials(env), cwd, tmp, "", nil
}

// anfs runs the anfs program: its stdout, exit status, and stderr's last line
func (s *Supervisor) anfs(args ...string) (string, int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := exec.CommandContext(ctx, s.Anfs, args...)
	c.Env = append(gitNoCredentials(runEnv(s.Env, "")), "ANFS_EVENTS=off")
	c.Stdin = nil
	var out, errb bytes.Buffer
	c.Stdout, c.Stderr = &out, &errb
	err := c.Run()
	rc := 0
	if err != nil {
		rc = 1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			rc = ee.ExitCode()
		}
	}
	msg := strings.TrimSpace(errb.String())
	if i := strings.LastIndex(msg, "\n"); i >= 0 {
		msg = msg[i+1:]
	}
	if msg == "" && err != nil {
		msg = err.Error()
	}
	return out.String(), rc, msg
}

// ParseEnv reads `anfs work env`'s lines: `unset ...` and `export NAME=VALUE`, the value as bash's
// printf %q writes it (backslash escapes, or $'...').
func ParseEnv(out string) (map[string]string, error) {
	vars := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "unset ") || strings.HasPrefix(l, "#") {
			continue
		}
		l, ok := strings.CutPrefix(l, "export ")
		if !ok {
			return nil, fmt.Errorf("not an export line: %q", l)
		}
		k, v, ok := strings.Cut(l, "=")
		if !ok {
			return nil, fmt.Errorf("not NAME=VALUE: %q", l)
		}
		uv, err := unquote(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", k, err)
		}
		vars[k] = uv
	}
	return vars, sc.Err()
}

func unquote(v string) (string, error) {
	if strings.HasPrefix(v, "$'") && strings.HasSuffix(v, "'") {
		return strconv.Unquote(`"` + strings.ReplaceAll(v[2:len(v)-1], `"`, `\"`) + `"`)
	}
	if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") && len(v) >= 2 {
		return v[1 : len(v)-1], nil
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		if v[i] == '\\' && i+1 < len(v) {
			i++
		}
		b.WriteByte(v[i])
	}
	return b.String(), nil
}

func setEnv(env []string, k, v string) []string {
	for i, kv := range env {
		if strings.HasPrefix(kv, k+"=") {
			env[i] = k + "=" + v
			return env
		}
	}
	return append(env, k+"="+v)
}

func exitCode(ps *os.ProcessState) int {
	if ps == nil {
		return -1
	}
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return ps.ExitCode()
}

// finish: how a run that ended reads, from its exit, the cancel marker and the timeout
func (s *Supervisor) finish(r *Record, code int, timedOut bool) {
	c := code
	switch by, canceled := s.Store.Canceled(r.ID); {
	case canceled:
		s.end(r, Canceled, "", "", &c, by)
	case timedOut:
		s.end(r, Failed, "timeout", "after "+r.Timeout, &c, "")
	case code == 0:
		s.end(r, Completed, "", "", &c, "")
	default:
		s.end(r, Failed, "exit", "", &c, "")
	}
}

// finishDead: a working run whose process is gone and that anfsd did not wait for (it was down,
// or the run was adopted): its exit file if it left one, else orphaned
func (s *Supervisor) finishDead(r *Record, timedOut bool) {
	if by, ok := s.Store.Canceled(r.ID); ok {
		s.end(r, Canceled, "", "", nil, by)
		return
	}
	if timedOut {
		s.end(r, Failed, "timeout", "after "+r.Timeout, nil, "")
		return
	}
	if code, ok := s.Store.ExitStatus(r.ID); ok {
		if code == 0 {
			s.end(r, Completed, "", "", &code, "")
		} else {
			s.end(r, Failed, "exit", "", &code, "")
		}
		return
	}
	s.end(r, Failed, "orphaned", "the process ended while anfsd was not watching it, without an exit status", nil, "")
}

// end records a final state and emits it; a failed exit with retries left is queued again. What
// the run wrote into its spool goes into the log first, so its events come before its end.
func (s *Supervisor) end(r *Record, state, reason, detail string, exit *int, by string) {
	s.drainFinal(r)
	r.State, r.Reason, r.Detail, r.Exit, r.By, r.Ended = state, reason, detail, exit, by, now()
	data := map[string]string{"rule": r.Rule}
	if r.Job != "" {
		data["job"] = r.Job
	}
	switch state {
	case Completed:
		r.Summary = s.summary(r)
		r.Commits = s.commits(r)
		data["commits"] = commitsString(r.Repos, r.Commits)
		data["summary"] = r.Summary
	case Failed:
		data["reason"] = reason
		if exit != nil {
			data["exit"] = strconv.Itoa(*exit)
		}
		if detail != "" {
			data["detail"] = harness.Cut(detail, 500)
		}
		data["attempt"] = strconv.Itoa(r.Attempt)
	case Canceled:
		data["by"] = by
	case Rejected:
		data["reason"], data["event"] = reason, r.Event
		if detail != "" {
			data["detail"] = harness.Cut(detail, 500)
		}
	}
	retry := state == Failed && reason == "exit" && r.Attempt <= r.Retries && s.ctx.Err() == nil
	if retry {
		data["retry"] = fmt.Sprintf("%d of %d", r.Attempt, r.Retries)
	}
	s.save(r)
	s.emit(r, "run."+state, data)
	s.Logf("run %s (%s) %s%s", r.ID, r.Rule, state, map[bool]string{true: " (" + reason + ")", false: ""}[reason != ""])
	if !retry {
		s.mu.Lock()
		delete(s.unended, r.ruleKey()+"|"+r.Event)
		s.mu.Unlock()
		return
	}
	delay := s.RetryDelay(r.Attempt)
	r.Attempt++
	r.State, r.Ended, r.Pid, r.Pgid, r.PsStart = Submitted, "", 0, 0, ""
	s.save(r)
	s.Logf("run %s (%s): retry %d of %d in %s", r.ID, r.Rule, r.Attempt-1, r.Retries, delay)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-s.ctx.Done():
			return // queued again at the next start (its record says submitted)
		case <-t.C:
		}
		s.mu.Lock()
		s.queue = append(s.queue, r)
		s.mu.Unlock()
		s.signal()
	}()
}

func (s *Supervisor) save(r *Record) {
	if err := s.Store.Save(r); err != nil {
		s.Logf("run %s: writing run.json: %v", r.ID, err)
	}
}

// emit a run.* event into the rule's space, subject the run id, actor anfsd. A line over the
// profile's 4096 bytes is cut in its summary until it fits.
func (s *Supervisor) emit(r *Record, typ string, data map[string]string) {
	for {
		_, err := cloudevent.Append(s.Events, cloudevent.New{
			Scope: r.SpaceID, Type: typ, Subject: r.ID, Actor: "anfsd", SpacePath: r.Space, Data: data,
		})
		if err == nil {
			return
		}
		if sum := data["summary"]; strings.Contains(err.Error(), "over") && len(sum) > 64 {
			data["summary"] = harness.Cut(sum, len(sum)/2)
			continue
		}
		s.Logf("run %s: %s not recorded: %v", r.ID, typ, err)
		return
	}
}

// summary: the final answer in out.log (its last 4 MiB)
func (s *Supervisor) summary(r *Record) string {
	f, err := os.Open(filepath.Join(s.Store.RunDir(r.ID), "out.log"))
	if err != nil {
		return ""
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() > 4<<20 {
		f.Seek(st.Size()-4<<20, io.SeekStart)
	}
	b, _ := io.ReadAll(f)
	return harness.Summary(r.Harness, b)
}

// commits: per repo of the job, the commits its branch has that main has not
func (s *Supervisor) commits(r *Record) map[string]int {
	if r.Job == "" || r.Cwd == "" {
		return nil
	}
	out := map[string]int{}
	for _, repo := range r.Repos {
		b, err := exec.Command("git", "-C", filepath.Join(r.Cwd, repo), "rev-list", "--count", "main..HEAD").Output()
		if err != nil {
			out[repo] = -1
			continue
		}
		out[repo], _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	return out
}

func commitsString(repos []string, c map[string]int) string {
	var parts []string
	for _, repo := range repos {
		n, ok := c[repo]
		if !ok || n < 0 {
			parts = append(parts, repo+":?")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s:%d", repo, n))
	}
	return strings.Join(parts, " ")
}

// --- adopted runs -------------------------------------------------------------------------------

// alive: the pid exists and started when the record says (a reused pid is not the run)
func (s *Supervisor) alive(r *Record) bool {
	if r.Pid <= 0 {
		return false
	}
	if err := syscall.Kill(r.Pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		return false
	}
	return r.PsStart == "" || psStart(r.Pid) == r.PsStart
}

// psStart is the process's start time as ps prints it (lstart: the same on macOS and procps)
func psStart(pid int) string {
	b, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return ""
	}
	return strings.Join(strings.Fields(string(b)), " ")
}

// watchAdopted checks the adopted runs every Check while there are any: ended, they are finished
// from what they left; past their timeout, their group gets SIGTERM, then SIGKILL
func (s *Supervisor) watchAdopted() {
	defer s.wg.Done()
	t := time.NewTicker(s.Check)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		s.mu.Lock()
		list := make([]*adoptee, 0, len(s.adopted))
		for _, a := range s.adopted {
			list = append(list, a)
		}
		s.mu.Unlock()
		for _, a := range list {
			r := a.r
			if !s.alive(r) {
				s.mu.Lock()
				delete(s.adopted, r.ID)
				s.mu.Unlock()
				s.finishDead(r, !a.termSent.IsZero())
				s.release(r)
				continue
			}
			started, err := time.Parse(time.RFC3339, r.Started)
			if err != nil {
				continue
			}
			switch {
			case a.termSent.IsZero() && time.Since(started) > r.TimeoutDur():
				s.Logf("run %s (%s): timeout after %s, SIGTERM to its group", r.ID, r.Rule, r.Timeout)
				syscall.Kill(-r.Pgid, syscall.SIGTERM)
				a.termSent = time.Now()
			case !a.termSent.IsZero() && time.Since(a.termSent) > s.Grace:
				syscall.Kill(-r.Pgid, syscall.SIGKILL)
			}
		}
		s.mu.Lock()
		n := len(s.adopted)
		s.mu.Unlock()
		if n == 0 {
			return
		}
	}
}

// Footer is what anfsd adds to every prompt: who the agent is, the rule, the event, the job,
// what it may not do, and how to end.
func Footer(r *Record, cwd string, eventLine []byte, write, read []string) string {
	var b strings.Builder
	b.WriteString("\n\n---\n\n")
	fmt.Fprintf(&b, "You are %s, started by anfsd's dispatch (rule %s in space %s, run %s), with nobody watching.\n\n", r.Address, r.Rule, r.Space, r.ID)
	fmt.Fprintf(&b, "The event that started you (also in $ANFS_EVENT):\n\n%s\n\n", eventLine)
	if r.Job != "" {
		fmt.Fprintf(&b, "Your job is %s (anfs work job %s, branch %s; repos: %s). Work and commit only there.\n\n", cwd, r.Job, r.Job, strings.Join(r.Repos, ", "))
	} else {
		fmt.Fprintf(&b, "You have no job: you work in the space's directory %s, read-only.\n\n", cwd)
	}
	fmt.Fprintf(&b, "You may write only in: %s. You may read: %s.\n\n", strings.Join(uniqSorted(write), ", "), strings.Join(uniqSorted(append(append([]string{}, read...), write...)), ", "))
	b.WriteString("You may not: " + strings.Join(harness.Denied, "; ") + "; or write anywhere else. Merge, install, push and `anfs report close` are the person's.\n\n")
	fmt.Fprintf(&b, "Events you emit go through `$ANFS_EVENTS_SPOOL` and are recorded as yours (run %s).\n\n", r.ID)
	b.WriteString("End with a summary of what you did and what is left for the person.\n")
	return b.String()
}

func uniqSorted(ps []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range ps {
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}
