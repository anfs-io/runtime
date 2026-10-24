// Package supervise runs what the dispatcher submits: each run a child of anfsd in its own
// session, its record and files in ~/.local/state/anfsd/runs/<run id>/, its life as run.* events
// (A2A's task states: submitted, working, completed, failed, canceled, rejected) through the spool.
//
// A run is a task, not a service: it is never restarted (a rule's retries: aside, after a
// backoff). Stopping or restarting anfsd does not stop runs (their own sessions; systemd
// KillMode=process): at start anfsd adopts every working run whose pid is alive with the start
// time it recorded, checks them every 10 s while there are any, and marks the rest failed
// (orphaned), or finishes them from the exit status they left. Every wait blocks: on the child, a
// timer, or the scheduler's channel.
package supervise

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// States of a run.
const (
	Submitted = "submitted"
	Working   = "working"
	Completed = "completed"
	Failed    = "failed"
	Canceled  = "canceled"
	Rejected  = "rejected"
)

// Ended says whether a state is final.
func Ended(state string) bool {
	return state == Completed || state == Failed || state == Canceled || state == Rejected
}

// Record is run.json: everything about a run, written temp file + rename on each change.
type Record struct {
	ID        string   `json:"id"`
	Rule      string   `json:"rule"`
	Space     string   `json:"space"`    // the rule's space path
	SpaceID   string   `json:"space_id"` // its id: the scope of the run's events
	SpaceDir  string   `json:"space_dir"`
	Event     string   `json:"event"` // the trigger event's id
	EventType string   `json:"event_type"`
	Subject   string   `json:"subject,omitempty"`
	Actor     string   `json:"actor,omitempty"` // the trigger event's actor
	Role      string   `json:"role"`
	Address   string   `json:"address"` // <space path>/<role>
	Harness   string   `json:"harness"`
	Model     string   `json:"model,omitempty"`
	BudgetUSD float64  `json:"budget_usd,omitempty"`
	Args      []string `json:"args,omitempty"`

	Job     string   `json:"job,omitempty"` // empty: job: none
	Repos   []string `json:"repos,omitempty"`
	JobMade bool     `json:"job_made,omitempty"` // this run made the job (a retry reuses it)
	Cwd     string   `json:"cwd,omitempty"`
	Read    []string `json:"read,omitempty"`  // access: read paths
	Write   []string `json:"write,omitempty"` // access: write paths

	Concurrency int    `json:"concurrency"`
	Queue       int    `json:"queue"` // the role's queue: how many may wait (02d: when a held run may enter)
	Timeout     string `json:"timeout"`
	Retries     int    `json:"retries"`
	Attempt     int    `json:"attempt"`
	// the rule's rate: starts per window (02d: when a held run may enter the queue)
	RateN      int    `json:"rate_n,omitempty"`
	RateWindow string `json:"rate_window,omitempty"`
	// Stale is the rule's stale: a run that waited past it is rejected when it would start
	// (EventTime is the trigger's time); a redispatched one is exempt
	Stale     string `json:"stale,omitempty"`
	EventTime string `json:"event_time,omitempty"`

	Argv    []string `json:"argv,omitempty"`
	Pid     int      `json:"pid,omitempty"`
	Pgid    int      `json:"pgid,omitempty"`
	PsStart string   `json:"ps_start,omitempty"` // the process's start time as ps says it (pid reuse)

	State string `json:"state"`
	// Held: a submitted run waiting for its rule's rate or its role's queue (rate, queue), since
	// HeldSince; Queued: when it entered its role's queue (what rate: counts)
	Held      string `json:"held,omitempty"`
	HeldSince string `json:"held_since,omitempty"`
	Queued    string `json:"queued,omitempty"`
	// RedispatchBy: the run was asked for by anfs agents redispatch (stale, rate and loop skipped)
	RedispatchBy string `json:"redispatch_by,omitempty"`

	Reason    string         `json:"reason,omitempty"` // rejected: stale, rate, queue, loop, invalid; failed: exit, timeout, job, spawn, orphaned
	Detail    string         `json:"detail,omitempty"`
	Exit      *int           `json:"exit,omitempty"`
	By        string         `json:"by,omitempty"` // who canceled it
	Submitted string         `json:"submitted"`
	Started   string         `json:"started,omitempty"`
	Ended     string         `json:"ended,omitempty"`
	Summary   string         `json:"summary,omitempty"`
	Commits   map[string]int `json:"commits,omitempty"`

	// what the run wrote into its spool (02d): lines moved into the log, lines refused
	EventsIngested int `json:"events_ingested,omitempty"`
	EventsRefused  int `json:"events_refused,omitempty"`
}

// Last8 is the run id's last 8 characters: ANFS_ACTOR's part, and what `anfs agents` accepts.
func (r *Record) Last8() string { return r.ID[len(r.ID)-8:] }

// TimeoutDur is the record's timeout.
func (r *Record) TimeoutDur() time.Duration {
	d, err := time.ParseDuration(r.Timeout)
	if err != nil || d <= 0 {
		return time.Hour
	}
	return d
}

// RateWin is the rule's rate window (0: no rate recorded, a record from before 02d).
func (r *Record) RateWin() time.Duration {
	d, _ := time.ParseDuration(r.RateWindow)
	return d
}

// StaleDur is the rule's stale (0: none recorded).
func (r *Record) StaleDur() time.Duration {
	d, _ := time.ParseDuration(r.Stale)
	return d
}

// Store is the runs directory.
type Store struct{ Dir string }

// RunDir is one run's directory.
func (s Store) RunDir(id string) string { return filepath.Join(s.Dir, id) }

// Save writes run.json (temp file, rename).
func (s Store) Save(r *Record) error {
	d := s.RunDir(r.ID)
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(d, ".run.json.")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(d, "run.json"))
}

// Load reads one run.json.
func (s Store) Load(id string) (*Record, error) {
	b, err := os.ReadFile(filepath.Join(s.RunDir(id), "run.json"))
	if err != nil {
		return nil, err
	}
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("%s/run.json: %v", id, err)
	}
	return &r, nil
}

// All reads every record, oldest first (ids are UUIDv7: they sort by time). A directory without a
// readable run.json is skipped.
func (s Store) All() ([]*Record, error) {
	ents, err := os.ReadDir(s.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*Record
	for _, e := range ents {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if r, err := s.Load(e.Name()); err == nil {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Canceled reads the cancel marker `anfs agents stop` writes: who, or "" with false when none.
func (s Store) Canceled(id string) (string, bool) {
	b, err := os.ReadFile(filepath.Join(s.RunDir(id), "cancel"))
	if err != nil {
		return "", false
	}
	by := strings.TrimSpace(string(b))
	if by == "" {
		by = "unknown"
	}
	return by, true
}

// ExitStatus reads the exit status the run's wrapper left, if it ended on its own.
func (s Store) ExitStatus(id string) (int, bool) {
	b, err := os.ReadFile(filepath.Join(s.RunDir(id), "exit"))
	if err != nil {
		return 0, false
	}
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

func now() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }
