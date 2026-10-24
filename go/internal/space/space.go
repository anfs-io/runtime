// Package space reads what a space's space.yml says about its agents: access: (what a dispatched
// agent may reach beyond its job), agents: (the roles the space runs, each the address
// <space path>/<role>) and dispatch: (which event starts which role). One parser, used by anfsd's
// dispatcher and by `anfsd rules` (core's `anfs agents rules` reads its --json), so what is
// refused is refused the same way everywhere.
//
// A rule that fails validation is invalid, with the reason, and is not loaded; the space's other
// rules load. A rule with `enabled: false` is valid and not loaded (status disabled): anfsd shows
// it, `anfsd match` says what it would do, and nothing runs.
package space

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"go.yaml.in/yaml/v3"
)

// Harnesses are the agent CLIs a role may run, and the program each is.
var Harnesses = map[string]string{
	"claude":      "claude",
	"opencode":    "opencode",
	"pi":          "pi",
	"antigravity": "agy",
}

// RunTypes are anfsd's own events; a rule on them is refused (a run would start runs).
var RunTypes = []string{"run.submitted", "run.working", "run.completed", "run.failed", "run.canceled", "run.rejected"}

// Statuses of a rule.
const (
	StatusOK         = "ok"
	StatusInvalid    = "invalid"
	StatusDisabled   = "disabled"
	StatusUnenforced = "unenforced" // pi: no permission system
	StatusPartial    = "partial"    // opencode, antigravity: no read/write split
	StatusOff        = "off"        // valid, but the space's dispatcher: is none (02d)
)

// Dispatchers a space's dispatcher: may name (02d): anfsd (the default) or none, which leaves the
// space's rules unloaded for another tool to act on. When the tools: slots arrive it moves there.
const (
	DispatcherAnfsd = "anfsd"
	DispatcherNone  = "none"
)

// Defaults and limits of a rule.
const (
	DefaultTimeout = time.Hour
	MaxTimeout     = 12 * time.Hour
	DefaultStale   = 24 * time.Hour
	DefaultRate    = 20 // starts per hour
	MaxRetries     = 2
	DefaultQueue   = 10
	DefaultHold    = 20 // runs of a rule held at once (02d)
	MaxHold        = 200
)

// Overflow is what a full queue or a spent rate does to a run (02d): delay holds it (submitted,
// held), reject rejects it.
const (
	OverflowDelay  = "delay"
	OverflowReject = "reject"
)

// Access is one path a space's dispatched agents may reach: absolute, cleaned, mode read or write.
type Access struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
}

// Role is one of agents:.
type Role struct {
	Name        string  `json:"name"`
	Harness     string  `json:"harness"`
	Model       string  `json:"model,omitempty"`
	Concurrency int     `json:"concurrency"`
	Queue       int     `json:"queue"`
	BudgetUSD   float64 `json:"budget_usd,omitempty"`
	err         error
}

// On is what an event must be for a rule to act on it.
type On struct {
	Type    string            `json:"type"`
	Subject string            `json:"subject,omitempty"`
	Data    map[string]string `json:"data,omitempty"`
	Space   string            `json:"space,omitempty"` // a space spec; empty: the rule's own space
}

// Job is the anfs work job a run gets; nil is job: none (the space's directory, read-only).
type Job struct {
	Name  string   `json:"name"`
	Repos []string `json:"repos"`
}

// Rate is so many starts per window.
type Rate struct {
	N      int
	Window time.Duration
}

func (r Rate) String() string {
	switch r.Window {
	case time.Second:
		return fmt.Sprintf("%d/s", r.N)
	case time.Minute:
		return fmt.Sprintf("%d/m", r.N)
	case 24 * time.Hour:
		return fmt.Sprintf("%d/d", r.N)
	}
	return fmt.Sprintf("%d/h", r.N)
}

// Rule is one of dispatch:.
type Rule struct {
	Name       string
	Enabled    bool
	On         On
	As         string
	Harness    string // the role's, or the rule's override
	Model      string
	BudgetUSD  float64
	Job        *Job
	Prompt     string // the template: prompt:, or prompt_file:'s text
	PromptFile string // absolute, when the prompt is a file
	Args       []string
	Timeout    time.Duration
	Rate       Rate
	Stale      time.Duration
	Retries    int
	Overflow   string // delay (default) or reject
	Hold       int    // most runs of this rule held at once

	Space    *Space
	Role     *Role
	Status   string
	Reason   string   // why it is invalid
	Warnings []string // said at load, not fatal
}

// Key is the rule's identity across spaces: <space id>/<name>.
func (r *Rule) Key() string { return r.Space.ID + "/" + r.Name }

// Loaded says whether the dispatcher acts on it.
func (r *Rule) Loaded() bool {
	return r.Status == StatusOK || r.Status == StatusUnenforced || r.Status == StatusPartial
}

// Address is who a run of the rule is: <space path>/<role>.
func (r *Rule) Address() string { return r.Space.Path + "/" + r.As }

// Space is one space.yml's agents.
type Space struct {
	ID     string
	Path   string // wsm's path (anfs-dev, lgat/tech/web)
	Dir    string // the space's directory (installed)
	File   string
	Access []Access
	Roles  map[string]*Role
	Rules  []*Rule
	Err    error // the file could not be read or parsed: no rule loads
	// Dispatcher is who acts on the space's rules: anfsd, or none (its rules are shown, off)
	Dispatcher string
}

// Env is what validation needs of the host: HOME, the environment, and which programs exist.
type Env struct {
	Home     string
	Getenv   func(string) string
	LookPath func(string) (string, error)
}

func (e Env) getenv(k string) string {
	if e.Getenv == nil {
		return os.Getenv(k)
	}
	return e.Getenv(k)
}

func (e Env) lookPath(p string) error {
	if e.LookPath == nil {
		return errors.New("no PATH lookup")
	}
	_, err := e.LookPath(p)
	return err
}

var (
	idRE       = regexp.MustCompile(`^[a-z0-9]{10}$`)
	ruleNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	roleNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	rateRE     = regexp.MustCompile(`^([0-9]+)/(s|m|h|d)$`)
	repoRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// skipFlags are harness options that turn permissions off, or override what anfsd hands the
// harness for access: refused in a rule's args:.
var skipFlags = []string{
	"--dangerously-skip-permissions", "--allow-dangerously-skip-permissions", "--auto", "--yolo",
	"bypassPermissions", "--permission-mode", "--permission-prompts", "--settings", "--setting-sources",
	"--add-dir", "--sandbox", "--no-sandbox", "--dir", "--tools", "--no-tools", "--allowedTools",
	"--allowed-tools", "--disallowedTools", "--disallowed-tools",
	// what a dispatched harness loads (02d): no more MCP servers or plugins than anfsd gives
	"--mcp-config", "--strict-mcp-config", "--plugin-dir", "--plugin-dir-no-mcp", "--bare", "--safe-mode",
	"--extension", "--approve",
}

// Load reads one space.yml. path is the space's wsm path and dir its directory (both for display
// and for relative paths); a file with no id: or an unreadable one is a Space with Err set.
func Load(file, spacePath, dir string, env Env) *Space {
	s := &Space{Path: spacePath, Dir: dir, File: file, Roles: map[string]*Role{}}
	b, err := os.ReadFile(file)
	if err != nil {
		s.Err = err
		return s
	}
	return parse(s, b, env)
}

// Parse is Load from bytes (tests).
func Parse(b []byte, spacePath, dir string, env Env) *Space {
	return parse(&Space{Path: spacePath, Dir: dir, Roles: map[string]*Role{}}, b, env)
}

func parse(s *Space, b []byte, env Env) *Space {
	// every document's keys (anfs-dev's space.yml has its id before a `---`; wsm reads the
	// stream the same way with yq)
	m := map[string]*yaml.Node{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	for {
		var d yaml.Node
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.Err = fmt.Errorf("not YAML: %v", err)
			return s
		}
		if len(d.Content) == 0 || d.Content[0].Kind != yaml.MappingNode {
			continue
		}
		c := d.Content[0].Content
		for i := 0; i+1 < len(c); i += 2 {
			m[c[i].Value] = c[i+1]
		}
	}
	if n := m["id"]; n != nil {
		s.ID = n.Value
	}
	if !idRE.MatchString(s.ID) {
		s.Err = errors.New("no id: (wsm id --new)")
		if s.ID != "" {
			s.Err = fmt.Errorf("id %q is not 10 of a-z0-9", s.ID)
		}
	}
	s.Dispatcher = DispatcherAnfsd
	var dispErr error
	if n := m["dispatcher"]; n != nil {
		switch {
		case n.Kind == yaml.ScalarNode && (n.Value == DispatcherAnfsd || n.Value == DispatcherNone):
			s.Dispatcher = n.Value
		default:
			dispErr = fmt.Errorf("dispatcher: %q is anfsd or none (another tool's name waits for the tools: slots; until then, none)", n.Value)
		}
	}
	accessErr := s.parseAccess(m["access"], env)
	if accessErr == nil {
		accessErr = dispErr
	}
	rolesErr := s.parseRoles(m["agents"], env)
	s.parseRules(m["dispatch"], env, accessErr, rolesErr)
	return s
}

// expand: ~ and $VAR / ${VAR} from the environment (an unset one is an error), a relative path
// from the space's directory, then cleaned
func (s *Space) expand(p string, env Env) (string, error) {
	if p == "" {
		return "", errors.New("an empty path")
	}
	if p == "~" || strings.HasPrefix(p, "~/") {
		p = env.Home + p[1:]
	}
	var unset string
	p = os.Expand(p, func(k string) string {
		v := env.getenv(k)
		if k == "HOME" && v == "" {
			v = env.Home
		}
		if v == "" && unset == "" {
			unset = k
		}
		return v
	})
	if unset != "" {
		return "", fmt.Errorf("$%s is not set", unset)
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(s.Dir, p)
	}
	return filepath.Clean(p), nil
}

func (s *Space) parseAccess(n *yaml.Node, env Env) error {
	if n == nil {
		return nil
	}
	if n.Kind != yaml.SequenceNode {
		return errors.New("access: is a list of {path, mode}")
	}
	home := filepath.Clean(env.Home)
	for i, e := range n.Content {
		var a struct {
			Path string `yaml:"path"`
			Mode string `yaml:"mode"`
		}
		if err := strictDecode(e, &a); err != nil {
			return fmt.Errorf("access[%d]: %v", i, err)
		}
		p, err := s.expand(a.Path, env)
		if err != nil {
			return fmt.Errorf("access[%d] %s: %v", i, a.Path, err)
		}
		if p == "/" || p == home {
			return fmt.Errorf("access[%d] %s: not / or $HOME itself", i, a.Path)
		}
		if a.Mode != "read" && a.Mode != "write" {
			return fmt.Errorf("access[%d] %s: mode is read or write", i, a.Path)
		}
		s.Access = append(s.Access, Access{Path: p, Mode: a.Mode})
	}
	return nil
}

func (s *Space) parseRoles(n *yaml.Node, env Env) error {
	if n == nil {
		return nil
	}
	if n.Kind != yaml.MappingNode {
		return errors.New("agents: is a map of roles")
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		name := n.Content[i].Value
		var raw struct {
			Harness     string   `yaml:"harness"`
			Model       string   `yaml:"model"`
			Concurrency *int     `yaml:"concurrency"`
			Queue       *int     `yaml:"queue"`
			BudgetUSD   *float64 `yaml:"budget_usd"`
		}
		r := &Role{Name: name, Concurrency: 1, Queue: DefaultQueue}
		s.Roles[name] = r
		if !roleNameRE.MatchString(name) {
			r.err = fmt.Errorf("role %q: a-z, 0-9, ., _ and -", name)
			continue
		}
		if err := strictDecode(n.Content[i+1], &raw); err != nil {
			r.err = fmt.Errorf("agents.%s: %v", name, err)
			continue
		}
		r.Harness, r.Model = raw.Harness, raw.Model
		if raw.Concurrency != nil {
			r.Concurrency = *raw.Concurrency
		}
		if raw.Queue != nil {
			r.Queue = *raw.Queue
		}
		if raw.BudgetUSD != nil {
			r.BudgetUSD = *raw.BudgetUSD
		}
		switch {
		case r.Harness == "":
			r.err = fmt.Errorf("agents.%s: harness: is required (%s)", name, harnessList())
		case Harnesses[r.Harness] == "":
			r.err = fmt.Errorf("agents.%s: harness %q is not one of %s", name, r.Harness, harnessList())
		case r.Concurrency < 1:
			r.err = fmt.Errorf("agents.%s: concurrency is at least 1", name)
		case r.Queue < 0:
			r.err = fmt.Errorf("agents.%s: queue is 0 or more", name)
		case raw.BudgetUSD != nil && r.BudgetUSD <= 0:
			r.err = fmt.Errorf("agents.%s: budget_usd is a positive number", name)
		}
	}
	return nil
}

func harnessList() string {
	h := make([]string, 0, len(Harnesses))
	for k := range Harnesses {
		h = append(h, k)
	}
	sort.Strings(h)
	return strings.Join(h, ", ")
}

type rawRule struct {
	Name       string    `yaml:"name"`
	Enabled    *bool     `yaml:"enabled"`
	On         yaml.Node `yaml:"on"`
	As         string    `yaml:"as"`
	Harness    string    `yaml:"harness"`
	Job        yaml.Node `yaml:"job"`
	Prompt     *string   `yaml:"prompt"`
	PromptFile *string   `yaml:"prompt_file"`
	Args       []string  `yaml:"args"`
	Timeout    string    `yaml:"timeout"`
	Rate       string    `yaml:"rate"`
	Stale      string    `yaml:"stale"`
	Retries    *int      `yaml:"retries"`
	Overflow   string    `yaml:"overflow"`
	Hold       *int      `yaml:"hold"`
}

func (s *Space) parseRules(n *yaml.Node, env Env, accessErr, rolesErr error) {
	if n == nil {
		return
	}
	if n.Kind != yaml.SequenceNode {
		s.Rules = append(s.Rules, &Rule{Name: "dispatch", Space: s, Status: StatusInvalid, Reason: "dispatch: is a list of rules"})
		return
	}
	seen := map[string]bool{}
	for i, e := range n.Content {
		r := &Rule{Space: s, Enabled: true}
		s.Rules = append(s.Rules, r)
		err := s.parseRule(r, e, env)
		if err == nil && r.Name != "" && seen[r.Name] {
			err = fmt.Errorf("rule %s is named twice in this space", r.Name)
		}
		if r.Name == "" {
			r.Name = fmt.Sprintf("dispatch[%d]", i)
		}
		seen[r.Name] = true
		switch {
		case s.Err != nil:
			err = s.Err
		case err != nil:
		case accessErr != nil:
			err = accessErr
		case rolesErr != nil:
			err = rolesErr
		}
		if err != nil {
			r.Status, r.Reason = StatusInvalid, err.Error()
			continue
		}
		switch {
		case s.Dispatcher == DispatcherNone:
			r.Status, r.Reason = StatusOff, "dispatcher: none"
		case !r.Enabled:
			r.Status = StatusDisabled
		case r.Harness == "pi":
			r.Status = StatusUnenforced
		case r.Harness == "antigravity" || r.Harness == "opencode":
			r.Status = StatusPartial
		default:
			r.Status = StatusOK
		}
	}
}

func (s *Space) parseRule(r *Rule, n *yaml.Node, env Env) error {
	var raw rawRule
	if err := strictDecode(n, &raw); err != nil {
		return err
	}
	r.Name = raw.Name
	if raw.Name == "" {
		return errors.New("name: is required")
	}
	if !ruleNameRE.MatchString(raw.Name) {
		return fmt.Errorf("name %q: a-z, 0-9 and -", raw.Name)
	}
	if raw.Enabled != nil {
		r.Enabled = *raw.Enabled
	}
	// on:
	if raw.On.Kind == 0 {
		return errors.New("on: is required (on: {type: report.filed})")
	}
	var on struct {
		Type    string            `yaml:"type"`
		Subject string            `yaml:"subject"`
		Data    map[string]string `yaml:"data"`
		Space   string            `yaml:"space"`
	}
	if err := strictDecode(&raw.On, &on); err != nil {
		return fmt.Errorf("on: %v", err)
	}
	// the short type (anfs-io/events SPEC.md §2): io.anfs.report.* and anfs.report.* are
	// report.*, io.cnfs.x.* is cnfs.x.*
	on.Type = cloudevent.NormalShort(on.Type)
	if on.Type == "" {
		return errors.New("on.type: is required (a glob on the short type: report.filed, report.*)")
	}
	for _, g := range append([]string{on.Type, on.Subject}, mapValues(on.Data)...) {
		if _, err := path.Match(g, ""); err != nil {
			return fmt.Errorf("on: %q is not a glob", g)
		}
	}
	for _, t := range RunTypes {
		if ok, _ := path.Match(on.Type, t); ok {
			return fmt.Errorf("on.type %q matches anfsd's own %s: a run would start runs", on.Type, t)
		}
	}
	r.On = On{Type: on.Type, Subject: on.Subject, Data: on.Data, Space: strings.TrimSpace(on.Space)}
	// as:, harness:
	if raw.As == "" {
		return errors.New("as: is required (a role of agents:)")
	}
	role := s.Roles[raw.As]
	if role == nil {
		return fmt.Errorf("as: %s is not a role of agents:", raw.As)
	}
	if role.err != nil {
		return role.err
	}
	r.As, r.Role = raw.As, role
	r.Harness, r.Model, r.BudgetUSD = role.Harness, role.Model, role.BudgetUSD
	if raw.Harness != "" {
		if Harnesses[raw.Harness] == "" {
			return fmt.Errorf("harness %q is not one of %s", raw.Harness, harnessList())
		}
		r.Harness = raw.Harness
	}
	if err := env.lookPath(Harnesses[r.Harness]); err != nil {
		return fmt.Errorf("harness %s: %s is not on anfsd's PATH", r.Harness, Harnesses[r.Harness])
	}
	if r.BudgetUSD > 0 && r.Harness != "claude" {
		r.Warnings = append(r.Warnings, fmt.Sprintf("budget_usd is claude's only (--max-budget-usd); %s has none", r.Harness))
	}
	for _, a := range raw.Args {
		for _, f := range skipFlags {
			if a == f || strings.HasPrefix(a, f+"=") || strings.Contains(a, "dangerously") || a == "bypassPermissions" {
				return fmt.Errorf("args: %s is refused: anfsd never skips permissions, and hands the harness its access itself", a)
			}
		}
	}
	r.Args = raw.Args
	// job:
	switch raw.Job.Kind {
	case 0:
	case yaml.ScalarNode:
		if raw.Job.Value != "none" {
			return fmt.Errorf("job: is {name, repos} or none, not %q", raw.Job.Value)
		}
	default:
		var j Job
		if err := strictDecode(&raw.Job, &j); err != nil {
			return fmt.Errorf("job: %v", err)
		}
		if j.Name == "" || len(j.Repos) == 0 {
			return errors.New("job: needs a name and repos (job: {name: \"fix-{{subject}}\", repos: [core]})")
		}
		for _, repo := range j.Repos {
			if !repoRE.MatchString(repo) {
				return fmt.Errorf("job.repos: %q is not a repo's name", repo)
			}
		}
		if err := checkTemplate(j.Name, "job.name", false); err != nil {
			return err
		}
		if err := env.lookPath("anfs-work"); err != nil {
			return errors.New("job: needs anfs work, which is not installed (ppm install anfs-dev/dev); or job: none")
		}
		r.Job = &j
	}
	// prompt:
	switch {
	case raw.Prompt != nil && raw.PromptFile != nil:
		return errors.New("prompt: and prompt_file: both given: exactly one")
	case raw.Prompt != nil:
		r.Prompt = *raw.Prompt
	case raw.PromptFile != nil:
		p, err := s.expand(*raw.PromptFile, env)
		if err != nil {
			return fmt.Errorf("prompt_file: %v", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("prompt_file %s: %v", *raw.PromptFile, unwrapPath(err))
		}
		r.PromptFile, r.Prompt = p, string(b)
	default:
		return errors.New("prompt: or prompt_file: is required")
	}
	if strings.TrimSpace(r.Prompt) == "" {
		return errors.New("the prompt is empty")
	}
	if err := checkTemplate(r.Prompt, "the prompt", true); err != nil {
		return err
	}
	// timeout, rate, stale, retries
	var err error
	if r.Timeout, err = durationOr(raw.Timeout, DefaultTimeout); err != nil {
		return fmt.Errorf("timeout: %v", err)
	}
	if r.Timeout <= 0 || r.Timeout > MaxTimeout {
		return fmt.Errorf("timeout: %s is not within 12h", raw.Timeout)
	}
	if r.Stale, err = durationOr(raw.Stale, DefaultStale); err != nil || r.Stale <= 0 {
		return fmt.Errorf("stale: %q is a duration (24h)", raw.Stale)
	}
	r.Rate = Rate{N: DefaultRate, Window: time.Hour}
	if raw.Rate != "" {
		m := rateRE.FindStringSubmatch(raw.Rate)
		if m == nil {
			return fmt.Errorf("rate: %q is starts per window (20/h, 5/m, 100/d)", raw.Rate)
		}
		r.Rate.N, _ = strconv.Atoi(m[1])
		r.Rate.Window = map[string]time.Duration{"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[m[2]]
		if r.Rate.N < 1 {
			return fmt.Errorf("rate: %q starts nothing", raw.Rate)
		}
	}
	if raw.Retries != nil {
		r.Retries = *raw.Retries
	}
	if r.Retries < 0 || r.Retries > MaxRetries {
		return fmt.Errorf("retries: %d is 0 to %d", r.Retries, MaxRetries)
	}
	switch raw.Overflow {
	case "", OverflowDelay:
		r.Overflow = OverflowDelay
	case OverflowReject:
		r.Overflow = OverflowReject
	default:
		return fmt.Errorf("overflow: %q is delay or reject", raw.Overflow)
	}
	r.Hold = DefaultHold
	if raw.Hold != nil {
		r.Hold = *raw.Hold
	}
	if r.Hold < 0 || r.Hold > MaxHold {
		return fmt.Errorf("hold: %d is 0 to %d", r.Hold, MaxHold)
	}
	return nil
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func unwrapPath(err error) error {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

func durationOr(v string, def time.Duration) (time.Duration, error) {
	if v == "" {
		return def, nil
	}
	if m := regexp.MustCompile(`^([0-9]+)d$`).FindStringSubmatch(v); m != nil {
		n, _ := strconv.Atoi(m[1])
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%q is a duration (2h, 30m, 1d)", v)
	}
	return d, nil
}

// strictDecode decodes a node refusing keys the struct does not have.
func strictDecode(n *yaml.Node, v any) error {
	if n.Kind == yaml.MappingNode {
		known := yamlKeys(v)
		for i := 0; i+1 < len(n.Content); i += 2 {
			if !known[n.Content[i].Value] {
				return fmt.Errorf("unknown key %s", n.Content[i].Value)
			}
		}
	} else if n.Kind != 0 {
		if _, ok := v.(*Job); ok || isStruct(v) {
			return fmt.Errorf("a map, not %q", n.Value)
		}
	}
	if err := n.Decode(v); err != nil {
		var te *yaml.TypeError
		if errors.As(err, &te) && len(te.Errors) > 0 {
			return errors.New(strings.TrimPrefix(te.Errors[0], "yaml: "))
		}
		return err
	}
	return nil
}
