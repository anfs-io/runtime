// Package cloudevent is anfs's CloudEvents profile, version 1 (anfsv 1), in Go: parse and validate
// one line of the event log, and write one. The language-neutral profile is anfs-io/docs,
// units/agents/01-events/plan.md; the bash writer is core's lib/anfs/events.sh (anfs_emit).
//
// A line is one CloudEvent 1.0 in the structured JSON format, at most 4096 bytes with its newline:
// specversion "1.0", id a UUIDv7, source /anfs/spaces/<space id> or /anfs/host, type
// io.anfs.<noun>.<verb>, optional subject, time (UTC, milliseconds, Z), datacontenttype
// application/json, the extensions actor (always), actorspace, actorrole, spacepath, anfsv 1, and
// data, an object of string values under ^[A-Za-z_][A-Za-z0-9_]*$ keys. An optional attribute with
// no value is left out. Readers ignore attributes they do not know.
package cloudevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Max is the most bytes a line may have, its newline included.
const Max = 4096

// Version is the profile version this code knows (anfsv).
const Version = 1

var (
	typeRE  = regexp.MustCompile(`^io\.anfs\.[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+$`)
	shortRE = regexp.MustCompile(`^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+$`)
	spaceRE = regexp.MustCompile(`^[a-z0-9]{10}$`)
	keyRE   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	roleRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	timeRE  = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$`)
)

// Event is what a reader needs of a line: the attributes, and the scope its source names.
type Event struct {
	ID         string
	Source     string
	Type       string // full: io.anfs.report.filed
	Subject    string
	Time       string
	Actor      string
	ActorSpace string
	ActorRole  string
	SpacePath  string
	Data       map[string]string
	Scope      string // the space id, or "host"
}

// Short is the type without io.anfs.: report.filed.
func (e Event) Short() string { return strings.TrimPrefix(e.Type, "io.anfs.") }

// NatsSubject is the NATS subject the event is published on: anfs.<scope>.<short type>.
func (e Event) NatsSubject() string { return "anfs." + e.Scope + "." + e.Short() }

// Parse validates one line (without or with its newline) against the profile.
func Parse(line []byte) (Event, error) {
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	if len(line)+1 > Max {
		return Event{}, fmt.Errorf("%d bytes, over %d", len(line)+1, Max)
	}
	if bytes.ContainsAny(line, "\n\r") {
		return Event{}, errors.New("not one line")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return Event{}, fmt.Errorf("not a JSON object: %w", err)
	}
	str := func(k string, required bool) (string, error) {
		v, ok := raw[k]
		if !ok {
			if required {
				return "", fmt.Errorf("no %s", k)
			}
			return "", nil
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return "", fmt.Errorf("%s is not a string", k)
		}
		if required && s == "" {
			return "", fmt.Errorf("%s is empty", k)
		}
		return s, nil
	}
	var e Event
	var err error
	sv, err := str("specversion", true)
	if err != nil {
		return e, err
	}
	if sv != "1.0" {
		return e, fmt.Errorf("specversion %q, not 1.0", sv)
	}
	av, ok := raw["anfsv"]
	if !ok {
		return e, errors.New("no anfsv")
	}
	var anfsv int
	if err := json.Unmarshal(av, &anfsv); err != nil || anfsv != Version {
		return e, fmt.Errorf("anfsv %s, not %d", string(av), Version)
	}
	if e.ID, err = str("id", true); err != nil {
		return e, err
	}
	u, err := uuid.Parse(e.ID)
	if err != nil || u.Version() != 7 || u.Variant() != uuid.RFC4122 || u.String() != e.ID {
		return e, fmt.Errorf("id %q is not a UUIDv7", e.ID)
	}
	if e.Source, err = str("source", true); err != nil {
		return e, err
	}
	switch {
	case e.Source == "/anfs/host":
		e.Scope = "host"
	case strings.HasPrefix(e.Source, "/anfs/spaces/") && spaceRE.MatchString(strings.TrimPrefix(e.Source, "/anfs/spaces/")):
		e.Scope = strings.TrimPrefix(e.Source, "/anfs/spaces/")
	default:
		return e, fmt.Errorf("source %q is not /anfs/spaces/<id> or /anfs/host", e.Source)
	}
	if e.Type, err = str("type", true); err != nil {
		return e, err
	}
	if !typeRE.MatchString(e.Type) {
		return e, fmt.Errorf("type %q is not io.anfs.<noun>.<verb>", e.Type)
	}
	if e.Subject, err = str("subject", false); err != nil {
		return e, err
	}
	if e.Time, err = str("time", false); err != nil {
		return e, err
	}
	if e.Time != "" && !timeRE.MatchString(e.Time) {
		return e, fmt.Errorf("time %q is not UTC RFC 3339", e.Time)
	}
	if dct, err := str("datacontenttype", false); err != nil {
		return e, err
	} else if dct != "" && dct != "application/json" {
		return e, fmt.Errorf("datacontenttype %q", dct)
	}
	if e.Actor, err = str("actor", true); err != nil {
		return e, err
	}
	if e.ActorSpace, err = str("actorspace", false); err != nil {
		return e, err
	}
	if e.ActorRole, err = str("actorrole", false); err != nil {
		return e, err
	}
	if e.SpacePath, err = str("spacepath", false); err != nil {
		return e, err
	}
	e.Data = map[string]string{}
	if d, ok := raw["data"]; ok {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(d, &obj); err != nil || obj == nil {
			return e, errors.New("data is not an object")
		}
		for k, v := range obj {
			if !keyRE.MatchString(k) {
				return e, fmt.Errorf("data key %q", k)
			}
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return e, fmt.Errorf("data.%s is not a string", k)
			}
			e.Data[k] = s
		}
	}
	return e, nil
}

// New is an event to write: the short type, its subject and data, who; Write fills the rest.
type New struct {
	Scope      string // a space id, or "" / "host" for the host scope
	Type       string // short: run.completed
	Subject    string
	Actor      string
	ActorSpace string
	ActorRole  string
	SpacePath  string
	Data       map[string]string
}

// Line builds the line (with its newline) for n at time now: a fresh UUIDv7 whose milliseconds
// are the time attribute's. The attributes are in the bash writer's order, data last.
func Line(n New, now time.Time) ([]byte, Event, error) {
	if !shortRE.MatchString(n.Type) {
		return nil, Event{}, fmt.Errorf("type %q is not <noun>.<verb>", n.Type)
	}
	scope := n.Scope
	if scope == "" {
		scope = "host"
	}
	source := "/anfs/host"
	if scope != "host" {
		if !spaceRE.MatchString(scope) {
			return nil, Event{}, fmt.Errorf("space id %q", scope)
		}
		source = "/anfs/spaces/" + scope
	}
	if n.Actor == "" {
		return nil, Event{}, errors.New("no actor")
	}
	if n.ActorRole != "" && !roleRE.MatchString(n.ActorRole) {
		return nil, Event{}, fmt.Errorf("role %q", n.ActorRole)
	}
	if n.ActorSpace != "" && !spaceRE.MatchString(n.ActorSpace) {
		return nil, Event{}, fmt.Errorf("actor space %q", n.ActorSpace)
	}
	id, err := newV7(now)
	if err != nil {
		return nil, Event{}, err
	}
	ms := idMillis(id)
	t := time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
	var b bytes.Buffer
	q := func(s string) {
		j, _ := json.Marshal(s)
		b.Write(j)
	}
	b.WriteString(`{"specversion":"1.0","id":`)
	q(id.String())
	b.WriteString(`,"source":`)
	q(source)
	b.WriteString(`,"type":`)
	q("io.anfs." + n.Type)
	if n.Subject != "" {
		b.WriteString(`,"subject":`)
		q(n.Subject)
	}
	b.WriteString(`,"time":`)
	q(t)
	b.WriteString(`,"datacontenttype":"application/json","actor":`)
	q(n.Actor)
	if n.ActorSpace != "" {
		b.WriteString(`,"actorspace":`)
		q(n.ActorSpace)
	}
	if n.ActorRole != "" {
		b.WriteString(`,"actorrole":`)
		q(n.ActorRole)
	}
	if n.SpacePath != "" {
		b.WriteString(`,"spacepath":`)
		q(n.SpacePath)
	}
	b.WriteString(`,"anfsv":1,"data":{`)
	keys := make([]string, 0, len(n.Data))
	for k := range n.Data {
		if !keyRE.MatchString(k) {
			return nil, Event{}, fmt.Errorf("data key %q", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		q(k)
		b.WriteByte(':')
		q(n.Data[k])
	}
	b.WriteString("}}\n")
	if b.Len() > Max {
		return nil, Event{}, fmt.Errorf("%d bytes, over %d: keep data small", b.Len(), Max)
	}
	ev, err := Parse(b.Bytes())
	if err != nil {
		return nil, Event{}, fmt.Errorf("built a line the profile refuses: %w", err)
	}
	return b.Bytes(), ev, nil
}

// Append writes n into <events>/<scope>/<YYYY-MM>.jsonl (the UTC month of its time) in one write
// with O_APPEND, so lines from other writers never interleave with it.
func Append(events string, n New) (Event, error) {
	line, ev, err := Line(n, time.Now())
	if err != nil {
		return Event{}, err
	}
	dir := filepath.Join(events, ev.Scope)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Event{}, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ev.Time[:7]+".jsonl"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return Event{}, err
	}
	defer f.Close()
	if _, err := f.Write(line); err != nil {
		return Event{}, err
	}
	return ev, f.Close()
}

// UUIDv7s from one process sort in the order they were made (uuid's monotonic counter); the clock
// is the caller's so a test can pin it.
func newV7(now time.Time) (uuid.UUID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return u, err
	}
	// uuid.NewV7 takes its own clock; when the caller's differs (a test), put its milliseconds in
	ms := now.UnixMilli()
	if d := idMillis(u) - ms; d > 1 || d < -1 {
		u[0], u[1], u[2], u[3], u[4], u[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	}
	return u, nil
}

func idMillis(u uuid.UUID) int64 {
	return int64(u[0])<<40 | int64(u[1])<<32 | int64(u[2])<<24 | int64(u[3])<<16 | int64(u[4])<<8 | int64(u[5])
}
