package cloudevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
)

// New is an event anfsd writes: the short type, its subject and data, who; Line fills the rest.
// anfsd's own events are anfs's: source /anfs/spaces/<id> or /anfs/host, data flat strings.
type New struct {
	Scope      string // a space id, or "" / "host" for the host scope
	Type       string // short (run.completed; another namespace's with it: cnfs.x.y) or full
	Subject    string
	Actor      string
	ActorSpace string
	ActorRole  string
	SpacePath  string
	Data       map[string]string
	// Time is when the occurrence happened (podman's own time); zero is Line's now
	Time time.Time
}

// Line builds the line (with its newline) for n at time now (n.Time when set): a fresh UUIDv7
// whose milliseconds are the time attribute's. The attributes are in the bash writer's order,
// data last: the canonical form.
func Line(n New, now time.Time) ([]byte, Event, error) {
	typ := FullType(n.Type)
	if !typeRE.MatchString(typ) {
		return nil, Event{}, fmt.Errorf("type %q is not <noun>.<verb>", n.Type)
	}
	if !n.Time.IsZero() {
		now = n.Time
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
	q(typ)
	if n.Subject != "" {
		b.WriteString(`,"subject":`)
		q(n.Subject)
	}
	b.WriteString(`,"time":`)
	q(msTime(idMillis(id)))
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
	return ev, appendFile(events, ev.Scope, ev.Month()+".jsonl", line)
}

// AppendLine appends one line (a newline is added when it has none) to
// <events>/<scope>/<month>.jsonl in one write with O_APPEND, as Append does: anfsd's ingestion of
// a run's spool (agents/02d). month is YYYY-MM.
func AppendLine(events, scope, month string, line []byte) error {
	if !monthRE.MatchString(month) {
		return fmt.Errorf("month %q", month)
	}
	return appendFile(events, scope, month+".jsonl", line)
}

// AppendRecorded appends one line to <events>/<scope>/<month>.bus.jsonl, the recorder's file
// (SPEC.md §7.3): what came on the bus and is not in the spool. The shipper never reads it.
func AppendRecorded(events, scope, month string, line []byte) error {
	if !monthRE.MatchString(month) {
		return fmt.Errorf("month %q", month)
	}
	return appendFile(events, scope, month+".bus.jsonl", line)
}

func appendFile(events, scope, name string, line []byte) error {
	if scope != "host" && !spaceRE.MatchString(scope) {
		return fmt.Errorf("scope %q", scope)
	}
	if len(line) == 0 || line[len(line)-1] != '\n' {
		line = append(append([]byte(nil), line...), '\n')
	}
	if len(line) > Max {
		return fmt.Errorf("%d bytes, over %d", len(line), Max)
	}
	dir := filepath.Join(events, scope)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// UUIDv7s from one process sort in the order they were made (uuid's monotonic counter); the clock
// is the caller's so a test can pin it, and an event's own time (podman's) is its id's.
func newV7(now time.Time) (uuid.UUID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return u, err
	}
	// uuid.NewV7 takes its own clock; when the caller's differs, put its milliseconds in
	ms := now.UnixMilli()
	if d := idMillis(u) - ms; d > 1 || d < -1 {
		u[0], u[1], u[2], u[3], u[4], u[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	}
	return u, nil
}
