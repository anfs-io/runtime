// Package cloudevent is the anfs events profile, version 1 (anfsv 1), in Go: parse and validate
// one event as one line, read its subject on NATS, and write one. The profile is anfs-io/events
// (https://github.com/anfs-io/events: SPEC.md, REGISTRY.md, schema/event.v1.json and the
// conformance examples, vendored in testdata/spec with the commit they come from); core's
// lib/anfs/events.sh (anfs_emit) is the bash writer.
//
// An event is one CloudEvent 1.0 in the structured JSON format, at most 4096 bytes as one line
// with its newline: specversion "1.0", id a UUIDv7 whose milliseconds are time's, source
// /<layer>/spaces/<space id>[/...] or /<layer>/host[/...], type io.<namespace>.<segment>(.<segment>)+,
// optional subject, time (UTC, exactly milliseconds, Z), datacontenttype application/json, the
// extensions actor (always), actorclaimed, actorspace (only with actorrole), actorrole, actorrun,
// spacepath, traceparent, tracestate (only with traceparent), anfsv 1, and data, a JSON object
// (flat strings for io.anfs.*). No attribute is null; attribute strings hold no control
// characters; member names are unique; integers in data stay within 2^53 - 1. Readers ignore
// attributes they do not know; other extensions are a string, a 32-bit integer or a boolean.
//
// Parse returns an *Error naming the rule an invalid event breaks (SPEC.md's table of rule
// codes), so anfsd validate and the conformance tests speak the spec's language.
package cloudevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Max is the most bytes a line may have, its newline included (on the bus: the body is the line
// without it, so at most Max-1).
const Max = 4096

// Version is the profile version this code knows (anfsv).
const Version = 1

// Namespaces are the registered type namespaces (REGISTRY.md): what a short type's first segment
// is checked against. A type of another namespace is still valid (validators check the grammar);
// only its short type cannot be told from an anfs one.
var Namespaces = []string{"anfs", "cnfs", "lgat", "rws", "roteoh"}

var (
	idRE          = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	sourceRE      = regexp.MustCompile(`^/[a-z][a-z0-9-]*/(host|spaces/([a-z0-9]{10}))(/[A-Za-z0-9._~-]+)*$`)
	typeRE        = regexp.MustCompile(`^io\.[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+$`)
	timeRE        = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$`)
	actorRE       = regexp.MustCompile(`^[a-z][a-z0-9-]*(:[^\x00-\x20\x{7f}-\x{9f}]+)?$`)
	claimedRE     = regexp.MustCompile(`^[a-z][a-z0-9-]*(:[^\x00-\x20\x{7f}-\x{9f}]+)?( as ([a-z0-9]{10})?/[a-z0-9][a-z0-9._-]*)?$`)
	spaceRE       = regexp.MustCompile(`^[a-z0-9]{10}$`)
	roleRE        = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	keyRE         = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	extRE         = regexp.MustCompile(`^[a-z0-9]{1,20}$`)
	traceparentRE = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$`)
	tracestateRE  = regexp.MustCompile(`^[ -~]{1,512}$`)
	controlRE     = regexp.MustCompile(`[\x00-\x1f\x{7f}-\x{9f}]`)
	monthRE       = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}$`)
)

// Error is an event the profile refuses: the rule it breaks (SPEC.md, "Conformance") and why.
type Error struct {
	Rule string
	Msg  string
}

func (e *Error) Error() string { return e.Msg + " (" + e.Rule + ")" }

func fail(rule, f string, a ...any) error { return &Error{Rule: rule, Msg: fmt.Sprintf(f, a...)} }

// Rule is the rule an error from Parse names ("" for any other error).
func Rule(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Rule
	}
	return ""
}

// Event is what a reader needs of an event: the attributes, and the scope its source names.
type Event struct {
	ID         string
	Source     string
	Type       string // full: io.anfs.report.filed, io.cnfs.partitia.report.filed
	Subject    string
	Time       string
	Actor      string
	ActorSpace string
	ActorRole  string
	SpacePath  string
	// ActorRun is the run id anfsd stamped on a line a run wrote (agents/02d); ActorClaimed what
	// that line said of its actor when it differed from the stamp: a claim, never an identity
	ActorRun     string
	ActorClaimed string
	TraceParent  string
	TraceState   string
	// Data is data's top-level string values (all of it for io.anfs.*, whose data is flat
	// strings); RawData is data as written, any JSON object
	Data    map[string]string
	RawData json.RawMessage
	Scope   string // the space id in source, or "host"
}

// Namespace is the type's owner: anfs, cnfs, lgat.
func (e Event) Namespace() string {
	return strings.SplitN(strings.TrimPrefix(e.Type, "io."), ".", 2)[0]
}

// Short is the short type (SPEC.md §2): io.anfs. dropped from anfs's types (report.filed), io.
// from every other namespace's (cnfs.partitia.report.filed). What --type and on.type are.
func (e Event) Short() string { return ShortType(e.Type) }

// ShortType drops io.anfs. or io. from a full type (a short one is returned as it is).
func ShortType(t string) string {
	if s, ok := strings.CutPrefix(t, "io.anfs."); ok {
		return s
	}
	if s, ok := strings.CutPrefix(t, "io."); ok {
		return s
	}
	return t
}

// IsNamespace says whether n is a registered namespace.
func IsNamespace(n string) bool {
	for _, x := range Namespaces {
		if x == n {
			return true
		}
	}
	return false
}

// FullType is the full type of a short one (or a glob of one): io. before it when its first
// segment is a registered namespace (cnfs.partitia.report.filed, anfs.report.filed), io.anfs.
// otherwise (report.filed). A full type is returned as it is.
func FullType(short string) string {
	if strings.HasPrefix(short, "io.") {
		return short
	}
	if IsNamespace(strings.SplitN(short, ".", 2)[0]) {
		return "io." + short
	}
	return "io.anfs." + short
}

// NormalShort is a type as people type it (short or full, anfs's with or without its namespace)
// in its short form: io.anfs.report.* and anfs.report.* are report.*, io.cnfs.x.y is cnfs.x.y.
func NormalShort(t string) string { return ShortType(FullType(t)) }

// NatsSubject is the NATS subject the event is published on (SPEC.md §5): anfs.<scope>. and the
// type without io., one shape for every layer: anfs.<id>.anfs.report.filed,
// anfs.host.anfs.container.started, anfs.<id>.cnfs.partitia.report.filed.
func (e Event) NatsSubject() string {
	return "anfs." + e.Scope + "." + strings.TrimPrefix(e.Type, "io.")
}

// SubjectParts is NatsSubject's inverse: the scope and the full type a subject names; ok false
// when it is not one (the recorder checks a message's subject against its event with it).
func SubjectParts(subject string) (scope, typ string, ok bool) {
	t := strings.SplitN(subject, ".", 3)
	if len(t) != 3 || t[0] != "anfs" || (t[1] != "host" && !spaceRE.MatchString(t[1])) || !typeRE.MatchString("io."+t[2]) {
		return "", "", false
	}
	return t[1], "io." + t[2], true
}

// known attributes and the profile's extensions; anything else is an extension a reader ignores
var known = map[string]bool{"specversion": true, "id": true, "source": true, "type": true, "subject": true,
	"time": true, "datacontenttype": true, "dataschema": true, "data": true, "actor": true, "actorclaimed": true,
	"actorspace": true, "actorrole": true, "actorrun": true, "spacepath": true, "traceparent": true,
	"tracestate": true, "anfsv": true, "commitment": true}

// Parse validates one event as one line (without or with its newline) against the profile: the
// schema's rules and the line rules. The error is an *Error.
func Parse(line []byte) (Event, error) {
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	var e Event
	if len(line)+1 > Max {
		return e, fail("size.max", "%d bytes with the newline, over %d", len(line)+1, Max)
	}
	if bytes.ContainsAny(line, "\n\r") {
		return e, fail("line.one", "not one line")
	}
	if !utf8.Valid(line) {
		return e, fail("json.utf8", "not UTF-8")
	}
	if !json.Valid(line) {
		return e, fail("json.parse", "not JSON")
	}
	members, err := rawMembers(line)
	if err != nil {
		return e, err
	}
	raw := map[string]json.RawMessage{}
	for _, m := range members {
		if !known[m.key] && !extRE.MatchString(m.key) {
			return e, fail("ext.name", "%q is not an extension name (^[a-z0-9]{1,20}$)", m.key)
		}
		if m.key != "data" && string(m.val) == "null" {
			return e, fail("attr.null", "%s is null (leave it out)", m.key)
		}
		raw[m.key] = m.val
	}
	// a string attribute: absent is "", ok false; another JSON type is its rule; control
	// characters are attr.control
	str := func(k, rule string) (string, bool, error) {
		v, ok := raw[k]
		if !ok {
			return "", false, nil
		}
		var s string
		if v[0] != '"' || json.Unmarshal(v, &s) != nil {
			return "", true, fail(rule, "%s is not a string", k)
		}
		if controlRE.MatchString(s) {
			return "", true, fail("attr.control", "%s holds a control character", k)
		}
		return s, true, nil
	}
	need := func(k string) error {
		if _, ok := raw[k]; !ok {
			return fail(k+".required", "no %s", k)
		}
		return nil
	}
	for _, k := range []string{"specversion", "id", "source", "type", "time", "datacontenttype", "actor", "anfsv", "data"} {
		if err := need(k); err != nil {
			return e, err
		}
	}

	if sv, _, err := str("specversion", "specversion.value"); err != nil {
		return e, err
	} else if sv != "1.0" {
		return e, fail("specversion.value", "specversion %q, not 1.0", sv)
	}
	if string(raw["anfsv"]) != "1" {
		return e, fail("anfsv.value", "anfsv %s, not %d", raw["anfsv"], Version)
	}
	if e.ID, _, err = str("id", "id.uuidv7"); err != nil {
		return e, err
	}
	if !idRE.MatchString(e.ID) {
		return e, fail("id.uuidv7", "id %q is not a lower-case UUIDv7", e.ID)
	}
	if e.Source, _, err = str("source", "source.form"); err != nil {
		return e, err
	}
	m := sourceRE.FindStringSubmatch(e.Source)
	if m == nil {
		return e, fail("source.form", "source %q is not /<layer>/spaces/<space id>[/...] or /<layer>/host[/...]", e.Source)
	}
	e.Scope = "host"
	if m[2] != "" {
		e.Scope = m[2]
	}
	if e.Type, _, err = str("type", "type.form"); err != nil {
		return e, err
	}
	if !typeRE.MatchString(e.Type) {
		return e, fail("type.form", "type %q is not io.<namespace>.<segment>(.<segment>)+", e.Type)
	}
	if s, has, err := str("subject", "subject.empty"); err != nil {
		return e, err
	} else if has && s == "" {
		return e, fail("subject.empty", "subject is empty (leave it out)")
	} else {
		e.Subject = s
	}
	if e.Time, _, err = str("time", "time.form"); err != nil {
		return e, err
	}
	if !timeRE.MatchString(e.Time) {
		return e, fail("time.form", "time %q is not UTC with milliseconds and Z", e.Time)
	}
	if want := msTime(idMillis(uuid.MustParse(e.ID))); e.Time != want {
		return e, fail("time.id", "time %s is not the id's %s", e.Time, want)
	}
	if dct, _, err := str("datacontenttype", "datacontenttype.value"); err != nil {
		return e, err
	} else if dct != "application/json" {
		return e, fail("datacontenttype.value", "datacontenttype %q, not application/json", dct)
	}
	if _, _, err := str("dataschema", "attr.control"); err != nil {
		return e, err
	}
	if e.Actor, _, err = str("actor", "actor.form"); err != nil {
		return e, err
	}
	if !actorRE.MatchString(e.Actor) || len(e.Actor) > 128 {
		return e, fail("actor.form", "actor %q is not <kind>[:<name>] of at most 128 bytes", e.Actor)
	}
	if e.ActorRole, _, err = str("actorrole", "actorrole.form"); err != nil {
		return e, err
	}
	if _, has := raw["actorrole"]; has && !roleRE.MatchString(e.ActorRole) {
		return e, fail("actorrole.form", "actorrole %q", e.ActorRole)
	}
	if s, has, err := str("actorspace", "actorspace.form"); err != nil {
		return e, err
	} else if has {
		if !spaceRE.MatchString(s) {
			return e, fail("actorspace.form", "actorspace %q is not a space id", s)
		}
		if _, ok := raw["actorrole"]; !ok {
			return e, fail("actorspace.role", "actorspace without actorrole")
		}
		e.ActorSpace = s
	}
	if s, has, err := str("actorrun", "actorrun.form"); err != nil {
		return e, err
	} else if has && !idRE.MatchString(s) {
		return e, fail("actorrun.form", "actorrun %q is not a UUIDv7", s)
	} else {
		e.ActorRun = s
	}
	if s, has, err := str("actorclaimed", "actorclaimed.form"); err != nil {
		return e, err
	} else if has {
		if !claimedRE.MatchString(s) || len(s) > 256 {
			return e, fail("actorclaimed.form", "actorclaimed %q is not <actor>[ as [<space id>]/<role>] of at most 256 bytes", s)
		}
		stamp := e.Actor
		if _, ok := raw["actorrole"]; ok {
			stamp += " as " + e.ActorSpace + "/" + e.ActorRole
		}
		if s == stamp {
			return e, fail("actorclaimed.same", "actorclaimed is the attested identity")
		}
		e.ActorClaimed = s
	}
	if s, has, err := str("spacepath", "spacepath.form"); err != nil {
		return e, err
	} else if has && s == "" {
		return e, fail("spacepath.form", "spacepath is empty (leave it out)")
	} else {
		e.SpacePath = s
	}
	if s, has, err := str("traceparent", "traceparent.form"); err != nil {
		return e, err
	} else if has {
		tm := traceparentRE.FindStringSubmatch(s)
		if tm == nil || strings.Trim(tm[1], "0") == "" || strings.Trim(tm[2], "0") == "" {
			return e, fail("traceparent.form", "traceparent %q", s)
		}
		e.TraceParent = s
	}
	if s, has, err := str("tracestate", "tracestate.form"); err != nil {
		return e, err
	} else if has {
		if !tracestateRE.MatchString(s) {
			return e, fail("tracestate.form", "tracestate is not 1 to 512 printable ASCII characters")
		}
		if e.TraceParent == "" {
			return e, fail("tracestate.traceparent", "tracestate without traceparent")
		}
		e.TraceState = s
	}
	if s, has, err := str("commitment", "commitment.value"); err != nil {
		return e, err
	} else if has {
		if s != "confirmed" && s != "finalized" {
			return e, fail("commitment.value", "commitment %q, not confirmed or finalized", s)
		}
		if !strings.HasPrefix(e.Type, "io.lgat.") {
			return e, fail("commitment.namespace", "commitment on %s: io.lgat.* only", e.Type)
		}
	}
	for _, mb := range members {
		if known[mb.key] {
			continue
		}
		switch mb.val[0] {
		case '"':
			if _, _, err := str(mb.key, "ext.value"); err != nil {
				return e, err
			}
		case 't', 'f':
		default:
			if _, ok := new(big.Int).SetString(string(mb.val), 10); !ok || !int32ok(mb.val) {
				return e, fail("ext.value", "extension %s is not a string, a 32-bit integer or a boolean", mb.key)
			}
		}
	}
	if err := e.data(raw["data"]); err != nil {
		return e, err
	}
	return e, nil
}

func int32ok(v json.RawMessage) bool {
	n, ok := new(big.Int).SetString(string(v), 10)
	return ok && n.IsInt64() && n.Int64() >= -1<<31 && n.Int64() <= 1<<31-1
}

// data: an object, its top-level keys, unique names at every depth, integers within 2^53 - 1,
// and io.anfs.*'s flat strings
func (e *Event) data(d json.RawMessage) error {
	if len(d) == 0 || d[0] != '{' {
		return fail("data.object", "data is not a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(d))
	dec.UseNumber()
	if err := walk(dec); err != nil {
		return err
	}
	members, err := rawMembers(d)
	if err != nil {
		return err
	}
	flat := strings.HasPrefix(e.Type, "io.anfs.")
	e.Data = map[string]string{}
	e.RawData = d
	for _, m := range members {
		if !keyRE.MatchString(m.key) {
			return fail("data.key", "data key %q", m.key)
		}
		if m.val[0] != '"' {
			if flat {
				return fail("anfs.data.flat", "data.%s is not a string (io.anfs data is flat strings)", m.key)
			}
			continue
		}
		var s string
		if err := json.Unmarshal(m.val, &s); err != nil {
			return fail("json.parse", "data.%s: %v", m.key, err)
		}
		e.Data[m.key] = s
	}
	return nil
}

var maxInt53 = big.NewInt(1<<53 - 1)

// walk one JSON value from dec: names unique in each object, integers within 2^53 - 1
func walk(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return fail("json.parse", "%v", err)
	}
	switch v := t.(type) {
	case json.Delim:
		switch v {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return fail("json.parse", "%v", err)
				}
				k := kt.(string)
				if seen[k] {
					return fail("json.unique", "data: %q twice", k)
				}
				seen[k] = true
				if err := walk(dec); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := walk(dec); err != nil {
					return err
				}
			}
		}
		if _, err := dec.Token(); err != nil {
			return fail("json.parse", "%v", err)
		}
	case json.Number:
		s := v.String()
		if !strings.ContainsAny(s, ".eE") {
			n, _ := new(big.Int).SetString(s, 10)
			if n != nil && new(big.Int).Abs(n).Cmp(maxInt53) > 0 {
				return fail("data.int53", "data holds %s, beyond 2^53 - 1: write it as a string", s)
			}
		}
	}
	return nil
}

// Noncanonical says whether a valid line is not in the spool's canonical form (SPEC.md §6):
// whitespace outside strings, data not the last member, or anfsv not written "anfsv":1.
func Noncanonical(line []byte) bool {
	line = bytes.TrimSuffix(line, []byte("\n"))
	if !compact(line) || !anfsvRE.Match(line) {
		return true
	}
	members, err := rawMembers(line)
	return err != nil || len(members) == 0 || members[len(members)-1].key != "data"
}

var anfsvRE = regexp.MustCompile(`[{,]"anfsv":1[,}]`)

// compact: no whitespace outside strings
func compact(b []byte) bool {
	in, esc := false, false
	for _, c := range b {
		switch {
		case in && esc:
			esc = false
		case in && c == '\\':
			esc = true
		case in && c == '"':
			in = false
		case !in && c == '"':
			in = true
		case !in && (c == ' ' || c == '\t' || c == '\r' || c == '\n'):
			return false
		}
	}
	return true
}

// Canonical is a valid line in the canonical form, with its newline: the same members and
// values in the same order, whitespace outside strings removed, data moved last (the recorder,
// SPEC.md §7.3).
func Canonical(line []byte) ([]byte, error) {
	if _, err := Parse(line); err != nil {
		return nil, err
	}
	members, err := rawMembers(bytes.TrimSuffix(line, []byte("\n")))
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteByte('{')
	put := func(m member) {
		if b.Len() > 1 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(m.key)
		b.Write(k)
		b.WriteByte(':')
		json.Compact(&b, m.val)
	}
	var data *member
	for i := range members {
		if members[i].key == "data" {
			data = &members[i]
			continue
		}
		put(members[i])
	}
	put(*data)
	b.WriteString("}\n")
	return b.Bytes(), nil
}

type member struct {
	key string
	val json.RawMessage
}

// rawMembers are an object's members in their order, each value's bytes as written; a name twice
// is json.unique
func rawMembers(obj []byte) ([]member, error) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	t, err := dec.Token()
	if err != nil || t != json.Delim('{') {
		return nil, fail("json.object", "not a JSON object")
	}
	var out []member
	seen := map[string]bool{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, fail("json.parse", "%v", err)
		}
		k, ok := t.(string)
		if !ok {
			return nil, fail("json.object", "not a JSON object")
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fail("json.parse", "%v", err)
		}
		if seen[k] {
			return nil, fail("json.unique", "%s twice", k)
		}
		seen[k] = true
		out = append(out, member{k, bytes.TrimSpace(v)})
	}
	if _, err := dec.Token(); err != nil {
		return nil, fail("json.parse", "%v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fail("json.parse", "more after the object")
	}
	return out, nil
}

// Month is the UTC month (YYYY-MM) of an event: its time's.
func (e Event) Month() string {
	if len(e.Time) >= 7 {
		return e.Time[:7]
	}
	if u, err := uuid.Parse(e.ID); err == nil {
		return time.UnixMilli(idMillis(u)).UTC().Format("2006-01")
	}
	return time.Now().UTC().Format("2006-01")
}

func msTime(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z") }

func idMillis(u uuid.UUID) int64 {
	return int64(u[0])<<40 | int64(u[1])<<32 | int64(u[2])<<24 | int64(u[3])<<16 | int64(u[4])<<8 | int64(u[5])
}
