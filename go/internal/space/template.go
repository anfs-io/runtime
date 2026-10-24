package space

import (
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strings"

	"github.com/anfs-io/runtime/go/internal/cloudevent"
)

// The names a template may use: {{id}}, {{type}}, {{subject}}, {{time}}, {{space}} (the path),
// {{actor}}, {{data.<key>}}, {{run}}, {{job}}. Plain substitution, no logic.
var templateRE = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.]*)\s*\}\}`)

var templateNames = map[string]bool{"id": true, "type": true, "subject": true, "time": true, "space": true, "actor": true, "run": true, "job": true}

var dataKeyRE = regexp.MustCompile(`^data\.[A-Za-z_][A-Za-z0-9_]*$`)

// checkTemplate refuses a name it does not know; job.name cannot use {{job}} or {{run}} (it is
// what makes the job, before the run has one).
func checkTemplate(t, what string, all bool) error {
	for _, m := range templateRE.FindAllStringSubmatch(t, -1) {
		n := m[1]
		if dataKeyRE.MatchString(n) {
			continue
		}
		if !templateNames[n] || (!all && (n == "job" || n == "run")) {
			return fmt.Errorf("%s: {{%s}} is not a template name (id, type, subject, time, space, actor, data.<key>%s)", what, n, map[bool]string{true: ", run, job", false: ""}[all])
		}
	}
	return nil
}

// Vars are a template's values for one event and run.
type Vars struct {
	Event cloudevent.Event
	Space string // the rule's space path
	Run   string
	Job   string
}

func (v Vars) get(n string) string {
	switch n {
	case "id":
		return v.Event.ID
	case "type":
		return v.Event.Short()
	case "subject":
		return v.Event.Subject
	case "time":
		return v.Event.Time
	case "space":
		return v.Space
	case "actor":
		return v.Event.Actor
	case "run":
		return v.Run
	case "job":
		return v.Job
	}
	if strings.HasPrefix(n, "data.") {
		return v.Event.Data[strings.TrimPrefix(n, "data.")]
	}
	return ""
}

// Render substitutes the names (validated at load).
func Render(t string, v Vars) string {
	return templateRE.ReplaceAllStringFunc(t, func(m string) string {
		return v.get(templateRE.FindStringSubmatch(m)[1])
	})
}

var notJobChar = regexp.MustCompile(`[^a-z0-9-]`)

// JobName renders job.name: each value lowered and anything outside [a-z0-9-] made -.
func JobName(t string, v Vars) string {
	return templateRE.ReplaceAllStringFunc(t, func(m string) string {
		return notJobChar.ReplaceAllString(strings.ToLower(v.get(templateRE.FindStringSubmatch(m)[1])), "-")
	})
}

// Matches says whether the event is one the rule acts on: its type, subject and data globs, and
// its space (the rule's own unless on.space names others). spacePath is the event's space's path
// ("" when it is not a known space; "host" for the host scope).
func (r *Rule) Matches(e cloudevent.Event, spacePath string) bool {
	if ok, _ := path.Match(r.On.Type, e.Short()); !ok {
		return false
	}
	if r.On.Subject != "" {
		if ok, _ := path.Match(r.On.Subject, e.Subject); !ok {
			return false
		}
	}
	for k, g := range r.On.Data {
		v, has := e.Data[k]
		if !has {
			return false
		}
		if ok, _ := path.Match(g, v); !ok {
			return false
		}
	}
	return SpecMatches(r.On.Space, r.Space, e.Scope, spacePath)
}

// SpecMatches: an empty spec is the rule's own space; a spec is a space id, a path, a prefix
// ending in / (that space and every one under it), or * (any space, the host scope included).
func SpecMatches(spec string, own *Space, scope, spacePath string) bool {
	switch {
	case spec == "":
		return scope == own.ID
	case spec == "*":
		return true
	case spec == scope:
		return true
	case spacePath == "":
		return false
	case strings.HasSuffix(spec, "/"):
		return spacePath+"/" == spec || strings.HasPrefix(spacePath, spec)
	}
	return spacePath == spec
}

// FilterSubject is the NATS subject the rule needs from the stream: anfs.<scope>., the type's
// namespace, then the glob's literal tokens, ending in > once the glob has a wildcard (the globs
// are matched in Go). The subject is the type without io. (SPEC.md §5), so an anfs short type
// gets its namespace back (report.* is anfs.<id>.anfs.report.>) and another namespace's has it
// already (cnfs.* is anfs.<id>.cnfs.>); a glob whose first token is a wildcard is anfs.<id>.>.
func (r *Rule) FilterSubject() string {
	scope := r.Space.ID
	if r.On.Space != "" {
		scope = "*"
	}
	toks := strings.Split(r.On.Type, ".")
	var lit []string
	wild := false
	for _, t := range toks {
		if strings.ContainsAny(t, "*?[\\") {
			wild = true
			break
		}
		lit = append(lit, t)
	}
	s := "anfs." + scope
	if len(lit) > 0 && !cloudevent.IsNamespace(lit[0]) {
		lit = append([]string{"anfs"}, lit...)
	}
	if len(lit) > 0 {
		s += "." + strings.Join(lit, ".")
	}
	if wild || len(lit) == 0 {
		s += ".>"
	}
	return s
}

func yamlKeys(v any) map[string]bool {
	out := map[string]bool{}
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return out
	}
	for i := 0; i < t.NumField(); i++ {
		tag := strings.Split(t.Field(i).Tag.Get("yaml"), ",")[0]
		if tag == "" {
			tag = strings.ToLower(t.Field(i).Name)
		}
		out[tag] = true
	}
	return out
}

func isStruct(v any) bool {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}
