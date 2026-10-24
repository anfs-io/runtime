package cloudevent

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Stamp is who wrote a line into a run's spool, as anfsd knows it (agents/02d): the run's actor
// (<harness>:<last 8 of the run id>), the rule's space id, the role and the run id.
type Stamp struct {
	Actor      string
	ActorSpace string
	ActorRole  string
	ActorRun   string
}

// stamped are the attributes Stamp writes; whatever a line says of them is replaced
var stamped = map[string]bool{"actor": true, "actorspace": true, "actorrole": true, "actorrun": true, "actorclaimed": true}

// order is the profile's attribute order (the bash writer's); the stamp goes where actor is, and
// data stays last, which the bash reader relies on
var order = []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype",
	"actor", "actorspace", "actorrole", "actorrun", "actorclaimed", "spacepath", "anfsv"}

// StampLine rewrites a valid line's actor, actorspace, actorrole and actorrun from st, and sets
// actorclaimed when the line's own actor, actorspace or actorrole differs from the stamp: its
// actor, plus " as <actorspace>/<actorrole>" when it named a role. Every other attribute keeps its
// bytes; the attributes are written in the profile's order, those it does not know after them,
// data last. The result has its newline, and is refused over Max.
func StampLine(line []byte, st Stamp) ([]byte, Event, error) {
	ev, err := Parse(line)
	if err != nil {
		return nil, ev, err
	}
	members, err := rawMembers(bytes.TrimRight(line, "\n"))
	if err != nil {
		return nil, ev, err
	}
	claim := ""
	if ev.Actor != st.Actor || ev.ActorSpace != "" && ev.ActorSpace != st.ActorSpace || ev.ActorRole != "" && ev.ActorRole != st.ActorRole {
		claim = ev.Actor
		if ev.ActorRole != "" {
			claim += " as " + ev.ActorSpace + "/" + ev.ActorRole
		}
	}
	str := func(s string) json.RawMessage { b, _ := json.Marshal(s); return b }
	set := map[string]json.RawMessage{"actor": str(st.Actor), "actorrun": str(st.ActorRun)}
	if st.ActorSpace != "" {
		set["actorspace"] = str(st.ActorSpace)
	}
	if st.ActorRole != "" {
		set["actorrole"] = str(st.ActorRole)
	}
	if claim != "" {
		set["actorclaimed"] = str(claim)
	}
	byKey := map[string]json.RawMessage{}
	for _, m := range members {
		if !stamped[m.key] {
			byKey[m.key] = m.val
		}
	}
	var b bytes.Buffer
	b.WriteByte('{')
	put := func(k string, v json.RawMessage) {
		if b.Len() > 1 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		b.Write(v)
	}
	known := map[string]bool{"data": true}
	for _, k := range order {
		known[k] = true
		if v, ok := set[k]; ok {
			put(k, v)
		} else if v, ok := byKey[k]; ok && !stamped[k] {
			put(k, v)
		}
	}
	for _, m := range members {
		if !known[m.key] && !stamped[m.key] {
			put(m.key, m.val)
		}
	}
	if v, ok := byKey["data"]; ok {
		put("data", v)
	}
	b.WriteString("}\n")
	if b.Len() > Max {
		return nil, ev, fmt.Errorf("%d bytes once stamped, over %d", b.Len(), Max)
	}
	out, err := Parse(b.Bytes())
	if err != nil {
		return nil, ev, fmt.Errorf("stamped a line the profile refuses: %w", err)
	}
	return b.Bytes(), out, nil
}
