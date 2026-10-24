package cloudevent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The conformance examples of anfs-io/events, vendored in testdata/spec (VERSION names the
// commit): every example through Parse and through the schema, agreeing with the manifest
// (valid or the rule it breaks; the canonical flag; the NATS subject; the derived id), so the
// hand-written parser and the schema cannot drift.

const spec = "testdata/spec"

type example struct {
	File    string  `json:"file"`
	Valid   bool    `json:"valid"`
	Rule    *string `json:"rule"`
	Schema  *bool   `json:"schema"`
	Subject *string `json:"subject"`
	Key     *string `json:"key"`
	About   string  `json:"about"`
}

func manifest(t *testing.T) []example {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(spec, "examples", "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Version  string    `json:"version"`
		Examples []example `json:"examples"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Version != "1.0.0" || len(m.Examples) < 60 {
		t.Fatalf("manifest %s, %d examples", m.Version, len(m.Examples))
	}
	// every file is listed, once
	var files []string
	for _, d := range []string{"valid", "invalid"} {
		ents, _ := os.ReadDir(filepath.Join(spec, "examples", d))
		for _, e := range ents {
			files = append(files, d+"/"+e.Name())
		}
	}
	var listed []string
	for _, e := range m.Examples {
		listed = append(listed, e.File)
	}
	sort.Strings(files)
	sort.Strings(listed)
	if strings.Join(files, " ") != strings.Join(listed, " ") {
		t.Fatalf("the manifest does not list the files:\n%v\n%v", files, listed)
	}
	return m.Examples
}

func TestConformanceParse(t *testing.T) {
	valid, invalid := 0, 0
	for _, x := range manifest(t) {
		b, err := os.ReadFile(filepath.Join(spec, "examples", x.File))
		if err != nil {
			t.Fatal(err)
		}
		ev, err := Parse(b)
		if !x.Valid {
			invalid++
			if err == nil {
				t.Errorf("%s: accepted (%s)", x.File, x.About)
			} else if Rule(err) != *x.Rule {
				t.Errorf("%s: refused for %s (%v), the manifest says %s", x.File, Rule(err), err, *x.Rule)
			}
			continue
		}
		valid++
		if err != nil {
			t.Errorf("%s: %v", x.File, err)
			continue
		}
		if got, want := Noncanonical(b), x.Rule != nil && *x.Rule == "noncanonical"; got != want {
			t.Errorf("%s: noncanonical %v, the manifest says %v", x.File, got, want)
		}
		if ev.NatsSubject() != *x.Subject {
			t.Errorf("%s: subject %s, the manifest says %s", x.File, ev.NatsSubject(), *x.Subject)
		}
		if scope, typ, ok := SubjectParts(*x.Subject); !ok || scope != ev.Scope || typ != ev.Type {
			t.Errorf("%s: %s read back as %s %s", x.File, *x.Subject, scope, typ)
		}
		if x.Key != nil {
			if id := deriveID(t, ev.Time, *x.Key); id != ev.ID {
				t.Errorf("%s: id %s, derived from its key %s", x.File, ev.ID, id)
			}
		}
		// its canonical form is itself, or (noncanonical) a canonical line of the same event
		c, err := Canonical(b)
		if err != nil || Noncanonical(c) {
			t.Errorf("%s: canonical form %s: %v", x.File, c, err)
		} else if !Noncanonical(b) && !bytes.Equal(c, b) {
			t.Errorf("%s: a canonical line rewritten:\n%s", x.File, c)
		}
	}
	if valid != 20 || invalid != 44 {
		t.Errorf("%d valid, %d invalid", valid, invalid)
	}
}

// What a reader takes from the examples of other layers: the scope, the short type, the data's
// strings, the raw data
func TestConformanceReading(t *testing.T) {
	read := func(f string) Event {
		b, _ := os.ReadFile(filepath.Join(spec, "examples", "valid", f))
		ev, err := Parse(b)
		if err != nil {
			t.Fatal(err)
		}
		return ev
	}
	ev := read("cnfs-partitia-report-filed.jsonl")
	if ev.Scope != "m2vq7cfr4k" || ev.Short() != "cnfs.partitia.report.filed" || ev.Namespace() != "cnfs" || !json.Valid(ev.RawData) || !bytes.HasPrefix(ev.RawData, []byte("{")) {
		t.Errorf("%+v", ev)
	}
	ev = read("cnfs-host.jsonl")
	if ev.Scope != "host" || ev.Short() != "cnfs.appwork.worker.started" {
		t.Errorf("%+v", ev)
	}
	ev = read("traceparent.jsonl")
	if ev.TraceParent == "" || ev.TraceState == "" {
		t.Errorf("%+v", ev)
	}
	ev = read("data-null-value.jsonl")
	if len(ev.Data) != 0 || string(ev.RawData) != `{"report_id":4182,"closed_at":null}` {
		t.Errorf("data %v raw %s", ev.Data, ev.RawData)
	}
	ev = read("report-filed.jsonl")
	if ev.Data["title"] != "pcm run drops env" || ev.SpacePath != "anfs-dev" || ev.Actor != "claude:2e9431a0" || ev.Short() != "report.filed" {
		t.Errorf("%+v", ev)
	}
}

func TestConformanceSchema(t *testing.T) {
	sb, err := os.ReadFile(filepath.Join(spec, "schema", "event.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(sb))
	if err != nil {
		t.Fatal(err)
	}
	const id = "https://raw.githubusercontent.com/anfs-io/events/v1.0.0/schema/event.v1.json"
	c := jsonschema.NewCompiler()
	if err := c.AddResource(id, doc); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range manifest(t) {
		b, _ := os.ReadFile(filepath.Join(spec, "examples", x.File))
		inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("%s: %v", x.File, err)
		}
		err = sch.Validate(inst)
		switch {
		case x.Valid && err != nil:
			t.Errorf("%s: the schema refuses a valid example: %v", x.File, err)
		case !x.Valid && *x.Schema && err == nil:
			t.Errorf("%s: the schema passes it (manifest: schema true)", x.File)
		case !x.Valid && !*x.Schema && err != nil:
			t.Errorf("%s: the schema refuses it (manifest: schema false): %v", x.File, err)
		}
	}
}

// SPEC.md §9: the time's milliseconds, then the first 74 bits of SHA-256(key) as rand_a (12)
// and rand_b (62), version 7, variant 10
func deriveID(t *testing.T, ts, key string) string {
	tm, err := time.Parse("2006-01-02T15:04:05.000Z", ts)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(key))
	h := new(big.Int).Rsh(new(big.Int).SetBytes(sum[:]), 256-74)
	randA := new(big.Int).Rsh(h, 62)
	randB := new(big.Int).And(h, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 62), big.NewInt(1)))
	u := new(big.Int).Lsh(big.NewInt(tm.UnixMilli()), 80)
	u.Or(u, new(big.Int).Lsh(big.NewInt(7), 76))
	u.Or(u, new(big.Int).Lsh(randA, 64))
	u.Or(u, new(big.Int).Lsh(big.NewInt(2), 62))
	u.Or(u, randB)
	x := fmt.Sprintf("%032x", u)
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}
