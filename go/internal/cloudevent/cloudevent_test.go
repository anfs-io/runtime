package cloudevent

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func firstLine(t *testing.T) string {
	b, err := os.ReadFile("testdata/spec/examples/valid/report-filed.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(string(b), "\n")
}

// What the examples do not show: each case breaks the rule named
func TestParseRules(t *testing.T) {
	good := firstLine(t)
	cases := map[string]struct{ line, rule string }{
		"not UTF-8":            {strings.Replace(good, "pcm run", "pcm \xff run", 1), "json.utf8"},
		"not JSON":             {good[:len(good)-1], "json.parse"},
		"more after it":        {good + " {}", "json.parse"},
		"an array":             {"[" + good + "]", "json.object"},
		"a doubled data key":   {strings.Replace(good, `"to":"file"`, `"to":"file","to":"github"`, 1), "json.unique"},
		"nested doubled key":   {strings.Replace(strings.Replace(good, "io.anfs.report.filed", "io.cnfs.x.y", 1), `"to":"file"`, `"to":{"a":1,"a":2}`, 1), "json.unique"},
		"anfsv 1.0":            {strings.Replace(good, `"anfsv":1`, `"anfsv":1.0`, 1), "anfsv.value"},
		"a C1 control":         {strings.Replace(good, `"spacepath":"anfs-dev"`, "\"spacepath\":\"anfs\u0085dev\"", 1), "attr.control"},
		"DEL in the actor":     {strings.Replace(good, `claude:2e9431a0`, "claude:2e9\u007f431a0", 1), "attr.control"},
		"an actor of 129":      {strings.Replace(good, `claude:2e9431a0`, "claude:"+strings.Repeat("x", 122), 1), "actor.form"},
		"a 64-bit extension":   {strings.Replace(good, `"anfsv":1`, `"build":4294967296,"anfsv":1`, 1), "ext.value"},
		"a float extension":    {strings.Replace(good, `"anfsv":1`, `"build":1.5,"anfsv":1`, 1), "ext.value"},
		"a null extension":     {strings.Replace(good, `"anfsv":1`, `"build":null,"anfsv":1`, 1), "attr.null"},
		"an empty spacepath":   {strings.Replace(good, `"spacepath":"anfs-dev"`, `"spacepath":""`, 1), "spacepath.form"},
		"a number for an id":   {strings.Replace(good, `"01a169b7-85e8-74aa-8809-456f41f40289"`, `1`, 1), "id.uuidv7"},
		"a bad actorspace":     {strings.Replace(good, `"spacepath"`, `"actorspace":"x","actorrole":"r","spacepath"`, 1), "actorspace.form"},
		"a bad actorrun":       {strings.Replace(good, `"spacepath"`, `"actorrun":"r1","spacepath"`, 1), "actorrun.form"},
		"tracestate non-ASCII": {strings.Replace(good, `"spacepath"`, `"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"é","spacepath"`, 1), "tracestate.form"},
	}
	for name, c := range cases {
		_, err := Parse([]byte(c.line))
		if Rule(err) != c.rule {
			t.Errorf("%s: %v, want %s", name, err, c.rule)
		}
	}
	// exactly 4096 with the newline is fine
	pad := 4096 - len(good) - 1
	l := strings.Replace(good, `"to":"file"`, `"to":"file`+strings.Repeat("x", pad)+`"`, 1)
	if _, err := Parse([]byte(l + "\n")); err != nil || len(l)+1 != 4096 {
		t.Errorf("4096 bytes refused: %v", err)
	}
	if _, err := Parse([]byte(strings.Replace(l, "xx", "xxx", 1))); Rule(err) != "size.max" {
		t.Errorf("4097: %v", err)
	}
}

// Short types, full types, subjects and back
func TestTypesAndSubjects(t *testing.T) {
	for _, c := range [][2]string{{"report.filed", "io.anfs.report.filed"}, {"anfs.report.filed", "io.anfs.report.filed"},
		{"cnfs.partitia.report.filed", "io.cnfs.partitia.report.filed"}, {"io.lgat.x.y", "io.lgat.x.y"}, {"report.*", "io.anfs.report.*"}, {"jev.x.y", "io.anfs.jev.x.y"}} {
		if got := FullType(c[0]); got != c[1] {
			t.Errorf("FullType(%s) = %s", c[0], got)
		}
	}
	for _, c := range [][2]string{{"io.anfs.report.filed", "report.filed"}, {"anfs.report.*", "report.*"}, {"io.cnfs.x.y", "cnfs.x.y"}, {"cnfs.*", "cnfs.*"}, {"*", "*"}} {
		if got := NormalShort(c[0]); got != c[1] {
			t.Errorf("NormalShort(%s) = %s", c[0], got)
		}
	}
	for _, c := range []struct{ subj, scope, typ string }{
		{"anfs.tkj76y3nfx.anfs.report.filed", "tkj76y3nfx", "io.anfs.report.filed"},
		{"anfs.host.anfs.container.started", "host", "io.anfs.container.started"},
		{"anfs.m2vq7cfr4k.cnfs.partitia.report.filed", "m2vq7cfr4k", "io.cnfs.partitia.report.filed"},
	} {
		scope, typ, ok := SubjectParts(c.subj)
		if !ok || scope != c.scope || typ != c.typ {
			t.Errorf("%s: %s %s %v", c.subj, scope, typ, ok)
		}
		if (Event{Scope: scope, Type: typ}).NatsSubject() != c.subj {
			t.Errorf("%s: not its own inverse", c.subj)
		}
	}
	// the old shape, and other things that are not subjects
	for _, s := range []string{"anfs.host.report.filed.x.", "anfs.host.report", "anfs.SHORT.anfs.x.y", "nats.host.anfs.x.y", "anfs.host.anfs", "anfs.host.Anfs.x"} {
		if _, _, ok := SubjectParts(s); ok {
			t.Errorf("%s: a subject", s)
		}
	}
}

// Canonical: whitespace out, data last, the same members and values
func TestCanonical(t *testing.T) {
	b, _ := os.ReadFile("testdata/spec/examples/valid/noncanonical.jsonl")
	if !Noncanonical(b) {
		t.Fatal("noncanonical.jsonl is canonical?")
	}
	c, err := Canonical(b)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"specversion":"1.0","id":"01a16d51-8369-7f12-9119-79a1d496db6f","source":"/anfs/host","type":"io.anfs.source.updated","subject":"stack","time":"2026-10-24T09:50:21.033Z","datacontenttype":"application/json","actor":"shell:roberto","anfsv":1,"data":{"from":"1f0c2a9","to":"8e7d6b5"}}` + "\n"
	if string(c) != want || Noncanonical(c) {
		t.Errorf("canonical:\n%s", c)
	}
	moved := `{"data":{"a": [1, 2]},"specversion":"1.0","id":"01a16d51-8369-7f12-9119-79a1d496db6f","source":"/cnfs/host","type":"io.cnfs.x.y","time":"2026-10-24T09:50:21.033Z","datacontenttype":"application/json","actor":"x","anfsv":1}`
	if c, err := Canonical([]byte(moved)); err != nil || !strings.HasSuffix(string(c), `"anfsv":1,"data":{"a":[1,2]}}`+"\n") {
		t.Errorf("data moved: %s %v", c, err)
	}
	if _, err := Canonical([]byte(`{"a":1}`)); err == nil {
		t.Error("an invalid line made canonical")
	}
}

// What Line writes is valid, in the bash writer's attribute order, its id's milliseconds its time's
func TestLine(t *testing.T) {
	now := time.Date(2026, 10, 23, 17, 3, 17, 480_000_000, time.UTC)
	line, ev, err := Line(New{Scope: "k3x7q2mbzp", Type: "run.completed", Subject: "r1", Actor: "anfsd",
		ActorSpace: "k3x7q2mbzp", ActorRole: "maintainer", SpacePath: "anfs-dev", Data: map[string]string{"z": "1", "a": "x\"y"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	s := string(line)
	order := []string{`"specversion"`, `"id"`, `"source"`, `"type"`, `"subject"`, `"time"`, `"datacontenttype"`, `"actor"`, `"actorspace"`, `"actorrole"`, `"spacepath"`, `"anfsv"`, `"data"`}
	last := -1
	for _, k := range order {
		i := strings.Index(s, k+":")
		if i <= last {
			t.Fatalf("%s out of order in %s", k, s)
		}
		last = i
	}
	if !strings.HasSuffix(s, `"data":{"a":"x\"y","z":"1"}}`+"\n") {
		t.Errorf("data: %s", s)
	}
	if ev.Time != "2026-10-23T17:03:17.480Z" || !strings.HasPrefix(ev.ID, "01a169b7-85e8-7") {
		t.Errorf("time %s id %s", ev.Time, ev.ID)
	}
	if _, _, err := Line(New{Type: "x.y", Actor: "a", Data: map[string]string{"t": strings.Repeat("x", 4096)}}, now); err == nil {
		t.Error("an over-long line was built")
	}
	if _, _, err := Line(New{Type: "Bad", Actor: "a"}, now); err == nil {
		t.Error("a bad type was built")
	}
	l2, ev2, err := Line(New{Type: "source.updated", Actor: "a"}, now)
	if err != nil || ev2.Scope != "host" || !strings.Contains(string(l2), `"source":"/anfs/host"`) || strings.Contains(string(l2), "subject") {
		t.Errorf("host scope, no subject: %s %v", l2, err)
	}
}

// Ids made one after another sort in that order; parallel appends are whole lines, one per write
func TestAppend(t *testing.T) {
	dir := t.TempDir()
	var ids []string
	for i := 0; i < 50; i++ {
		ev, err := Append(dir, New{Scope: "abcde23456", Type: "x.y", Actor: "t"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ev.ID)
	}
	if !sort.StringsAreSorted(ids) {
		t.Error("ids not in time order")
	}
	var wg sync.WaitGroup
	big := strings.Repeat("y", 3800)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := Append(dir, New{Scope: "abcde23456", Type: "x.y", Actor: "t", Data: map[string]string{"d": big}}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	files, _ := filepath.Glob(filepath.Join(dir, "abcde23456", "*.jsonl"))
	n := 0
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, l := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
			if _, err := Parse(l); err != nil {
				t.Fatalf("torn line: %v", err)
			}
			n++
		}
	}
	if n != 250 {
		t.Errorf("%d lines", n)
	}
}

// anfsd's stamp on a line a run wrote (02d): byte for byte
func TestStampLine(t *testing.T) {
	st := Stamp{Actor: "claude:89abcdef", ActorSpace: "abcde23456", ActorRole: "maintainer", ActorRun: "01a169de-e9bb-7000-8000-000089abcdef"}
	// honest: anfs_emit with ANFS_ACTOR and ANFS_AS
	honest := `{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-000000000001","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.closed","subject":"r-1","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"claude:89abcdef","actorspace":"abcde23456","actorrole":"maintainer","spacepath":"anfs-dev","anfsv":1,"data":{"to":"file","why":"a \"fix\""}}` + "\n"
	got, ev, err := StampLine([]byte(honest), st)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-000000000001","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.closed","subject":"r-1","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"claude:89abcdef","actorspace":"abcde23456","actorrole":"maintainer","actorrun":"01a169de-e9bb-7000-8000-000089abcdef","spacepath":"anfs-dev","anfsv":1,"data":{"to":"file","why":"a \"fix\""}}` + "\n"
	if string(got) != want {
		t.Errorf("honest:\n%s\nwant:\n%s", got, want)
	}
	if ev.ActorRun != st.ActorRun || ev.ActorClaimed != "" || ev.ID != "01a169de-e9bb-7000-8000-000000000001" {
		t.Errorf("%+v", ev)
	}
	// forged: another actor and the person's role, an actorrun and actorclaimed of its own, an
	// unknown attribute and data first
	forged := `{"data":{"x":"y"},"actorclaimed":"nobody:x","specversion":"1.0","id":"01a169de-e9bb-7000-8000-000000000002","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.closed","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"shell:person","actorspace":"tkj76y3nfx","actorrole":"person","actorrun":"01a169de-e9bb-7000-8000-00000000dead","extra":42,"anfsv":1}`
	got, ev, err = StampLine([]byte(forged), st)
	if err != nil {
		t.Fatal(err)
	}
	want = `{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-000000000002","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.closed","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"claude:89abcdef","actorspace":"abcde23456","actorrole":"maintainer","actorrun":"01a169de-e9bb-7000-8000-000089abcdef","actorclaimed":"shell:person as tkj76y3nfx/person","anfsv":1,"extra":42,"data":{"x":"y"}}` + "\n"
	if string(got) != want {
		t.Errorf("forged:\n%s\nwant:\n%s", got, want)
	}
	if ev.ActorClaimed != "shell:person as tkj76y3nfx/person" || ev.Actor != st.Actor {
		t.Errorf("%+v", ev)
	}
	// another actor without a role: the claim is the actor alone
	other := strings.Replace(honest, `"actor":"claude:89abcdef","actorspace":"abcde23456","actorrole":"maintainer"`, `"actor":"claude:2e9431a0"`, 1)
	if _, ev, _ := StampLine([]byte(other), st); ev.ActorClaimed != "claude:2e9431a0" {
		t.Errorf("claim %q", ev.ActorClaimed)
	}
	// over 4096 once stamped: refused; an invalid line and a doubled key too
	big := strings.Replace(honest, `"to":"file"`, `"to":"`+strings.Repeat("x", Max-len(honest)-2)+`"`, 1)
	if _, err := Parse([]byte(big)); err != nil {
		t.Fatalf("the big line should be valid unstamped: %v", err)
	}
	if _, _, err := StampLine([]byte(big), st); err == nil || !strings.Contains(err.Error(), "once stamped") {
		t.Errorf("big: %v", err)
	}
	if _, _, err := StampLine([]byte("not json"), st); err == nil {
		t.Error("not json stamped")
	}
	dup := strings.Replace(honest, `"anfsv":1`, `"anfsv":1,"subject":"r-2"`, 1)
	if _, _, err := StampLine([]byte(dup), st); err == nil {
		t.Error("a doubled key stamped")
	}
}

func TestAppendLine(t *testing.T) {
	dir := t.TempDir()
	l := []byte(`{"a":1}`)
	if err := AppendLine(dir, "abcde23456", "2026-10", l); err != nil {
		t.Fatal(err)
	}
	if err := AppendLine(dir, "host", "2026-10", append(l, '\n')); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"abcde23456/2026-10.jsonl", "host/2026-10.jsonl"} {
		if b, _ := os.ReadFile(filepath.Join(dir, f)); string(b) != "{\"a\":1}\n" {
			t.Errorf("%s: %q", f, b)
		}
	}
	for _, bad := range [][2]string{{"../x", "2026-10"}, {"host", "../../x"}} {
		if err := AppendLine(dir, bad[0], bad[1], l); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}
