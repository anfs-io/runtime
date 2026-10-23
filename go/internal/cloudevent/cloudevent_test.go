package cloudevent

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Lines written by core's lib/anfs/events.sh (01's example first) parse, with their scope and subject
func TestParseBashLines(t *testing.T) {
	f, err := os.Open("testdata/bash.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	want := []struct{ scope, subj, typ string }{
		{"k3x7q2mbzp", "anfs.k3x7q2mbzp.report.filed", "io.anfs.report.filed"},
		{"abcde23456", "anfs.abcde23456.report.filed", "io.anfs.report.filed"},
		{"host", "anfs.host.source.updated", "io.anfs.source.updated"},
	}
	sc := bufio.NewScanner(f)
	i := 0
	for sc.Scan() {
		ev, err := Parse(sc.Bytes())
		if err != nil {
			t.Fatalf("line %d: %v", i+1, err)
		}
		if ev.Scope != want[i].scope || ev.NatsSubject() != want[i].subj || ev.Type != want[i].typ {
			t.Errorf("line %d: scope %s subject %s type %s", i+1, ev.Scope, ev.NatsSubject(), ev.Type)
		}
		i++
	}
	if i != 3 {
		t.Fatalf("%d lines", i)
	}
	ev, _ := Parse([]byte(firstLine(t)))
	if ev.Data["title"] != "pcm run drops env" || ev.SpacePath != "anfs-dev" || ev.Actor != "claude:2e9431a0" {
		t.Errorf("attributes: %+v", ev)
	}
}

func firstLine(t *testing.T) string {
	b, err := os.ReadFile("testdata/bash.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return strings.SplitN(string(b), "\n", 2)[0]
}

func TestParseRefuses(t *testing.T) {
	good := firstLine(t)
	cases := map[string]string{
		"no specversion":  strings.Replace(good, `"specversion":"1.0",`, "", 1),
		"specversion 0.3": strings.Replace(good, `"specversion":"1.0"`, `"specversion":"0.3"`, 1),
		"anfsv 2":         strings.Replace(good, `"anfsv":1`, `"anfsv":2`, 1),
		"no anfsv":        strings.Replace(good, `"anfsv":1,`, "", 1),
		"non-string data": strings.Replace(good, `"to":"file"`, `"to":1`, 1),
		"data a list":     strings.Replace(good, `"data":{"package":"core/pcm","title":"pcm run drops env","to":"file"}`, `"data":["x"]`, 1),
		"over 4096":       strings.Replace(good, `"to":"file"`, `"to":"`+strings.Repeat("x", 4096)+`"`, 1),
		"not json":        good[:len(good)-1],
		"a v4 id":         strings.Replace(good, `01a169b7-85e8-74aa`, `01a169b7-85e8-44aa`, 1),
		"another source":  strings.Replace(good, `/anfs/spaces/k3x7q2mbzp`, `/elsewhere`, 1),
		"a bad space id":  strings.Replace(good, `/anfs/spaces/k3x7q2mbzp`, `/anfs/spaces/K3X`, 1),
		"another type":    strings.Replace(good, `io.anfs.report.filed`, `com.example.x`, 1),
		"a type with *":   strings.Replace(good, `io.anfs.report.filed`, `io.anfs.report.*`, 1),
		"no actor":        strings.Replace(good, `"actor":"claude:2e9431a0",`, "", 1),
		"a local time":    strings.Replace(good, `17:03:17.480Z`, `17:03:17+02:00`, 1),
		"a bad data key":  strings.Replace(good, `"to":"file"`, `"a-b":"file"`, 1),
		"two lines":       strings.Replace(good, `"to":"file"`, "\"to\":\"file\"}\n{", 1),
	}
	for name, line := range cases {
		if _, err := Parse([]byte(line)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// exactly 4096 with the newline is fine
	pad := 4096 - len(good) - 1
	l := strings.Replace(good, `"to":"file"`, `"to":"file`+strings.Repeat("x", pad)+`"`, 1)
	if len(l)+1 != 4096 {
		t.Fatalf("len %d", len(l)+1)
	}
	if _, err := Parse([]byte(l + "\n")); err != nil {
		t.Errorf("4096 bytes refused: %v", err)
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
