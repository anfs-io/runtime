package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const examples = "../../internal/cloudevent/testdata/spec/examples"

// anfsd validate: one line per problem, FILE:LINE RULE message; the counts last; exit 1 on an
// invalid line or an unreadable file, 0 with only noncanonical ones, 64 without a file
func TestValidate(t *testing.T) {
	ex := func(f string) []byte {
		b, err := os.ReadFile(filepath.Join(examples, f))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good.jsonl")
	os.WriteFile(good, append(append(ex("valid/report-filed.jsonl"), ex("valid/cnfs-partitia-report-filed.jsonl")...), ex("valid/noncanonical.jsonl")...), 0o644)
	bad := filepath.Join(dir, "bad.jsonl")
	os.WriteFile(bad, append(append(ex("valid/report-closed.jsonl"), ex("invalid/time-not-id.jsonl")...), []byte("\nnot json")...), 0o644)

	var out, errb bytes.Buffer
	if c := validateCmd([]string{good}, nil, &out, &errb); c != 0 {
		t.Fatalf("exit %d: %s", c, out.String())
	}
	if want := good + ":3 noncanonical valid, but not the spool's canonical form (compact, data last, \"anfsv\":1): the bash reader needs it\n3 events, 0 invalid, 1 noncanonical (1 files)\n"; out.String() != want {
		t.Errorf("%q", out.String())
	}
	out.Reset()
	if c := validateCmd([]string{bad, filepath.Join(dir, "none.jsonl")}, nil, &out, &errb); c != 1 {
		t.Fatalf("exit %d", c)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 5 || !strings.HasPrefix(lines[0], bad+":2 time.id time 2026-10-24T10:21:07.122Z is not the id's 2026-10-24T10:21:07.121Z") ||
		lines[1] != bad+":3 json.parse not JSON" || lines[2] != bad+":4 json.parse not JSON" ||
		lines[3] != filepath.Join(dir, "none.jsonl")+": cannot read: no such file or directory" || lines[4] != "4 events, 3 invalid, 0 noncanonical (1 files)" {
		t.Errorf("%s", out.String())
	}
	// stdin
	out.Reset()
	if c := validateCmd([]string{"-"}, bytes.NewReader(ex("invalid/anfs-data-nested.jsonl")), &out, &errb); c != 1 || !strings.HasPrefix(out.String(), "-:1 anfs.data.flat ") {
		t.Errorf("%d %s", c, out.String())
	}
	// a spool file: its directory is the scope, its name the month; the recorder's too
	ev := filepath.Join(dir, "events")
	os.MkdirAll(filepath.Join(ev, "host"), 0o755)
	os.MkdirAll(filepath.Join(ev, "tkj76y3nfx"), 0o755)
	os.WriteFile(filepath.Join(ev, "host", "2026-10.jsonl"), append(ex("valid/source-updated.jsonl"), ex("valid/report-closed.jsonl")...), 0o644)
	os.WriteFile(filepath.Join(ev, "tkj76y3nfx", "2026-09.bus.jsonl"), ex("valid/report-closed.jsonl"), 0o644)
	out.Reset()
	if c := validateCmd([]string{filepath.Join(ev, "host", "2026-10.jsonl"), filepath.Join(ev, "tkj76y3nfx", "2026-09.bus.jsonl")}, nil, &out, &errb); c != 1 {
		t.Fatalf("exit %d", c)
	}
	if !strings.Contains(out.String(), "host/2026-10.jsonl:2 spool.scope an event of tkj76y3nfx (its source /anfs/spaces/tkj76y3nfx) in host's directory\n") ||
		!strings.Contains(out.String(), "tkj76y3nfx/2026-09.bus.jsonl:1 spool.month an event of 2026-10 (its time) in 2026-09's file\n") ||
		!strings.HasSuffix(out.String(), "3 events, 2 invalid, 0 noncanonical (2 files)\n") {
		t.Errorf("%s", out.String())
	}
	if c := validateCmd(nil, nil, &out, &errb); c != 64 {
		t.Errorf("no file: %d", c)
	}
	if c := run([]string{"validate", good}, &out, &errb); c != 0 {
		t.Errorf("run validate: %d", c)
	}
}
