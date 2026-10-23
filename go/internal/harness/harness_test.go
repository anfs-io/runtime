package harness

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "write the golden files")

func spec(h string) Spec {
	return Spec{
		Harness: h, Model: "m1", BudgetUSD: 10, Name: "fix-core-report:89abcdef",
		Prompt: "Fix it.\n\n---\nfooter", Cwd: "/work/fix-r-1", RunDir: "/state/runs/r1", Timeout: 2 * time.Hour,
		Write: []string{"/work/fix-r-1", "/tmp/anfs-work-501-3", "/notes/anfs"},
		Read:  []string{"/spaces/anfs-dev", "/home/u/.local/share/anfs/sources"},
	}
}

// The command line and files per harness, as golden files (go test -update writes them)
func TestGolden(t *testing.T) {
	for _, h := range []string{"claude", "opencode", "pi", "antigravity"} {
		c, err := Build(spec(h))
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		b.WriteString("# argv\n")
		for _, a := range c.Argv {
			b.WriteString(strings.ReplaceAll(a, "\n", `\n`) + "\n")
		}
		if len(c.Env) > 0 {
			b.WriteString("# env\n" + strings.Join(c.Env, "\n") + "\n")
		}
		for name, f := range c.Files {
			b.WriteString("# " + name + "\n" + string(f))
		}
		golden := filepath.Join("testdata", h+".golden")
		if *update {
			os.MkdirAll("testdata", 0o755)
			os.WriteFile(golden, []byte(b.String()), 0o644)
			continue
		}
		want, err := os.ReadFile(golden)
		if err != nil {
			t.Fatalf("%v (go test -update)", err)
		}
		if b.String() != string(want) {
			t.Errorf("%s:\n%s\nwant:\n%s", h, b.String(), want)
		}
	}
}

// claude.json: the shape Claude Code reads (a wrong one is silently ignored under -p)
func TestClaudeSettings(t *testing.T) {
	c, _ := Build(spec("claude"))
	var cfg struct {
		Permissions struct {
			DefaultMode string   `json:"defaultMode"`
			Allow       []string `json:"allow"`
			Deny        []string `json:"deny"`
		} `json:"permissions"`
		Sandbox struct {
			Enabled                  bool `json:"enabled"`
			FailIfUnavailable        bool `json:"failIfUnavailable"`
			AllowUnsandboxedCommands bool `json:"allowUnsandboxedCommands"`
			Filesystem               struct {
				AllowWrite []string `json:"allowWrite"`
			} `json:"filesystem"`
		} `json:"sandbox"`
	}
	dec := json.NewDecoder(strings.NewReader(string(c.Files["claude.json"])))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	p := cfg.Permissions
	has := func(l []string, s string) bool {
		for _, x := range l {
			if x == s {
				return true
			}
		}
		return false
	}
	if p.DefaultMode != "dontAsk" || !has(p.Allow, "Read(//spaces/anfs-dev/**)") || !has(p.Allow, "Edit(//notes/anfs/**)") ||
		!has(p.Allow, "Write(//work/fix-r-1/**)") || has(p.Allow, "Edit(//spaces/anfs-dev/**)") || !has(p.Allow, "Bash") {
		t.Errorf("allow %v", p.Allow)
	}
	for _, d := range []string{"Bash(git push:*)", "Bash(gh:*)", "Bash(gh)", "Bash(anfs report close:*)", "Bash(sudo:*)", "Bash(anfs agents:*)"} {
		if !has(p.Deny, d) {
			t.Errorf("deny lacks %s", d)
		}
	}
	if !cfg.Sandbox.Enabled || !cfg.Sandbox.FailIfUnavailable || cfg.Sandbox.AllowUnsandboxedCommands ||
		strings.Join(cfg.Sandbox.Filesystem.AllowWrite, " ") != "/notes/anfs /tmp/anfs-work-501-3 /work/fix-r-1" {
		t.Errorf("sandbox %+v", cfg.Sandbox)
	}
	// never a flag that skips permissions; the prompt is last, after --
	a := strings.Join(c.Argv, " ")
	if strings.Contains(a, "dangerously") || strings.Contains(a, "bypass") || c.Argv[len(c.Argv)-2] != "--" {
		t.Errorf("argv %v", c.Argv)
	}
}

func TestOpencodeConfig(t *testing.T) {
	c, _ := Build(spec("opencode"))
	v := strings.TrimPrefix(c.Env[0], "OPENCODE_CONFIG_CONTENT=")
	var cfg struct {
		Permission struct {
			External map[string]string `json:"external_directory"`
			Bash     map[string]string `json:"bash"`
		} `json:"permission"`
	}
	if err := json.Unmarshal([]byte(v), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Permission.External["*"] != "deny" || cfg.Permission.External["/notes/anfs/**"] != "allow" ||
		cfg.Permission.Bash["git push *"] != "deny" || cfg.Permission.Bash["*"] != "allow" {
		t.Errorf("%+v", cfg)
	}
}

func TestPiReadOnly(t *testing.T) {
	s := spec("pi")
	s.Write = []string{"/state/runs/r1/tmp"}
	c, _ := Build(s)
	if !strings.Contains(strings.Join(c.Argv, " "), "--tools read,grep,find,ls,bash") {
		t.Errorf("%v", c.Argv)
	}
	c, _ = Build(spec("pi"))
	if strings.Contains(strings.Join(c.Argv, " "), "--tools") {
		t.Errorf("%v", c.Argv)
	}
}

func TestSummary(t *testing.T) {
	claude := `{"type":"system","subtype":"init"}
{"type":"assistant","message":{"content":[{"type":"text","text":"step"}]}}
{"type":"result","subtype":"success","result":"All done: left review."}
`
	if s := Summary("claude", []byte(claude)); s != "All done: left review." {
		t.Errorf("%q", s)
	}
	oc := `{"type":"text","part":{"type":"text","text":"first"}}
{"type":"tool","part":{"type":"tool"}}
{"type":"text","part":{"type":"text","text":"the end"}}
`
	if s := Summary("opencode", []byte(oc)); s != "the end" {
		t.Errorf("%q", s)
	}
	pi := `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"pi says"}]}}`
	if s := Summary("pi", []byte(pi)); s != "pi says" {
		t.Errorf("%q", s)
	}
	long := strings.Repeat("é", 2000)
	if s := Summary("antigravity", []byte(long)); len(s) > SummaryMax || !strings.HasSuffix(s, "é") || !strings.HasPrefix(s, "é") {
		t.Errorf("%d bytes", len(s))
	}
	if r := Render("claude", []byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"ls"}}]}}`)); r != `> Bash {"command":"ls"}` {
		t.Errorf("render %q", r)
	}
	if r := Render("claude", []byte("not json")); r != "not json" {
		t.Errorf("render %q", r)
	}
}
