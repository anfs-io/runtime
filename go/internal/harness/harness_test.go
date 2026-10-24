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
		Spool: "/state/runs/r1/events", Home: "/home/u", Secrets: Secrets("/home/u", ""),
	}
}

// The command line and files per harness, as golden files (go test -update writes them)
func TestGolden(t *testing.T) {
	for _, h := range []string{"claude", "claude-bare", "opencode", "pi", "antigravity"} {
		sp := spec(strings.TrimSuffix(h, "-bare"))
		sp.Bare = strings.HasSuffix(h, "-bare")
		c, err := Build(sp)
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
		ClaudeMdExcludes []string `json:"claudeMdExcludes"`
		Permissions      struct {
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
				DenyRead   []string `json:"denyRead"`
			} `json:"filesystem"`
			Credentials struct {
				EnvVars []struct {
					Name string `json:"name"`
					Mode string `json:"mode"`
				} `json:"envVars"`
			} `json:"credentials"`
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
		strings.Join(cfg.Sandbox.Filesystem.AllowWrite, " ") != "/notes/anfs /state/runs/r1/events /tmp/anfs-work-501-3 /work/fix-r-1" {
		t.Errorf("sandbox %+v", cfg.Sandbox)
	}
	// the run's spool: its shell may write there (anfs_emit), its file tools may not
	for _, a := range p.Allow {
		if strings.Contains(a, "/state/runs/r1/events") {
			t.Errorf("the spool in a file tool's allow: %s", a)
		}
	}
	// the person's credential files unreadable by the shell; claude's key unset in it; the
	// person's CLAUDE.md and rules not loaded
	if strings.Join(cfg.Sandbox.Filesystem.DenyRead, " ") != "/home/u/.claude/.credentials.json /home/u/.config/gh /home/u/.git-credentials /home/u/.local/state/nats /home/u/.netrc /home/u/.ssh" {
		t.Errorf("denyRead %v", cfg.Sandbox.Filesystem.DenyRead)
	}
	var ev []string
	for _, e := range cfg.Sandbox.Credentials.EnvVars {
		ev = append(ev, e.Name+"="+e.Mode)
	}
	if strings.Join(ev, " ") != "ANTHROPIC_API_KEY=deny CLAUDE_CODE_OAUTH_TOKEN=deny" {
		t.Errorf("credentials.envVars %v", ev)
	}
	if strings.Join(cfg.ClaudeMdExcludes, " ") != "/home/u/.claude/CLAUDE.md /home/u/.claude/rules/**" {
		t.Errorf("claudeMdExcludes %v", cfg.ClaudeMdExcludes)
	}
	// no setting file but --settings, no MCP; the scrub; --bare only when asked
	a := strings.Join(c.Argv, " ")
	if !strings.Contains(a, " --settings /state/runs/r1/claude.json --setting-sources= --strict-mcp-config ") || strings.Contains(a, "--bare") ||
		strings.Join(c.Env, " ") != "CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1" {
		t.Errorf("argv %v env %v", c.Argv, c.Env)
	}
	bs := spec("claude")
	bs.Bare = true
	if b, _ := Build(bs); !strings.Contains(strings.Join(b.Argv, " "), " --strict-mcp-config --bare ") {
		t.Errorf("bare %v", b.Argv)
	}
	// never a flag that skips permissions; the prompt is last, after --
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

// The run usage from claude's stream: a result line in 2.1.296's shape (its field names checked
// in the binary's strings; built, not recorded: no harness runs here) with two models in
// modelUsage; the usage alone when there is no modelUsage; a failed run's result; others none
func TestUsage(t *testing.T) {
	out, err := os.ReadFile("testdata/claude-result.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	u := UsageOf("claude", out)
	if u != (Usage{Model: "claude-opus-5-5", InputTokens: "206041", OutputTokens: "3390", CostUSD: "1.8423"}) {
		t.Errorf("%+v", u)
	}
	data := map[string]string{}
	u.Data(data, "rule-model")
	if len(data) != 4 || data["model"] != "claude-opus-5-5" || data["cost_usd"] != "1.8423" {
		t.Errorf("%v", data)
	}
	plain := `{"type":"result","subtype":"error_max_budget_usd","is_error":true,"total_cost_usd":2.5e-05,"usage":{"input_tokens":3,"cache_creation_input_tokens":4,"cache_read_input_tokens":5,"output_tokens":6}}`
	if u := UsageOf("claude", []byte(plain)); u != (Usage{InputTokens: "12", OutputTokens: "6", CostUSD: "0.000025"}) {
		t.Errorf("%+v", u)
	}
	data = map[string]string{}
	UsageOf("claude", []byte(plain)).Data(data, "rule-model")
	if data["model"] != "rule-model" {
		t.Errorf("%v", data)
	}
	if u := UsageOf("claude", []byte("no stream\n")); u.Known() {
		t.Errorf("%+v", u)
	}
	if u := UsageOf("opencode", out); u.Known() {
		t.Errorf("opencode: %+v", u)
	}
	data = map[string]string{}
	Usage{}.Data(data, "")
	if len(data) != 0 {
		t.Errorf("%v", data)
	}
}
