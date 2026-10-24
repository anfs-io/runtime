package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPathsAndSettings(t *testing.T) {
	home := t.TempDir()
	env := map[string]string{"HOME": home}
	p, err := PathsFrom(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if p.Events != filepath.Join(home, ".local/state/anfs/events") || p.State != filepath.Join(home, ".local/state/anfsd") ||
		p.NatsState != filepath.Join(home, ".local/state/nats") || p.Config != filepath.Join(home, ".config/anfsd") {
		t.Fatalf("paths: %+v", p)
	}
	env["XDG_STATE_HOME"] = filepath.Join(home, "st")
	p2, _ := PathsFrom(func(k string) string { return env[k] })
	if p2.Events != filepath.Join(home, "st/anfs/events") {
		t.Errorf("XDG_STATE_HOME: %s", p2.Events)
	}

	// shipped file, then the local one wins, then the environment
	os.MkdirAll(p.Config, 0o755)
	os.WriteFile(filepath.Join(p.Config, "anfsd.env"), []byte("# settings\n#ANFSD_SHIP_RATE=100\nANFSD_STREAM_MAX_AGE=30d\nexport ANFSD_SHIP_BURST='50'\nANFSD_DUP_WINDOW=2h\n"), 0o644)
	os.WriteFile(filepath.Join(p.Config, "anfsd.local.env"), []byte("ANFSD_DUP_WINDOW=\"3h\"\nANFSD_STREAM_MAX_BYTES=512MiB\nANFSD_MAX_RUNS=3\nANFSD_PODMAN=off\nANFSD_RUN_EVENTS_MAX=50\n"), 0o644)
	vals, err := ReadSettings(p, func(k string) (string, bool) {
		if k == "ANFSD_SHIP_RATE" {
			return "7", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := ParseSettings(vals)
	if err != nil {
		t.Fatal(err)
	}
	want := Settings{StreamMaxAge: 30 * 24 * time.Hour, StreamMaxBytes: 512 << 20, DupWindow: 3 * time.Hour, ShipRate: 7, ShipBurst: 50, MaxRuns: 3, Podman: false, RunEventsMax: 50}
	if s != want {
		t.Errorf("settings %+v, want %+v", s, want)
	}
	if d := Defaults(); d.StreamMaxAge != 90*24*time.Hour || d.StreamMaxBytes != 1<<30 || d.DupWindow != 24*time.Hour || d.ShipRate != 100 || d.ShipBurst != 1000 || d.MaxRuns != 2 || !d.Podman || d.RunEventsMax != 1000 {
		t.Errorf("defaults %+v", d)
	}
	if _, err := ParseSettings(map[string]string{"ANFSD_STREAM_MAX_BYTES": "lots"}); err == nil {
		t.Error("a bad size parsed")
	}
	if _, err := ParseSettings(map[string]string{"ANFSD_SHIP_RATE": "0"}); err == nil {
		t.Error("a zero rate parsed")
	}
	if _, err := ParseSettings(map[string]string{"ANFSD_MAX_RUNS": "0"}); err == nil {
		t.Error("no runs at all parsed")
	}
	if _, err := ParseSettings(map[string]string{"ANFSD_RUN_EVENTS_MAX": "-1"}); err == nil {
		t.Error("ANFSD_RUN_EVENTS_MAX=-1 parsed")
	}
	if _, err := ParseSettings(map[string]string{"ANFSD_PODMAN": "no"}); err == nil {
		t.Error("ANFSD_PODMAN=no parsed (on or off)")
	}
	if p.Runs != filepath.Join(home, ".local/state/anfsd/runs") {
		t.Errorf("runs %s", p.Runs)
	}
}

func TestReadNats(t *testing.T) {
	home := t.TempDir()
	p, _ := PathsFrom(func(k string) string { return map[string]string{"HOME": home}[k] })
	if _, err := ReadNats(p); err == nil {
		t.Fatal("no port file: no error")
	}
	os.MkdirAll(filepath.Join(p.NatsState, "users"), 0o700)
	os.WriteFile(filepath.Join(p.NatsState, "port"), []byte("47222\n"), 0o600)
	os.WriteFile(filepath.Join(p.NatsState, "users", "anfsd"), []byte("abc123\n"), 0o600)
	n, err := ReadNats(p)
	if err != nil {
		t.Fatal(err)
	}
	if n.URL != "nats://127.0.0.1:47222" || n.User != "anfsd" || n.Password != "abc123" {
		t.Errorf("%+v", n)
	}
}

func TestEnvFileTilde(t *testing.T) {
	d := t.TempDir()
	f := filepath.Join(d, "x.env")
	os.WriteFile(f, []byte("A=~/bin\nB=~\nC=x~/y\nD=$HOME/z\n"), 0o644)
	v, err := ReadEnvFile(f, "/h")
	if err != nil {
		t.Fatal(err)
	}
	if v["A"] != "/h/bin" || v["B"] != "/h" || v["C"] != "x~/y" || v["D"] != "$HOME/z" {
		t.Errorf("%v", v)
	}
}
