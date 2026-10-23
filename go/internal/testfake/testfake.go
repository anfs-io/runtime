// Package testfake sets up the fakes anfsd's tests run instead of real programs: a fake agent CLI
// linked as claude, opencode, pi and agy (testdata/fake-harness), a fake anfs (testdata/fake-anfs:
// anfs work and anfs report __sink), a fake wsm that prints a fixed `wsm ls --json`, and a fake
// podman (testdata/fake-podman: away unless told to print recorded `podman events` lines). No
// test ever starts a real harness or podman: PATH is the fakes' directory and the system's own.
package testfake

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Fakes is one test's set.
type Fakes struct {
	Bin  string // on PATH first: claude, opencode, pi, agy, anfs, anfs-work, wsm, podman
	Anfs string // the fake anfs
	Log  string // what the fakes record ($FAKE_LOG)
	Ctl  string // what they are told ($FAKE_CTL)
	Work string // the jobs ($FAKE_WORK)
	Home string
}

// Dir is testdata's directory.
func Dir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "testdata")
}

// New makes the fakes in a temp dir.
func New(t testing.TB) *Fakes {
	t.Helper()
	root := t.TempDir()
	f := &Fakes{Bin: filepath.Join(root, "bin"), Log: filepath.Join(root, "log"), Ctl: filepath.Join(root, "ctl"),
		Work: filepath.Join(root, "work"), Home: filepath.Join(root, "home")}
	for _, d := range []string{f.Bin, f.Log, f.Ctl, f.Work, f.Home} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"claude", "opencode", "pi", "agy"} {
		if err := os.Symlink(filepath.Join(Dir(), "fake-harness"), filepath.Join(f.Bin, n)); err != nil {
			t.Fatal(err)
		}
	}
	os.Symlink(filepath.Join(Dir(), "fake-podman"), filepath.Join(f.Bin, "podman"))
	f.Anfs = filepath.Join(f.Bin, "anfs")
	os.Symlink(filepath.Join(Dir(), "fake-anfs"), f.Anfs)
	os.WriteFile(filepath.Join(f.Bin, "anfs-work"), []byte("#!/bin/sh\nexec anfs work \"$@\"\n"), 0o755)
	f.Wsm("[]")
	return f
}

// Wsm sets what the fake wsm prints for `wsm ls --json`.
func (f *Fakes) Wsm(json string) {
	os.WriteFile(filepath.Join(f.Ctl, "wsm.json"), []byte(json+"\n"), 0o644)
	os.WriteFile(filepath.Join(f.Bin, "wsm"), []byte("#!/bin/sh\ncat \""+filepath.Join(f.Ctl, "wsm.json")+"\"\n"), 0o755)
}

// Mode tells the fake harness what to do next (ok, exit N, sleep N, child, fail-once).
func (f *Fakes) Mode(m string) { os.WriteFile(filepath.Join(f.Ctl, "mode"), []byte(m), 0o644) }

// Podman tells the fake podman what to do at its next start (away, run N, run forever) and, with
// lines, what it prints (recorded podman events JSON, one a line).
func (f *Fakes) Podman(mode, lines string) {
	os.WriteFile(filepath.Join(f.Ctl, "podman"), []byte(mode), 0o644)
	if lines != "" {
		os.WriteFile(filepath.Join(f.Ctl, "podman.lines"), []byte(lines), 0o644)
	}
}

// PodmanStarts is how many times the fake podman was started.
func (f *Fakes) PodmanStarts() int {
	b, _ := os.ReadFile(filepath.Join(f.Log, "podman.log"))
	return strings.Count(string(b), "\n")
}

// Touch makes a control file (commit, sink-fail).
func (f *Fakes) Touch(name, content string) {
	os.WriteFile(filepath.Join(f.Ctl, name), []byte(content), 0o644)
}

// Path is PATH with the fakes first and nothing of the person's (no real harness can be found).
func (f *Fakes) Path() string { return f.Bin + ":/usr/bin:/bin:/usr/sbin:/sbin" }

// Env is the environment a supervisor's runs and the fakes get.
func (f *Fakes) Env() []string {
	return []string{
		"PATH=" + f.Path(), "HOME=" + f.Home,
		"FAKE_LOG=" + f.Log, "FAKE_CTL=" + f.Ctl, "FAKE_WORK=" + f.Work,
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "ANFS_EVENTS=off", "ANFS_LOG=off",
	}
}

// Setenv puts the environment into the test process (t.Setenv: restored after).
func (f *Fakes) Setenv(t *testing.T) {
	for _, kv := range f.Env() {
		k, v, _ := strings.Cut(kv, "=")
		t.Setenv(k, v)
	}
}

// LookPath finds a program in the fakes' directory only (validation in tests).
func (f *Fakes) LookPath(p string) (string, error) {
	c := filepath.Join(f.Bin, p)
	if st, err := os.Stat(c); err == nil && !st.IsDir() {
		return c, nil
	}
	return "", os.ErrNotExist
}

// Started is what the fake harness recorded for each start: its directory per pid, oldest first.
type Started struct {
	Name, Dir string
	Argv      []string
	Env       map[string]string
	Stdin     string
	Cwd       string
}

// Starts reads every start of the fake harness.
func (f *Fakes) Starts() []Started {
	ents, _ := os.ReadDir(f.Log)
	var out []Started
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		d := filepath.Join(f.Log, e.Name())
		s := Started{Name: strings.SplitN(e.Name(), ".", 2)[0], Dir: d, Env: map[string]string{}}
		if b, err := os.ReadFile(filepath.Join(d, "argv")); err == nil {
			s.Argv = strings.Split(strings.TrimSuffix(string(b), "\x00"), "\x00")
		}
		if b, err := os.ReadFile(filepath.Join(d, "env")); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if k, v, ok := strings.Cut(l, "="); ok {
					s.Env[k] = v
				}
			}
		}
		b, _ := os.ReadFile(filepath.Join(d, "stdin"))
		s.Stdin = string(b)
		b, _ = os.ReadFile(filepath.Join(d, "cwd"))
		s.Cwd = strings.TrimSpace(string(b))
		out = append(out, s)
	}
	return out
}

// AnfsCalls are the fake anfs's calls, one per line.
func (f *Fakes) AnfsCalls() []string {
	b, _ := os.ReadFile(filepath.Join(f.Log, "anfs.log"))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
