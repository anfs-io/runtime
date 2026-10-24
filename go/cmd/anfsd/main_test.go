package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/cloudevent"
	"github.com/anfs-io/runtime/go/internal/recorder"
	"github.com/anfs-io/runtime/go/internal/testfake"
	"github.com/anfs-io/runtime/go/internal/testnats"
)

func TestCommandLine(t *testing.T) {
	var out, errb bytes.Buffer
	if run([]string{"version"}, &out, &errb) != 0 || out.String() != "dev\n" {
		t.Errorf("version: %q", out.String())
	}
	out.Reset()
	if run([]string{"help"}, &out, &errb) != 0 || !strings.Contains(out.String(), "anfsd run") {
		t.Errorf("help: %q", out.String())
	}
	for _, args := range [][]string{nil, {"bogus"}, {"ship"}, {"run", "x"}} {
		if c := run(args, &out, &errb); c != 64 {
			t.Errorf("%v: exit %d", args, c)
		}
	}
}

// A temp HOME as core/nats leaves it: the port and the anfsd user's password in STATE
func home(t *testing.T, srv *testnats.Server, pw string) string {
	h := t.TempDir()
	ns := filepath.Join(h, ".local/state/nats")
	os.MkdirAll(filepath.Join(ns, "users"), 0o700)
	os.WriteFile(filepath.Join(ns, "port"), []byte(strconv.Itoa(srv.Port)+"\n"), 0o600)
	os.WriteFile(filepath.Join(ns, "users/anfsd"), []byte(pw+"\n"), 0o600)
	return h
}

func build(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "anfsd")
	c := exec.Command("go", "build", "-o", bin, ".")
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// the daemon's environment: a temp HOME and the fakes' PATH (a fake wsm, anfs and harnesses:
// no real one can be found)
func env(t *testing.T, h string) (*testfake.Fakes, []string) {
	f := testfake.New(t)
	// a run's environment is an allow-list (02d): the fakes find their directories beside HOME
	for name, d := range map[string]string{"log": f.Log, "ctl": f.Ctl, "work": f.Work} {
		os.Symlink(d, filepath.Join(filepath.Dir(h), name))
	}
	e := []string{"HOME=" + h}
	for _, kv := range f.Env() {
		if !strings.HasPrefix(kv, "HOME=") {
			e = append(e, kv)
		}
	}
	return f, e
}

// anfsd run: ships the backlog, writes its pid; idle, it uses almost no CPU (no loop: the
// 2026-10-23 incident), with podman away the whole time (the fake podman cannot connect, as when
// the podman machine is stopped): started again after 5 s, then 10 s, one log line, no event;
// SIGTERM ends it with exit 0, the cursors written and the pid file gone
func TestRunIdleAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("10 s idle")
	}
	srv := testnats.Start(t, testnats.Users{"anfsd": "pw"})
	h := home(t, srv, "pw")
	events := filepath.Join(h, ".local/state/anfs/events")
	for i := 0; i < 10; i++ {
		if _, err := cloudevent.Append(events, cloudevent.New{Scope: "host", Type: "x.y", Actor: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	bin := build(t)
	cmd := exec.Command(bin, "run")
	f, e := env(t, h)
	cmd.Env = e
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pidf := filepath.Join(h, ".local/state/anfsd/anfsd.pid")
	b, err := bus.Connect(bus.Options{URL: srv.URL(), User: "anfsd", Password: "pw"}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := b.StreamInfo(context.Background())
		if err == nil && info.State.Msgs == 10 {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("not shipped; stderr:\n%s", stderr.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if p, _ := os.ReadFile(pidf); strings.TrimSpace(string(p)) != strconv.Itoa(cmd.Process.Pid) {
		t.Errorf("pid file %q", p)
	}
	// a line appended now is shipped without waiting for the 30 s reconcile
	cloudevent.Append(events, cloudevent.New{Scope: "host", Type: "x.y", Actor: "t"})
	deadline = time.Now().Add(3 * time.Second)
	for {
		info, _ := b.StreamInfo(context.Background())
		if info != nil && info.State.Msgs == 11 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("an append was not shipped within 3 s; stderr:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(10 * time.Second)
	// the recorder read all 11 and found each in the spool: nothing recorded
	rc, err := b.JetStream().Consumer(context.Background(), bus.Stream, recorder.Consumer)
	if err != nil {
		t.Fatalf("no recorder: %v\n%s", err, stderr.String())
	}
	if ci, _ := rc.Info(context.Background()); ci == nil || ci.AckFloor.Stream != 11 || ci.NumAckPending != 0 {
		t.Errorf("recorder %+v", ci)
	}
	if m, _ := filepath.Glob(filepath.Join(events, "*", "*.bus.jsonl")); len(m) != 0 {
		t.Errorf("recorded the spool's lines: %v", m)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	err = cmd.Wait()
	if err != nil {
		t.Fatalf("exit: %v\n%s", err, stderr.String())
	}
	cpu := cmd.ProcessState.UserTime() + cmd.ProcessState.SystemTime()
	t.Logf("CPU over ~11 s: %v", cpu)
	if cpu > 200*time.Millisecond {
		t.Errorf("CPU %v over 10 s idle: a loop?", cpu)
	}
	if _, err := os.Stat(pidf); !os.IsNotExist(err) {
		t.Error("pid file left")
	}
	// podman away: tried at 0 and 5 s (the next at 15 s), never connected, logged once
	if n := f.PodmanStarts(); n < 1 || n > 3 {
		t.Errorf("podman started %d times in ~11 s (backoff 5, 10, 30, 60 s)", n)
	}
	if n := strings.Count(stderr.String(), "podman away: "); n != 1 {
		t.Errorf("%d podman away lines:\n%s", n, stderr.String())
	}
	if st, _ := os.ReadFile(filepath.Join(h, ".local/state/anfsd/podman.json")); !strings.Contains(string(st), `"state":"away"`) {
		t.Errorf("podman.json %s", st)
	}
	c, _ := os.ReadFile(filepath.Join(h, ".local/state/anfsd/cursors.json"))
	if !strings.Contains(string(c), "host/") {
		t.Errorf("cursors %s", c)
	}
	if !strings.Contains(stderr.String(), "anfsd: dev started") || !strings.Contains(stderr.String(), "stopped (11 shipped since start)") {
		t.Errorf("log:\n%s", stderr.String())
	}
}

// ship --once: what is pending, then exit; NATS down is exit 1 within its 10 s
func TestShipOnce(t *testing.T) {
	srv := testnats.Start(t, testnats.Users{"anfsd": "pw"})
	h := home(t, srv, "pw")
	cloudevent.Append(filepath.Join(h, ".local/state/anfs/events"), cloudevent.New{Scope: "host", Type: "x.y", Actor: "t"})
	t.Setenv("HOME", h)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	var out, errb bytes.Buffer
	if c := run([]string{"ship", "--once"}, &out, &errb); c != 0 || out.String() != "1 shipped\n" {
		t.Fatalf("exit %d out %q err %q", c, out.String(), errb.String())
	}
	out.Reset()
	if c := run([]string{"ship", "--once"}, &out, &errb); c != 0 || out.String() != "0 shipped\n" {
		t.Fatalf("again: exit %d out %q", c, out.String())
	}
	// no port file: says what to install
	os.Remove(filepath.Join(h, ".local/state/nats/port"))
	errb.Reset()
	if c := run([]string{"ship", "--once"}, &out, &errb); c != 1 || !strings.Contains(errb.String(), "ppm install core/nats") {
		t.Fatalf("exit %d err %q", c, errb.String())
	}
}

// ANFSD_PODMAN=off (anfsd.local.env): podman is never started, podman.json says off
func TestPodmanOff(t *testing.T) {
	srv := testnats.Start(t, testnats.Users{"anfsd": "pw"})
	h := home(t, srv, "pw")
	os.MkdirAll(filepath.Join(h, ".config/anfsd"), 0o755)
	os.WriteFile(filepath.Join(h, ".config/anfsd/anfsd.local.env"), []byte("ANFSD_PODMAN=off\n"), 0o644)
	bin := build(t)
	cmd := exec.Command(bin, "run")
	f, e := env(t, h)
	cmd.Env = e
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	st := filepath.Join(h, ".local/state/anfsd/podman.json")
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, _ := os.ReadFile(st)
		if strings.Contains(string(b), `"state":"off"`) {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("podman.json %s; stderr:\n%s", b, stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(2500 * time.Millisecond)
	cmd.Process.Signal(syscall.SIGTERM)
	cmd.Wait()
	if n := f.PodmanStarts(); n != 0 {
		t.Errorf("podman started %d times", n)
	}
	if !strings.Contains(stderr.String(), "podman: off (ANFSD_PODMAN=off)") {
		t.Errorf("log:\n%s", stderr.String())
	}
}
