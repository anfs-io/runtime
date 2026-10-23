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

func env(h string) []string {
	return []string{"HOME=" + h, "PATH=" + os.Getenv("PATH")}
}

// anfsd run: ships the backlog, writes its pid; idle, it uses almost no CPU (no loop: the
// 2026-10-23 incident); SIGTERM ends it with exit 0, the cursors written and the pid file gone
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
	cmd.Env = env(h)
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
