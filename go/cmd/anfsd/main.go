// anfsd is the anfs daemon: the shipper (the event log, $XDG_STATE_HOME/anfs/events, written by
// the tools, to NATS JetStream's stream ANFS, each line as it is, exactly once per event id), the
// dispatcher (the dispatch rules in the installed spaces' space.yml, on the durable consumer
// anfsd-dispatch), the supervisor (each run a child in its own session, recorded in
// $XDG_STATE_HOME/anfsd/runs), and the report sink (anfsd-sink-report: anfs report __sink renders
// the report records into the report file). It is not a broker: NATS is (core/nats).
//
//	anfsd run          all of it until SIGTERM (the launchd agent / systemd unit core/anfsd writes);
//	                   SIGHUP (anfs bus reload) reads the rules again
//	anfsd ship --once  ship what is pending and exit (tests, by hand)
//	anfsd rules        the rules and their status (anfs agents rules reads --json)
//	anfsd match        what the rules would do with an event, starting nothing (anfs agents match)
//	anfsd version      the version (a release's tag, or "dev")
//	anfsd help
//
// No HTTP, no RPC: anfs bus status and anfs agents read anfsd's files and NATS's monitoring port.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/config"
	"github.com/anfs-io/runtime/go/internal/dispatch"
	"github.com/anfs-io/runtime/go/internal/podman"
	"github.com/anfs-io/runtime/go/internal/shipper"
	"github.com/anfs-io/runtime/go/internal/sink"
	"github.com/anfs-io/runtime/go/internal/space"
	"github.com/anfs-io/runtime/go/internal/spool"
	"github.com/anfs-io/runtime/go/internal/supervise"
)

// version is stamped by the release build (-ldflags "-X main.version=v0.1.0")
var version = "dev"

const usage = `anfsd: the anfs daemon (the event log to NATS JetStream; dispatch rules; their runs;
podman's container and pod events)

Usage:
  anfsd run          ship, dispatch, supervise and follow podman events until SIGTERM
                     (run by launchd or systemd: anfs bus start); SIGHUP reads the
                     rules again
  anfsd ship --once  ship what is pending, then exit
  anfsd rules [--json] [space.yml...]
                     every rule of the installed spaces (wsm ls --json), or of the
                     files named: RULE STATUS SPACE ON AS HARNESS (ok, invalid,
                     disabled, unenforced, partial); the reason in --json
  anfsd match [--json] (--event FILE|- | --type T [--subject S] [--data K=V]...
              [--space SPACE]) [space.yml...]
                     what each rule would do with the event: no match (and why),
                     submit (the job, the harness's command line) or reject (why).
                     Starts nothing and writes nothing; disabled rules are shown too
  anfsd render HARNESS
                     a run's out.log on stdin as text (anfs agents logs)
  anfsd version      print the version
  anfsd help         this

Reads ~/.config/anfsd/anfsd.env and anfsd.local.env (the environment wins), NATS's
port and the anfsd user's password from ~/.local/state/nats; keeps its cursors in
~/.local/state/anfsd/cursors.json, its runs in ~/.local/state/anfsd/runs/ and
podman's state in ~/.local/state/anfsd/podman.json.
Logs to stderr.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	logf := func(format string, a ...any) {
		fmt.Fprintf(stderr, "%s anfsd: %s\n", time.Now().UTC().Format("2006-01-02T15:04:05Z"), fmt.Sprintf(format, a...))
	}
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 64
	}
	switch args[0] {
	case "version", "--version":
		fmt.Fprintln(stdout, version)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "run":
		if len(args) != 1 {
			fmt.Fprint(stderr, usage)
			return 64
		}
		return daemon(logf)
	case "ship":
		if len(args) != 2 || args[1] != "--once" {
			fmt.Fprint(stderr, usage)
			return 64
		}
		return shipOnce(stdout, logf)
	case "rules":
		return rulesCmd(args[1:], stdout, stderr)
	case "match":
		return matchCmd(args[1:], os.Stdin, stdout, stderr)
	case "render":
		if len(args) != 2 {
			fmt.Fprint(stderr, usage)
			return 64
		}
		return renderCmd(args[1], os.Stdin, stdout)
	default:
		fmt.Fprintf(stderr, "anfsd: unknown command %q\n%s", args[0], usage)
		return 64
	}
}

func setup(logf func(string, ...any)) (*config.Config, *spool.Spool, *bus.Bus, *shipper.Shipper, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sp, err := spool.Open(cfg.Paths.Events, cfg.Paths.State, logf)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	b, err := bus.Connect(bus.Options{URL: cfg.Nats.URL, User: cfg.Nats.User, Password: cfg.Nats.Password}, logf)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sh := &shipper.Shipper{
		Spool: sp,
		Bus:   b,
		Stream: bus.StreamSettings{
			MaxAge:    cfg.Settings.StreamMaxAge,
			MaxBytes:  cfg.Settings.StreamMaxBytes,
			DupWindow: cfg.Settings.DupWindow,
		},
		Rate:   cfg.Settings.ShipRate,
		Burst:  cfg.Settings.ShipBurst,
		Window: bus.MaxPending,
		Logf:   logf,
	}
	return cfg, sp, b, sh, nil
}

func daemon(logf func(string, ...any)) int {
	cfg, _, b, sh, err := setup(logf)
	if err != nil {
		logf("%v", err)
		return 1
	}
	defer b.Close()
	pid := filepath.Join(cfg.Paths.State, "anfsd.pid")
	if err := os.MkdirAll(cfg.Paths.State, 0o700); err == nil {
		_ = os.WriteFile(pid, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600)
	}
	defer os.Remove(pid)
	logf("%s started (pid %d): %s to stream %s", version, os.Getpid(), cfg.Paths.Events, bus.Stream)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	sup := &supervise.Supervisor{
		Store:   supervise.Store{Dir: cfg.Paths.Runs},
		Events:  cfg.Paths.Events,
		MaxRuns: cfg.Settings.MaxRuns,
		Logf:    logf,

		RunEventsMax: cfg.Settings.RunEventsMax,
	}
	if err := sup.Start(ctx); err != nil {
		logf("runs: %v", err)
		return 1
	}
	d := &dispatch.Dispatcher{Sup: sup, Load: discover, Logf: logf}
	d.Reload(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); d.Run(ctx, b) }()
	go func() { defer wg.Done(); (&sink.Sink{Logf: logf}).Run(ctx, b) }()
	pw := &podman.Watcher{Events: cfg.Paths.Events, StateFile: filepath.Join(cfg.Paths.State, "podman.json"), Logf: logf}
	if cfg.Settings.Podman {
		wg.Add(1)
		go func() { defer wg.Done(); pw.Run(ctx) }()
	} else {
		logf("podman: off (ANFSD_PODMAN=off)")
		pw.Off()
	}

	// SIGHUP (anfs bus reload): the rules again
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				logf("reload: reading the rules again")
				d.AskReload()
			}
		}
	}()

	w, err := spool.Watch(ctx, cfg.Paths.Events, spool.Reconcile, logf)
	if err != nil {
		logf("watching %s: %v", cfg.Paths.Events, err)
		stop()
		wg.Wait()
		sup.Wait()
		return 1
	}
	err = sh.Run(ctx, w.C)
	stop()
	wg.Wait()
	sup.Wait()
	logf("stopped (%d shipped since start)", sh.Shipped())
	if err != nil {
		logf("writing the cursors: %v", err)
		return 1
	}
	return 0
}

// The rules' host: HOME, the environment, PATH lookups
func validationEnv() space.Env {
	return space.Env{Home: os.Getenv("HOME"), Getenv: os.Getenv, LookPath: exec.LookPath}
}

func discover(ctx context.Context) ([]*space.Space, error) {
	return space.Discover(ctx, "wsm", validationEnv())
}

func shipOnce(stdout io.Writer, logf func(string, ...any)) int {
	_, sp, b, sh, err := setup(logf)
	if err != nil {
		logf("%v", err)
		return 1
	}
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.WaitConnected(ctx); err != nil {
		logf("NATS is not reachable: %v", err)
		return 1
	}
	n, err := sh.Drain(context.Background())
	if serr := sp.Save(); serr != nil && err == nil {
		err = serr
	}
	fmt.Fprintf(stdout, "%d shipped\n", n)
	if err != nil {
		logf("%v", err)
		return 1
	}
	return 0
}
