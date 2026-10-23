// anfsd is the anfs daemon. In 02a it is only the shipper: the event log
// ($XDG_STATE_HOME/anfs/events, written by the tools) to NATS JetStream's stream ANFS, each line as
// it is, exactly once per event id. It is not a broker: NATS is (core/nats); anfsd publishes as the
// anfsd user and keeps its cursors in $XDG_STATE_HOME/anfsd.
//
//	anfsd run          ship until SIGTERM (the launchd agent / systemd unit core/anfsd writes)
//	anfsd ship --once  ship what is pending and exit (tests, by hand)
//	anfsd version      the version (a release's tag, or "dev")
//	anfsd help
//
// No HTTP, no RPC: anfs bus status reads anfsd's files and NATS's monitoring port.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/anfs-io/runtime/go/internal/bus"
	"github.com/anfs-io/runtime/go/internal/config"
	"github.com/anfs-io/runtime/go/internal/shipper"
	"github.com/anfs-io/runtime/go/internal/spool"
)

// version is stamped by the release build (-ldflags "-X main.version=v0.1.0")
var version = "dev"

const usage = `anfsd: the anfs daemon (the shipper: the event log to NATS JetStream)

Usage:
  anfsd run          ship until SIGTERM (run by launchd or systemd: anfs bus start)
  anfsd ship --once  ship what is pending, then exit
  anfsd version      print the version
  anfsd help         this

Reads ~/.config/anfsd/anfsd.env and anfsd.local.env (the environment wins), NATS's
port and the anfsd user's password from ~/.local/state/nats; keeps its cursors in
~/.local/state/anfsd/cursors.json. Logs to stderr.
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
	// SIGHUP (anfs bus reload) re-reads the dispatch rules from 02b on; here there is nothing to read
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				logf("reload: nothing to reload yet")
			}
		}
	}()

	w, err := spool.Watch(ctx, cfg.Paths.Events, spool.Reconcile, logf)
	if err != nil {
		logf("watching %s: %v", cfg.Paths.Events, err)
		return 1
	}
	err = sh.Run(ctx, w.C)
	logf("stopped (%d shipped since start)", sh.Shipped())
	if err != nil {
		logf("writing the cursors: %v", err)
		return 1
	}
	return 0
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
