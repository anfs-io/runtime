# runtime

Compiled programs of the anfs toolkit. `go/` is one Go module (`github.com/anfs-io/runtime/go`);
its one program is `anfsd`, the anfs daemon.

## anfsd

anfsd is **the shipper**: it tails the event log the tools write
(`~/.local/state/anfs/events/<space id>/<YYYY-MM>.jsonl`, one CloudEvent per line: core's
`lib/anfs/events.sh`) and publishes each line, as it is, to the NATS JetStream stream `ANFS`
(subjects `anfs.<space id|host>.<noun>.<verb>`, `Nats-Msg-Id` the event's id, a 24 h duplicate
window, so each event is in the stream once). It is not a broker: NATS is (core/nats). The files
stay the complete, permanent record; the stream is a bounded window (90 days, 1 GiB) for programs.

It is also **the dispatcher and supervisor** (agents/02b): the `dispatch:` rules of every
installed space (`space.yml`, beside `agents:` and `access:`; core AGENTS.md, "Dispatch") are read
by one parser here, matched against the stream through the durable consumer `anfsd-dispatch`, and
each match is a run: an agent CLI (claude, opencode, pi, antigravity) started in an `anfs work`
job, in its own session, with the access its harness can enforce, its life recorded in
`~/.local/state/anfsd/runs/<run id>/` and as `run.*` events (A2A's task states). And **the report
sink**: the consumer `anfsd-sink-report` hands each `report.*` event to `anfs report __sink`, which
renders the report records into the report file.

```
anfsd run          ship, dispatch, supervise until SIGTERM (launchd or systemd runs it: anfs bus
                   start); SIGHUP reads the rules again (anfs bus reload)
anfsd ship --once  ship what is pending, then exit
anfsd rules [--json] [space.yml...]       the rules and their status (anfs agents rules)
anfsd match [--json] --type T [--subject S] [--data K=V]... [--space S] | --event FILE
                   what the rules would do with that event, starting nothing (anfs agents match)
anfsd render HARNESS                      a run's output as text (anfs agents logs)
anfsd version
```

It reads `~/.config/anfsd/anfsd.env` and `anfsd.local.env` (the environment wins), NATS's port and
the `anfsd` user's password from `~/.local/state/nats/`, and keeps `~/.local/state/anfsd/`
(`cursors.json`, `anfsd.pid`, `runs/`). It runs `wsm ls --json`, `anfs work new|path|env`, `anfs
report __sink` and the harnesses from its service's PATH. It is installed and run by core's `core/anfsd` package; a person
uses it through `anfs bus` (status, start, stop, logs).

## Build and test

Go is `stack/go` (the version is `go.mod`'s `go` line).

```bash
cd go
go build -o ~/.local/share/anfsd/dev/anfsd ./cmd/anfsd    # from source: ANFSD_BIN in anfsd.local.env
gofmt -l . && go vet ./... && go test -race ./...           # an embedded nats-server and fakes, nothing else
bats test/e2e.bats </dev/null                               # needs nats-server and nats on PATH
go test ./internal/harness -update                          # rewrite the harnesses' golden files
```

## Release

A `v*` tag builds `anfsd-<tag>-<os>-<arch>.tar.gz` for darwin and linux on arm64 and amd64, and
`SHA256SUMS`, attached to the tag's GitHub release (`.github/workflows/release.yml`). core's
`packages/anfsd/pins.yml` pins the version and the sums (`packages/anfsd/tests/pins --print` in
core prints them).
