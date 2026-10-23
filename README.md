# runtime

Compiled programs of the anfs toolkit. `go/` is one Go module (`github.com/anfs-io/runtime/go`);
its one program is `anfsd`, the anfs daemon.

## anfsd

In this release anfsd is only **the shipper**: it tails the event log the tools write
(`~/.local/state/anfs/events/<space id>/<YYYY-MM>.jsonl`, one CloudEvent per line: core's
`lib/anfs/events.sh`) and publishes each line, as it is, to the NATS JetStream stream `ANFS`
(subjects `anfs.<space id|host>.<noun>.<verb>`, `Nats-Msg-Id` the event's id, a 24 h duplicate
window, so each event is in the stream once). It is not a broker: NATS is (core/nats). The files
stay the complete, permanent record; the stream is a bounded window (90 days, 1 GiB) for programs.

```
anfsd run          ship until SIGTERM (launchd or systemd runs it: anfs bus start)
anfsd ship --once  ship what is pending, then exit
anfsd version
```

It reads `~/.config/anfsd/anfsd.env` and `anfsd.local.env` (the environment wins), NATS's port and
the `anfsd` user's password from `~/.local/state/nats/`, and keeps `~/.local/state/anfsd/`
(`cursors.json`, `anfsd.pid`). It is installed and run by core's `core/anfsd` package; a person
uses it through `anfs bus` (status, start, stop, logs).

## Build and test

Go is `stack/go` (the version is `go.mod`'s `go` line).

```bash
cd go
go build -o ~/.local/share/anfsd/dev/anfsd ./cmd/anfsd    # from source: ANFSD_BIN in anfsd.local.env
gofmt -l . && go vet ./... && go test -race ./...           # an embedded nats-server, nothing else
bats test/e2e.bats </dev/null                               # needs nats-server and nats on PATH
```

## Release

A `v*` tag builds `anfsd-<tag>-<os>-<arch>.tar.gz` for darwin and linux on arm64 and amd64, and
`SHA256SUMS`, attached to the tag's GitHub release (`.github/workflows/release.yml`). core's
`packages/anfsd/pins.yml` pins the version and the sums (`packages/anfsd/tests/pins --print` in
core prints them).
