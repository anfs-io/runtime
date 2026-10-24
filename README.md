# runtime

Compiled programs of the anfs toolkit. `go/` is one Go module (`github.com/anfs-io/runtime/go`);
its one program is `anfsd`, the anfs daemon.

## anfsd

The events are the profile of **[anfs-io/events](https://github.com/anfs-io/events)** (SPEC.md:
one CloudEvents 1.0 profile for every layer, anfs, cnfs, lgat; its conformance examples are
vendored in `go/internal/cloudevent/testdata/spec/`, `VERSION` naming the commit, and the Go tests
run every one through the parser and the schema).

anfsd is **the shipper**: it tails the event log the tools write
(`~/.local/state/anfs/events/<space id|host>/<YYYY-MM>.jsonl`, one event per line: core's
`lib/anfs/events.sh`, and any layer's producer that appends to the spool) and publishes each line,
as it is, to the NATS JetStream stream `ANFS` (subjects `anfs.<scope>.<type without io.>`:
`anfs.<id>.anfs.report.filed`, `anfs.host.anfs.container.started`,
`anfs.<id>.cnfs.partitia.report.filed`; `Nats-Msg-Id` the event's id, a 24 h duplicate window, so
each event is in the stream once; a body over 4095 bytes is refused by the stream). It is not a
broker: NATS is (core/nats). The files stay the complete, permanent record; the stream is a
bounded window (90 days, 1 GiB) for programs.

And **the recorder**: a layer may also publish to NATS directly (its NATS user, core/nats's
`ANFS_BUS_LAYERS`). The consumer `anfsd-recorder` reads every message and appends each event whose
id is not already in its month's files to `<scope>/<YYYY-MM>.bus.jsonl`, in the canonical form;
the spool's own lines are found and skipped, invalid or mismatched messages are terminated and
logged. The shipper never reads `.bus.jsonl`, so nothing recorded is published again.

It is also **the dispatcher and supervisor** (agents/02b): the `dispatch:` rules of every
installed space (`space.yml`, beside `agents:` and `access:`; core AGENTS.md, "Dispatch") are read
by one parser here, matched against the stream through the durable consumer `anfsd-dispatch`, and
each match is a run: an agent CLI (claude, opencode, pi, antigravity) started in an `anfs work`
job, in its own session, with the access its harness can enforce, its life recorded in
`~/.local/state/anfsd/runs/<run id>/` and as `run.*` events (A2A's task states). And **the report
sink**: the consumer `anfsd-sink-report` (`anfs.*.anfs.report.>`) hands each `report.*` event to
`anfs report __sink`, which renders the report records into the report file. A rule's `on.type` is
the short type (`report.*`; another layer's with its namespace, `cnfs.partitia.*`), its `on.data`
and `{{data.<key>}}` read data's top-level strings (a number or an object never matches and
renders empty). `run.completed` (and `run.failed` when known) carries the run usage: `model`,
`input_tokens`, `output_tokens`, `cost_usd` (claude's stream; the rule's model otherwise).

A run is fenced (agents/02d). Its **environment is an allow-list**: a working shell, the XDG
paths, `ANFS_*`, and claude's own credential for claude; never `SSH_AUTH_SOCK`, `GH_TOKEN` or
anything else anfsd's environment holds; git asks nobody and uses no credential helper. **Its own
events** go into its spool, `runs/<run id>/events/` (`$ANFS_EVENTS_SPOOL`, the one directory of
the run's it may write; core's `anfs_emit` writes there), and anfsd moves each line into the log
with the run's identity stamped (`actor`, `actorspace`, `actorrole`, `actorrun`; a differing claim
kept in `actorclaimed`), refusing `run.*`, a line whose directory is not its scope, and anything
past `ANFSD_RUN_EVENTS_MAX` (1000).
**What a harness loads** is only what anfsd hands it: claude runs with `--setting-sources=` (no
user, project or local settings file: no hooks, `env` or plugins from them), `--strict-mcp-config`
(no MCP server), the person's `~/.claude/CLAUDE.md` excluded, its shell unable to read the
credential files (`sandbox.filesystem.denyRead`) or its own key, and with no network;
`ANFSD_CLAUDE_BARE=on` adds `--bare` for a host that authenticates with an API key. pi gets
`--no-extensions --no-mcp --no-skills --no-prompt-templates --no-approve`; opencode its
`OPENCODE_DISABLE_*` and `OPENCODE_PURE` (its global config has no switch).

**Backpressure delays, with bounds**: a full queue or a spent `rate:` holds a run (submitted, with
`held: rate|queue`; `run.submitted` says so) instead of rejecting it, unless the rule says
`overflow: reject`; at most `hold:` (20) runs per rule, beyond that `run.rejected` (`hold: 20
full`). Held runs leave hold oldest first when the queue has room and the rule's window frees a
start (one timer per rule while it holds runs), survive a restart (they are records), and are
checked against `stale` again when they would start. `rate:` counts runs as they enter the queue.
An event rejected for `queue` or `rate` is unhandled; `anfs agents redispatch` leaves a request in
`~/.local/state/anfsd/redispatch/` and sends SIGHUP: the run skips stale, rate and loop.

**A space can turn anfsd's dispatcher off**: `dispatcher: none` in its `space.yml` (absent:
`anfsd`). Its rules are still parsed and shown (status `off`), never loaded nor in the consumer's
filters; its queued and held runs end `run.canceled by=dispatcher:none` at the reload, working
runs go on, and `redispatch` refuses it. The shipper and the report sink are unaffected.

And **the podman follower** (agents/02c): one child, `podman events --format json --filter
type=container --filter type=pod`, whose container and pod lifecycle (created, started, stopped,
died, removed, paused, unpaused, a change of health; a pod's created, started, stopped, removed)
it writes into the host scope as `container.<verb>` and `pod.<verb>` (actor `podman`, subject the
name, data `id`, `image`, `pod`, `exit`, `health`, `pcm`; the time podman's own). The same container, status and second
is one event, at most 20 a second. Podman away (not installed, the podman machine stopped): the
child is started again after 5, 10, 30, then every 60 s, with `podman.connected` and
`podman.disconnected` around each connection; nothing polls. `ANFSD_PODMAN=off` turns it off.

```
anfsd run          ship, record, dispatch, supervise, follow podman until SIGTERM (launchd or systemd
                   runs it: anfs bus start); SIGHUP reads the rules again (anfs bus reload)
anfsd ship --once  ship what is pending, then exit
anfsd rules [--json] [space.yml...]       the rules and their status (anfs agents rules)
anfsd match [--json] --type T [--subject S] [--data K=V]... [--space S] | --event FILE
                   what the rules would do with that event, starting nothing (anfs agents match)
anfsd render HARNESS                      a run's output as text (anfs agents logs)
anfsd validate FILE...|-                  every line against the profile: FILE:LINE RULE message,
                   a spool file's scope and month too; exit 1 on an invalid line (anfs events validate)
anfsd version
```

It reads `~/.config/anfsd/anfsd.env` and `anfsd.local.env` (the environment wins), NATS's port and
the `anfsd` user's password from `~/.local/state/nats/`, and keeps `~/.local/state/anfsd/`
(`cursors.json`, `anfsd.pid`, `runs/`, `podman.json`). It runs `wsm ls --json`, `anfs work
new|path|env`, `anfs report __sink`, the harnesses and `podman events` from its service's PATH. It is installed and run by core's `core/anfsd` package; a person
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
