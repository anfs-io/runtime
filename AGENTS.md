# runtime: for agents

The toolkit's compiled programs. Today: `anfsd` (Go, `go/`). Plans are in anfs-io/docs
(`units/agents/02-daemon/`); the event profile anfsd reads is `units/agents/01-events/plan.md`
there. The bash side (the packages that install and run anfsd, `anfs bus`) is core's
`packages/{nats,anfsd}`.

## Layout

```
go/go.mod                    module github.com/anfs-io/runtime/go
go/cmd/anfsd                 run | ship --once | version | help
go/internal/config           anfsd.env (core's settings-file rules), the XDG paths, NATS's port and password
go/internal/cloudevent       the profile (anfsv 1): Parse validates a line; Line/Append write one
go/internal/spool            the month files, complete lines past each cursor, cursors.json; Watch (fsnotify + 30 s)
go/internal/bus              connect (backoff 1/2/5/10/30 s), ensure stream ANFS, publish with Nats-Msg-Id
go/internal/shipper          spool -> bus: batches of 256, a token bucket per scope, cursors after the acked prefix
go/internal/testnats         an embedded nats-server for tests (restartable, same port and store)
go/test/e2e.bats             the real nats-server and the built anfsd in a temp HOME
.github/workflows            ci.yml (push, PR), release.yml (v* tags)
```

## Rules

- **anfsd stays thin.** It moves events between the files and NATS and (from 02b) starts and
  watches runs. It is not a broker, has no HTTP or RPC, and owns no state a person edits: the
  files under `~/.local/state/anfsd/` and the stream are its only outputs. `anfs bus status`
  reads them and NATS's monitoring port.
- **The files are the record.** Tools only append lines (one `write(2)`, `O_APPEND`, at most 4096
  bytes); anfsd's own events go through `cloudevent.Append` the same way, so the files stay the
  mirror of everything. Nothing rewrites or deletes a line.
- **Exactly once per event id**: `Nats-Msg-Id` is the CloudEvent id; a cursor moves only over the
  contiguous prefix NATS acknowledged; every consumer is idempotent by event id too.
- **No loops.** Every wait blocks (a channel, a timer, fsnotify); a failure backs off (1, 2, 5,
  10, 30 s). `cmd/anfsd`'s test holds an idle anfsd under 0.2 s of CPU in 10 s.
- One log line per change of state, to stderr (launchd's file, the journal).
- Tests: an embedded server (`internal/testnats`), never a real install; the e2e uses a temp HOME
  and a server on a random port. Never `pkill`; kill by pid.
- Dependencies are pinned in `go.mod`; a new one is a decision in the plan.
