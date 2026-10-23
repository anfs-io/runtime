# runtime: for agents

The toolkit's compiled programs. Today: `anfsd` (Go, `go/`). Plans are in anfs-io/docs
(`units/agents/02-daemon/`); the event profile anfsd reads is `units/agents/01-events/plan.md`
there. The bash side (the packages that install and run anfsd, `anfs bus`) is core's
`packages/{nats,anfsd}`.

## Layout

```
go/go.mod                    module github.com/anfs-io/runtime/go
go/cmd/anfsd                 run | ship --once | rules | match | render | version | help
go/internal/config           anfsd.env (core's settings-file rules), the XDG paths, NATS's port and password
go/internal/cloudevent       the profile (anfsv 1): Parse validates a line; Line/Append write one
go/internal/spool            the month files, complete lines past each cursor, cursors.json; Watch (fsnotify + 30 s)
go/internal/bus              connect (backoff 1/2/5/10/30 s), ensure stream ANFS, publish with Nats-Msg-Id
go/internal/shipper          spool -> bus: batches of 256, a token bucket per scope, cursors after the acked prefix
go/internal/space            space.yml's access:, agents:, dispatch: (the one parser: validation, templates,
                             matching, filter subjects); Discover: the installed spaces (wsm ls --json)
go/internal/dispatch         the consumer anfsd-dispatch: rules loaded, events matched, the guards (stale,
                             rate, queue, loop), one run per rule and event id; Decide (anfsd match)
go/internal/supervise        the runs: records (runs/<id>/run.json), the queue, slots per role and host,
                             start (anfs work new/env, setsid, a wrapper that keeps the exit status),
                             timeout, cancel marker, adoption after a restart, run.* events
go/internal/harness          one file per harness: claude, opencode, pi, antigravity (argv, env, files),
                             the summary of a run's output, the text of its stream
go/internal/sink             the consumer anfsd-sink-report: anfs report __sink per report.* event
go/internal/podman           podman events -> container.*, pod.* in the host scope: kept statuses, health
                             changes only, coalesced per container/status/second, 20/s; the child
                             restarted after 5, 10, 30, 60 s; podman.connected/disconnected; podman.json
                             (testdata/events.jsonl: recorded on a Mac, podman 6.1.1)
go/internal/testnats         an embedded nats-server for tests (restartable, same port and store)
go/internal/testfake         the fakes tests run instead of programs: an agent CLI (as claude, opencode,
                             pi, agy), anfs (work, report __sink), wsm, podman (away, or recorded lines)
go/test/e2e.bats             the real nats-server and the built anfsd in a temp HOME
.github/workflows            ci.yml (push, PR), release.yml (v* tags)
```

## Rules

- **anfsd stays thin.** It moves events between the files and NATS, starts and watches runs,
  and hands report events to core's renderer. It is not a broker, has no HTTP or RPC, and owns no state a person edits: the
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
- Dependencies are pinned in `go.mod`; a new one is a decision in the plan (02b added
  go.yaml.in/yaml/v3 v3.0.5 for space.yml).
- **Never a real harness in a test.** Every agent CLI, `anfs` and `wsm` a test runs is
  `internal/testfake`'s, on a PATH of the fakes and the system's (`/usr/bin:/bin`); the e2e bats does
  the same. A harness's flags are checked against `--help` only, by hand.
- **Never the real podman in a test**: `testfake`'s podman (away by default: as a stopped machine),
  and `ANFSD_PODMAN=off` where podman is not the subject (a Linux host's `/usr/bin/podman` is on
  the system PATH). No test starts or stops a container or a podman machine.
- **Podman away is not a loop**: the child is started again only after 5, 10, 30, then 60 s; a
  connection up a minute resets that; `cmd/anfsd`'s idle test runs with podman away throughout.
- **No spinning on a failing harness**: a run is never restarted but for `retries:` (at most 2,
  30 s then 2 min apart), and `rate:` caps starts per rule; the adopted runs' 10 s check runs only
  while one is adopted.
