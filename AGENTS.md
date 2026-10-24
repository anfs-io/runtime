# runtime: for agents

The toolkit's compiled programs. Today: `anfsd` (Go, `go/`). Plans are in anfs-io/docs
(`units/agents/02-daemon/`, `units/agents/05-spec/`); the event profile is anfs-io/events
(https://github.com/anfs-io/events: SPEC.md, REGISTRY.md, the schema and the conformance examples,
vendored in `go/internal/cloudevent/testdata/spec/`; updating is copying a newer tag's, with its
`VERSION`). The bash side (the packages that install and run anfsd, `anfs bus`) is core's
`packages/{nats,anfsd}`.

## Layout

```
go/go.mod                    module github.com/anfs-io/runtime/go
go/cmd/anfsd                 run | ship --once | rules | match | render | validate | version | help
go/internal/config           anfsd.env (core's settings-file rules), the XDG paths, NATS's port and password
go/internal/cloudevent       the profile (anfs-io/events, anfsv 1): Parse validates an event (an *Error
                             names the spec's rule); Short/FullType/NatsSubject/SubjectParts; Canonical,
                             Noncanonical; Line/Append write one; StampLine (a run's line with its
                             identity), AppendLine, AppendRecorded (.bus.jsonl); conformance_test.go
                             runs the vendored examples through Parse and the schema
go/internal/spool            the month files, complete lines past each cursor, cursors.json; Watch (fsnotify + 30 s)
go/internal/bus              connect (backoff 1/2/5/10/30 s), ensure stream ANFS (max_msg_size: 4095 + the
                             headers), publish with Nats-Msg-Id; testdata: core/nats's layer users
go/internal/shipper          spool -> bus: batches of 256, a token bucket per scope, cursors after the acked prefix
go/internal/space            space.yml's access:, agents:, dispatch: (the one parser: validation, templates,
                             matching, filter subjects); Discover: the installed spaces (wsm ls --json)
go/internal/dispatch         the consumer anfsd-dispatch: rules loaded, events matched, the guards (stale,
                             loop, then rate and queue: held under overflow: delay, up to hold:), one
                             record per rule and event id; Decide (anfsd match); redispatch requests;
                             a space's dispatcher: none
go/internal/supervise        the runs: records (runs/<id>/run.json), the queue, slots per role and host,
                             start (anfs work new/env, setsid, a wrapper that keeps the exit status),
                             timeout, cancel marker, adoption after a restart, run.* events; the
                             run's environment (env.go: the allow-list); its spool (ingest.go); held
                             runs and their rate timers (hold.go)
go/internal/harness          one file per harness: claude, opencode, pi, antigravity (argv, env, files),
                             the summary of a run's output, its usage (usage.go), the text of its stream
go/internal/sink             the consumer anfsd-sink-report: anfs report __sink per report.* event
go/internal/recorder         the consumer anfsd-recorder: what is on the bus and not in a month's files
                             into <scope>/<YYYY-MM>.bus.jsonl, canonical; invalid or mismatched: Term
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
  bytes); anfsd's own events go through `cloudevent.Append` the same way, and what a layer
  publishes to NATS directly the recorder appends to `<YYYY-MM>.bus.jsonl`, so the files stay the
  mirror of everything. Nothing rewrites or deletes a line; the shipper never reads `.bus.jsonl`.
- **The spec is anfs-io/events.** `cloudevent` follows it; a change of the profile is a change
  there first, then a newer tag's examples copied into `testdata/spec/`. Subjects are
  `anfs.<scope>.<type without io.>` for every layer; the short type (`report.filed`,
  `cnfs.x.y`) is what rules and `--type` take, never a subject.
- **Exactly once per event id**: `Nats-Msg-Id` is the CloudEvent id; a cursor moves only over the
  contiguous prefix NATS acknowledged; every consumer is idempotent by event id too.
- **No loops.** Every wait blocks (a channel, a timer, fsnotify); a failure backs off (1, 2, 5,
  10, 30 s). `cmd/anfsd`'s test holds an idle anfsd under 0.2 s of CPU in 10 s.
- One log line per change of state, to stderr (launchd's file, the journal).
- Tests: an embedded server (`internal/testnats`), never a real install; the e2e uses a temp HOME
  and a server on a random port. Never `pkill`; kill by pid.
- Dependencies are pinned in `go.mod`; a new one is a decision in the plan (02b added
  go.yaml.in/yaml/v3 v3.0.5 for space.yml; 05b santhosh-tekuri/jsonschema/v6 v6.0.3, in tests only:
  the spec's schema over its examples).
- **Never a real harness in a test.** Every agent CLI, `anfs` and `wsm` a test runs is
  `internal/testfake`'s, on a PATH of the fakes and the system's (`/usr/bin:/bin`); the e2e bats does
  the same. A harness's flags are checked against `--help` only, by hand.
- **Never the real podman in a test**: `testfake`'s podman (away by default: as a stopped machine),
  and `ANFSD_PODMAN=off` where podman is not the subject (a Linux host's `/usr/bin/podman` is on
  the system PATH). No test starts or stops a container or a podman machine.
- **Podman away is not a loop**: the child is started again only after 5, 10, 30, then 60 s; a
  connection up a minute resets that; `cmd/anfsd`'s idle test runs with podman away throughout.
- **A run writes no shared state** (02d): its events go into its own spool and anfsd stamps them
  into the log (`supervise/ingest.go`, `cloudevent.StampLine`); its environment is the allow-list
  in `supervise/env.go` (a new name passes only by a decision in a plan). `testfake`'s harness and
  anfs find their directories beside `HOME`, since `FAKE_*` do not pass the list either.
- **Held runs never poll**: they leave hold on the scheduler's wake-ups and one timer per rule
  while its rate holds them (`supervise/hold.go`); `cmd/anfsd`'s test holds anfsd under 0.2 s of
  CPU in 10 s with a run held on a rate timer.
- **No spinning on a failing harness**: a run is never restarted but for `retries:` (at most 2,
  30 s then 2 min apart), and `rate:` caps starts per rule; the adopted runs' 10 s check runs only
  while one is adopted.
