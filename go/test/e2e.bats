#!/usr/bin/env bats
# anfsd end to end: the real nats-server (the version core/nats pins) on a random port in a temp
# dir, with the users core/nats renders, anfsd built from this tree, and a temp HOME holding a
# spool of fixture lines. Every line is in the stream ANFS once; kill -9 anfsd, append, restart:
# still once each. A dispatch rule (02b) with the fake harness (internal/testfake: never a real
# one) and a fake wsm and anfs on PATH: one report.filed line in the spool is a run, its
# run.submitted, run.working and run.completed in the stream. The fake podman (internal/testfake:
# never the real one) printing a recorded container start: container.started in the stream on
# anfs.host.container.started. The other tests turn podman off (ANFSD_PODMAN=off), so a Linux
# host's /usr/bin/podman is never run. Skipped, saying so, when nats-server or the nats CLI is not
# on PATH.
# Run: bats go/test/e2e.bats </dev/null   (from the repo's root; CI puts the pinned binaries on PATH)

bats_require_minimum_version 1.5.0

setup_file() {
  command -v nats-server >/dev/null && command -v nats >/dev/null || return 0
  ROOT="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
  export ANFSD="$BATS_FILE_TMPDIR/anfsd"
  (cd "$ROOT" && go build -o "$ANFSD" ./cmd/anfsd)
}

setup() {
  command -v nats-server >/dev/null || skip "no nats-server on PATH (the pinned v2.15.1: core/nats's pins.yml)"
  command -v nats >/dev/null || skip "no nats CLI on PATH (the pinned v0.5.0: core/nats's pins.yml)"
  export HOME="$BATS_TEST_TMPDIR/home"
  unset XDG_CONFIG_HOME XDG_STATE_HOME XDG_DATA_HOME XDG_CACHE_HOME
  NS="$HOME/.local/state/nats"
  EV="$HOME/.local/state/anfs/events"
  mkdir -p "$NS/users" "$NS/jetstream" "$EV/host" "$EV/abcde23456" "$BATS_TEST_TMPDIR/ports"
  chmod 700 "$NS"
  printf 'anfsdpw\n' > "$NS/users/anfsd"
  printf 'adminpw\n' > "$NS/users/admin"
  cat > "$NS/server.conf" <<CONF
port: -1
host: 127.0.0.1
server_name: e2e
jetstream { store_dir: "$NS/jetstream" }
authorization {
  users: [
    {user: anfsd, password: anfsdpw}
    {user: admin, password: adminpw}
  ]
}
CONF
  nats-server -c "$NS/server.conf" --ports_file_dir "$BATS_TEST_TMPDIR/ports" 2>"$BATS_TEST_TMPDIR/nats.log" &
  NATS_PID=$!
  local i url=""
  for i in $(seq 50); do
    f=("$BATS_TEST_TMPDIR"/ports/*.ports)
    [[ -s "${f[0]}" ]] && url=$(yq -p json -oy -r '.nats[0]' "${f[0]}") && [[ "$url" == nats://* ]] && break
    sleep 0.1
  done
  [[ "$url" == nats://* ]] || { cat "$BATS_TEST_TMPDIR/nats.log"; false; }
  echo "${url##*:}" > "$NS/port"
  export NATS_URL="$url" NATS_USER=admin NATS_PASSWORD=adminpw
  ANFSD_PID=""
}

teardown() {
  [[ -z "${ANFSD_PID:-}" ]] || kill "$ANFSD_PID" 2>/dev/null || true
  [[ -z "${NATS_PID:-}" ]] || { kill "$NATS_PID" 2>/dev/null; wait "$NATS_PID" 2>/dev/null; } || true
}

# A valid line of the profile with id ...-<n> (UUIDv7: version 7, variant 10)
line() {
  local scope="$1" n="$2" src="/anfs/host"
  [[ "$scope" == host ]] || src="/anfs/spaces/$scope"
  printf '{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-%012x","source":"%s","type":"io.anfs.report.filed","subject":"r-%d","time":"2026-10-23T17:46:18.939Z","datacontenttype":"application/json","actor":"shell:e2e","anfsv":1,"data":{"n":"%d"}}\n' \
    "$n" "$src" "$n" "$n"
}

spool() {
  local scope="$1" from="$2" to="$3" i
  for (( i = from; i <= to; i++ )); do line "$scope" "$i" >> "$EV/$scope/2026-10.jsonl"; done
}

# anfsd with the system's PATH only: no wsm, anfs or harness of the person's is ever run; podman off
start_anfsd() {
  ANFSD_PODMAN=off PATH=/usr/bin:/bin "$ANFSD" run 2>>"$BATS_TEST_TMPDIR/anfsd.log" &
  ANFSD_PID=$!
}

msgs() { nats stream info ANFS --json 2>/dev/null | yq -p json -oy -r '.state.messages'; }

wait_msgs() {
  local i
  for i in $(seq 100); do
    [[ "$(msgs)" == "$1" ]] && return 0
    sleep 0.1
  done
  echo "messages: $(msgs), wanted $1"; cat "$BATS_TEST_TMPDIR/anfsd.log"
  return 1
}

# Every message's Nats-Msg-Id, one per line, sorted
ids() {
  local n seq
  n=$(msgs)
  for (( seq = 1; seq <= n; seq++ )); do
    nats stream get ANFS "$seq" --json | yq -p json -oy -r '.hdrs' | base64 --decode | tr -d '\r' | sed -n 's/^Nats-Msg-Id: //p'
  done | sort
}

@test "every spooled line is in the stream once, on anfs.<scope>.<type>, with its id" {
  spool host 1 20
  spool abcde23456 21 25
  printf 'not an event\n' >> "$EV/host/2026-10.jsonl"
  start_anfsd
  wait_msgs 25
  run ids
  [ "$(printf '%s\n' "$output" | sort -u | wc -l | tr -d ' ')" = 25 ]
  [ "$(printf '%s\n' "$output" | head -1)" = "01a169de-e9bb-7000-8000-000000000001" ]
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.host.report.filed": 20'* ]]
  [[ "$output" == *'"anfs.abcde23456.report.filed": 5'* ]]
  grep -q 'not shipped, not an event: host/2026-10.jsonl at byte' "$BATS_TEST_TMPDIR/anfsd.log"
  [ "$(cat "$HOME/.local/state/anfsd/anfsd.pid")" = "$ANFSD_PID" ]
  # the stream as core/nats's settings make it
  run nats stream info ANFS --json
  [ "$(yq -p json -oy -r '.config.max_age' <<< "$output")" = 7776000000000000 ]
  [ "$(yq -p json -oy -r '.config.max_bytes' <<< "$output")" = 1073741824 ]
  [ "$(yq -p json -oy -r '.config.duplicate_window' <<< "$output")" = 86400000000000 ]
}

@test "kill -9, append, restart: still once each" {
  spool host 1 30
  start_anfsd
  wait_msgs 30
  kill -9 "$ANFSD_PID"
  wait "$ANFSD_PID" 2>/dev/null || true
  spool host 31 40
  # a crash between an ack and the cursor's write: the cursor behind, some lines sent again
  printf '{"host/2026-10.jsonl":0}\n' > "$HOME/.local/state/anfsd/cursors.json"
  start_anfsd
  wait_msgs 40
  sleep 1
  [ "$(msgs)" = 40 ]
  run ids
  [ "$(printf '%s\n' "$output" | sort -u | wc -l | tr -d ' ')" = 40 ]
}

@test "SIGTERM: exit 0, the cursors at the end of the files, the pid file gone" {
  spool host 1 5
  start_anfsd
  wait_msgs 5
  kill -TERM "$ANFSD_PID"
  local rc=0
  wait "$ANFSD_PID" || rc=$?
  [ "$rc" -eq 0 ]
  ANFSD_PID=""
  [ "$(cat "$HOME/.local/state/anfsd/cursors.json")" = "{\"host/2026-10.jsonl\":$(wc -c < "$EV/host/2026-10.jsonl" | tr -d ' ')}" ]
  [ ! -e "$HOME/.local/state/anfsd/anfsd.pid" ]
}

@test "dispatch: a report.filed line in the spool is a run, its run.* in the stream" {
  local fakes="$BATS_TEST_DIRNAME/../internal/testfake/testdata" bin="$BATS_TEST_TMPDIR/bin" sp="$HOME/spaces/anfs-dev"
  mkdir -p "$bin" "$sp/prompts" "$BATS_TEST_TMPDIR/log" "$BATS_TEST_TMPDIR/ctl" "$BATS_TEST_TMPDIR/work"
  ln -s "$fakes/fake-harness" "$bin/claude"
  ln -s "$fakes/fake-anfs" "$bin/anfs"
  printf '#!/bin/sh\nexec anfs work "$@"\n' > "$bin/anfs-work"
  printf '#!/bin/sh\necho %s\n' "'[{\"space\":\"anfs-dev\",\"status\":\"ok\",\"dir\":\"$sp\",\"id\":\"abcde23456\"}]'" > "$bin/wsm"
  chmod +x "$bin/anfs-work" "$bin/wsm"
  cat > "$sp/space.yml" <<YML
id: abcde23456
agents:
  maintainer: {harness: claude}
dispatch:
  - name: fix-core-report
    on: {type: report.filed, data: {to: file, package: "core/*"}}
    as: maintainer
    job: {name: "fix-{{subject}}", repos: [core]}
    prompt_file: prompts/report-filed.md
YML
  echo 'Fix {{subject}}.' > "$sp/prompts/report-filed.md"
  : > "$BATS_TEST_TMPDIR/ctl/commit"
  # the fakes keep their records beside HOME (log/, ctl/, work/: FAKE_* do not pass a run's
  # allow-list); anfsd's PATH holds only the fakes and the system
  GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 ANFSD_PODMAN=off PATH="$bin:/usr/bin:/bin" \
    "$ANFSD" run 2>>"$BATS_TEST_TMPDIR/anfsd.log" &
  ANFSD_PID=$!
  local i
  for i in $(seq 100); do
    nats consumer info ANFS anfsd-dispatch --json >/dev/null 2>&1 && break
    sleep 0.1
  done
  printf '{"specversion":"1.0","id":"01a169de-e9bb-7000-8000-%012x","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.filed","subject":"r-20261023-9","time":"%s","datacontenttype":"application/json","actor":"shell:e2e","anfsv":1,"data":{"package":"core/pcm","title":"t","to":"file"}}\n' \
    99 "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" >> "$EV/abcde23456/$(date -u +%Y-%m).jsonl"
  for i in $(seq 150); do
    [[ "$(nats stream subjects ANFS --json 2>/dev/null)" == *'"anfs.abcde23456.run.completed": 1'* ]] && break
    sleep 0.2
  done
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.abcde23456.run.submitted": 1'* ]] || { cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  [[ "$output" == *'"anfs.abcde23456.run.working": 1'* ]]
  [[ "$output" == *'"anfs.abcde23456.run.completed": 1'* ]]
  # the fake harness ran in the job with its actor; the commit counted
  grep -q '^ANFS_AS=anfs-dev/maintainer$' "$BATS_TEST_TMPDIR"/log/claude.*/env
  grep -q 'work new fix-r-20261023-9 core' "$BATS_TEST_TMPDIR/log/anfs.log"
  run nats stream get ANFS --last-for anfs.abcde23456.run.completed --json
  [[ "$(yq -p json -oy -r '.data' <<< "$output" | base64 --decode)" == *'"commits":"core:1"'* ]]
}

@test "podman: a container started is container.started in the stream, on anfs.host.container.started" {
  local fakes="$BATS_TEST_DIRNAME/../internal/testfake/testdata" bin="$BATS_TEST_TMPDIR/bin" i
  mkdir -p "$bin" "$BATS_TEST_TMPDIR/flog" "$BATS_TEST_TMPDIR/fctl"
  ln -s "$fakes/fake-podman" "$bin/podman"
  printf '#!/bin/sh\necho "[]"\n' > "$bin/wsm"
  chmod +x "$bin/wsm"
  echo "run forever" > "$BATS_TEST_TMPDIR/fctl/podman"
  # the recorded glitchtip lifecycle: its start, and the noise around it
  grep '"Name":"glitchtip_web_1"' "$BATS_TEST_DIRNAME/../internal/podman/testdata/events.jsonl" > "$BATS_TEST_TMPDIR/fctl/podman.lines"
  FAKE_LOG="$BATS_TEST_TMPDIR/flog" FAKE_CTL="$BATS_TEST_TMPDIR/fctl" PATH="$bin:/usr/bin:/bin" \
    "$ANFSD" run 2>>"$BATS_TEST_TMPDIR/anfsd.log" &
  ANFSD_PID=$!
  for i in $(seq 100); do
    [[ "$(nats stream subjects ANFS --json 2>/dev/null)" == *'"anfs.host.container.removed": 1'* ]] && break
    sleep 0.1
  done
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.host.podman.connected": 1'* ]] || { cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  [[ "$output" == *'"anfs.host.container.started": 1'* ]]
  [[ "$output" == *'"anfs.host.container.created": 1'* ]]
  [[ "$output" == *'"anfs.host.container.health": 2'* ]]
  [[ "$output" == *'"anfs.host.container.died": 1'* ]]
  [[ "$output" != *exec* && "$output" != *sync* ]]
  run nats stream get ANFS --last-for anfs.host.container.started --json
  local ev
  ev=$(yq -p json -oy -r '.data' <<< "$output" | base64 --decode)
  [ "$(yq -p json -oy -r '.subject' <<< "$ev")" = glitchtip_web_1 ]
  [ "$(yq -p json -oy -r '.actor' <<< "$ev")" = podman ]
  [ "$(yq -p json -oy -r '.data.pcm' <<< "$ev")" = glitchtip ]
  [ "$(yq -p json -oy -r '.data.id' <<< "$ev")" = 2f820df40bb2 ]
  grep -q '"state":"connected"' "$HOME/.local/state/anfsd/podman.json"
}
