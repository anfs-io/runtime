#!/usr/bin/env bats
# anfsd end to end: the real nats-server (the version core/nats pins) on a random port in a temp
# dir, with the users core/nats renders, anfsd built from this tree, and a temp HOME holding a
# spool of fixture lines. Every line is in the stream ANFS once; kill -9 anfsd, append, restart:
# still once each. A dispatch rule (02b) with the fake harness (internal/testfake: never a real
# one) and a fake wsm and anfs on PATH: one report.filed line in the spool is a run, its
# run.submitted, run.working and run.completed in the stream. The fake podman (internal/testfake:
# never the real one) printing a recorded container start: container.started in the stream on
# anfs.host.anfs.container.started. A run's own events (02d): the fake harness emits into
# $ANFS_EVENTS_SPOOL through core's anfs_emit when ANFS_CORE_LIB names a core checkout's lib/anfs
# (else a printf'd line), and they reach the stream stamped with the run's identity; a forged
# actor arrives with actorclaimed; the run's environment lacks what anfsd's had (SSH_AUTH_SOCK,
# GH_TOKEN). The recorder (05b): cnfs's direct publish, as core/nats's layer user, lands in
# <scope>/<YYYY-MM>.bus.jsonl, the spool's lines do not. The other tests turn podman off (ANFSD_PODMAN=off), so a Linux
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
    {
      user: cnfs, password: cnfspw
      permissions: {
        subscribe: {allow: ["anfs.>", "_INBOX.>"]}
        publish: {allow: ["anfs.*.cnfs.>", "\$JS.API.INFO", "\$JS.API.STREAM.INFO.ANFS", "\$JS.API.CONSUMER.*.ANFS.>",
          "\$JS.API.CONSUMER.DURABLE.CREATE.ANFS.>", "\$JS.API.CONSUMER.MSG.NEXT.ANFS.>", "\$JS.ACK.ANFS.>"]}
      }
    }
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

# Now, to the second, for a line written now: T the time attribute, M its month, P the id's first
# 13 characters (its milliseconds: the profile's time.id)
now() {
  local s
  s=$(date -u +%s)
  T=$(date -u -r "$s" +%Y-%m-%dT%H:%M:%S.000Z 2>/dev/null || date -u -d "@$s" +%Y-%m-%dT%H:%M:%S.000Z)
  M=${T:0:7}
  P=$(printf '%08x-%04x' $(( s * 1000 >> 16 )) $(( s * 1000 & 65535 )))
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
  [[ "$output" == *'"anfs.host.anfs.report.filed": 20'* ]]
  [[ "$output" == *'"anfs.abcde23456.anfs.report.filed": 5'* ]]
  grep -q 'not shipped, not an event: host/2026-10.jsonl at byte' "$BATS_TEST_TMPDIR/anfsd.log"
  [ "$(cat "$HOME/.local/state/anfsd/anfsd.pid")" = "$ANFSD_PID" ]
  # the stream as core/nats's settings make it
  run nats stream info ANFS --json
  [ "$(yq -p json -oy -r '.config.max_age' <<< "$output")" = 7776000000000000 ]
  [ "$(yq -p json -oy -r '.config.max_bytes' <<< "$output")" = 1073741824 ]
  [ "$(yq -p json -oy -r '.config.duplicate_window' <<< "$output")" = 86400000000000 ]
}

@test "the recorder: cnfs's direct publish is in <scope>/<month>.bus.jsonl once; the spool's lines are not; anfs's subjects are refused to cnfs" {
  spool host 1 3
  start_anfsd
  wait_msgs 3
  local ex="$BATS_TEST_DIRNAME/../internal/cloudevent/testdata/spec/examples/valid" body id
  body=$(< "$ex/cnfs-partitia-report-filed.jsonl")
  id=$(yq -p json -oy -r '.id' <<< "$body")
  for i in 1 2; do
    nats --user cnfs --password cnfspw pub -J -H "Nats-Msg-Id:$id" -H "Content-Type:application/cloudevents+json" \
      anfs.m2vq7cfr4k.cnfs.partitia.report.filed --force-stdin <<< "$body" >/dev/null
  done
  run nats --user cnfs --password cnfspw pub -J anfs.abcde23456.anfs.report.filed '{}'
  [ "$status" -ne 0 ]
  wait_msgs 4
  local rec="$EV/m2vq7cfr4k/2026-10.bus.jsonl"
  for i in $(seq 50); do [[ -s "$rec" ]] && break; sleep 0.1; done
  [ "$(cat "$rec")" = "$body" ] || { cat "$rec"; cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  # the spool's own lines: known, never recorded
  sleep 0.5
  [ ! -e "$EV/host/2026-10.bus.jsonl" ]
  run nats consumer info ANFS anfsd-recorder --json
  [ "$(yq -p json -oy -r '.ack_floor.stream_seq' <<< "$output")" = 4 ]
  [ "$(yq -p json -oy -r '.config.deliver_policy' <<< "$output")" = all ]
  # the stream refuses a body over 4095 bytes with the two headers
  [ "$(nats stream info ANFS --json | yq -p json -oy -r '.config.max_msg_size')" = 4202 ]
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
  now
  printf '{"specversion":"1.0","id":"%s-7000-8000-%012x","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.filed","subject":"r-20261023-9","time":"%s","datacontenttype":"application/json","actor":"shell:e2e","anfsv":1,"data":{"package":"core/pcm","title":"t","to":"file"}}\n' \
    "$P" 99 "$T" >> "$EV/abcde23456/$M.jsonl"
  for i in $(seq 150); do
    [[ "$(nats stream subjects ANFS --json 2>/dev/null)" == *'"anfs.abcde23456.anfs.run.completed": 1'* ]] && break
    sleep 0.2
  done
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.abcde23456.anfs.run.submitted": 1'* ]] || { cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  [[ "$output" == *'"anfs.abcde23456.anfs.run.working": 1'* ]]
  [[ "$output" == *'"anfs.abcde23456.anfs.run.completed": 1'* ]]
  # the fake harness ran in the job with its actor; the commit counted
  grep -q '^ANFS_AS=anfs-dev/maintainer$' "$BATS_TEST_TMPDIR"/log/claude.*/env
  grep -q 'work new fix-r-20261023-9 core' "$BATS_TEST_TMPDIR/log/anfs.log"
  run nats stream get ANFS --last-for anfs.abcde23456.anfs.run.completed --json
  [[ "$(yq -p json -oy -r '.data' <<< "$output" | base64 --decode)" == *'"commits":"core:1"'* ]]
}

# Every message's data (the event), one per line
stream_events() {
  local n seq
  n=$(msgs)
  for (( seq = 1; seq <= n; seq++ )); do
    nats stream get ANFS "$seq" --json | yq -p json -oy -r '.data' | base64 --decode
    echo
  done
}

@test "a run's own events: stamped with its identity on the bus; a forged actor marked; its environment the allow-list" {
  local fakes="$BATS_TEST_DIRNAME/../internal/testfake/testdata" bin="$BATS_TEST_TMPDIR/bin" sp="$HOME/spaces/anfs-dev" i
  mkdir -p "$bin" "$sp" "$BATS_TEST_TMPDIR/log" "$BATS_TEST_TMPDIR/ctl" "$BATS_TEST_TMPDIR/work"
  ln -s "$fakes/fake-harness" "$bin/claude"
  ln -s "$fakes/fake-anfs" "$bin/anfs"
  ln -s "$(command -v bash)" "$bin/bash"     # core's lib needs bash 5; nothing else of the person's
  printf '#!/bin/sh\necho %s\n' "'[{\"space\":\"anfs-dev\",\"status\":\"ok\",\"dir\":\"$sp\",\"id\":\"abcde23456\"}]'" > "$bin/wsm"
  chmod +x "$bin/wsm"
  cat > "$sp/space.yml" <<YML
id: abcde23456
agents:
  maintainer: {harness: claude, concurrency: 3}
dispatch:
  - name: on-report
    on: {type: report.filed}
    as: maintainer
    job: none
    prompt: 'Look at {{subject}}.'
YML
  local how=printf
  if [[ -n "${ANFS_CORE_LIB:-}" ]]; then
    how=anfs_emit
    mkdir -p "$HOME/.local/lib"
    ln -s "$ANFS_CORE_LIB" "$HOME/.local/lib/anfs"
  fi
  # by the trigger's subject: -1 emits as itself, -2 as the person, -3 prints its environment
  cat > "$BATS_TEST_TMPDIR/ctl/script" <<SH
subj=\$(sed -n 's/.*"subject":"\([^"]*\)".*/\1/p' "\$ANFS_EVENT")
emit() {
  if [ $how = anfs_emit ]; then
    bash -c 'source "\$HOME/.local/lib/anfs/paths.sh"; source "\$ANFS_LIB_DIR/log.sh"; source "\$ANFS_LIB_DIR/events.sh"; anfs_emit report.closed "\$1" to=file' _ "\$subj"
  else
    mkdir -p "\$ANFS_EVENTS_SPOOL/abcde23456"
    s=\$(date -u +%s)
    t=\$(date -u -r "\$s" +%Y-%m-%dT%H:%M:%S.000Z 2>/dev/null || date -u -d "@\$s" +%Y-%m-%dT%H:%M:%S.000Z)
    p=\$(printf '%08x-%04x' \$(( s * 1000 / 65536 )) \$(( s * 1000 % 65536 )))
    printf '{"specversion":"1.0","id":"%s-7000-8000-%012d","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.closed","subject":"%s","time":"%s","datacontenttype":"application/json","actor":"%s","anfsv":1,"data":{"to":"file"}}\n' \
      "\$p" "\${subj##*-}" "\$subj" "\$t" "\$ANFS_ACTOR" >> "\$ANFS_EVENTS_SPOOL/abcde23456/\$(echo "\$t" | cut -c1-7).jsonl"
  fi
}
case "\$subj" in
  *-1) emit ;;
  *-2) ANFS_ACTOR=shell:person ANFS_AS=anfs-dev/person emit ;;
  *-3) env > "\$FAKE_LOG/run-env" ;;
esac
SH
  echo script > "$BATS_TEST_TMPDIR/ctl/mode"
  SSH_AUTH_SOCK=/tmp/agent.sock GH_TOKEN=gh-secret ANTHROPIC_API_KEY=sk-test ANFSD_PODMAN=off PATH="$bin:/usr/bin:/bin" \
    "$ANFSD" run 2>>"$BATS_TEST_TMPDIR/anfsd.log" &
  ANFSD_PID=$!
  for i in $(seq 100); do
    nats consumer info ANFS anfsd-dispatch --json >/dev/null 2>&1 && break
    sleep 0.1
  done
  now
  for i in 1 2 3; do
    printf '{"specversion":"1.0","id":"%s-7000-8000-%012x","source":"/anfs/spaces/abcde23456","type":"io.anfs.report.filed","subject":"r-20261024-%d","time":"%s","datacontenttype":"application/json","actor":"shell:e2e","anfsv":1,"data":{"to":"file"}}\n' \
      "$P" "$(( 90 + i ))" "$i" "$T" >> "$EV/abcde23456/$M.jsonl"
  done
  for i in $(seq 150); do
    [[ "$(nats stream subjects ANFS --json 2>/dev/null)" == *'"anfs.abcde23456.anfs.run.completed": 3'* ]] && break
    sleep 0.2
  done
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.abcde23456.anfs.run.completed": 3'* ]] || { cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  [[ "$output" == *'"anfs.abcde23456.anfs.report.closed": 2'* ]] || { echo "$output"; cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  local evs honest forged
  evs=$(stream_events)
  honest=$(grep '"io.anfs.report.closed"' <<< "$evs" | grep '"subject":"r-20261024-1"')
  forged=$(grep '"io.anfs.report.closed"' <<< "$evs" | grep '"subject":"r-20261024-2"')
  # stamped: the run's actor, its space and role, actorrun; no claim
  [[ "$(yq -p json -oy -r '.actor' <<< "$honest")" == claude:???????? ]] || { echo "$honest"; false; }
  [ "$(yq -p json -oy -r '.actorspace + " " + .actorrole' <<< "$honest")" = "abcde23456 maintainer" ]
  [[ "$(yq -p json -oy -r '.actorrun' <<< "$honest")" == *"$(yq -p json -oy -r '.actor' <<< "$honest" | cut -d: -f2)" ]]
  [ "$(yq -p json -oy -r '.actorclaimed // "none"' <<< "$honest")" = none ]
  # forged: the stamp is the identity, the claim kept
  [[ "$(yq -p json -oy -r '.actor' <<< "$forged")" == claude:???????? ]]
  [[ -n "$(yq -p json -oy -r '.actorrun' <<< "$forged")" ]]
  if [[ "$how" == anfs_emit ]]; then
    [ "$(yq -p json -oy -r '.actorclaimed' <<< "$forged")" = "shell:person as abcde23456/person" ] || { echo "$forged"; false; }
  else
    [ "$(yq -p json -oy -r '.actorclaimed' <<< "$forged")" = "shell:person" ] || { echo "$forged"; false; }
  fi
  echo "emitted with: $how" >&3
  # the third run's environment: none of anfsd's secrets, its own spool, claude's key (claude only)
  local envf="$BATS_TEST_TMPDIR/log/run-env"
  [ -f "$envf" ]
  ! grep -q '^SSH_AUTH_SOCK=' "$envf"
  ! grep -q '^GH_TOKEN=' "$envf"
  grep -q '^ANFS_EVENTS_SPOOL=.*/runs/.*/events$' "$envf"
  grep -q '^GIT_TERMINAL_PROMPT=0$' "$envf"
}

@test "podman: a container started is container.started in the stream, on anfs.host.anfs.container.started" {
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
    [[ "$(nats stream subjects ANFS --json 2>/dev/null)" == *'"anfs.host.anfs.container.removed": 1'* ]] && break
    sleep 0.1
  done
  run nats stream subjects ANFS --json
  [[ "$output" == *'"anfs.host.anfs.podman.connected": 1'* ]] || { cat "$BATS_TEST_TMPDIR/anfsd.log"; false; }
  [[ "$output" == *'"anfs.host.anfs.container.started": 1'* ]]
  [[ "$output" == *'"anfs.host.anfs.container.created": 1'* ]]
  [[ "$output" == *'"anfs.host.anfs.container.health": 2'* ]]
  [[ "$output" == *'"anfs.host.anfs.container.died": 1'* ]]
  [[ "$output" != *exec* && "$output" != *sync* ]]
  run nats stream get ANFS --last-for anfs.host.anfs.container.started --json
  local ev
  ev=$(yq -p json -oy -r '.data' <<< "$output" | base64 --decode)
  [ "$(yq -p json -oy -r '.subject' <<< "$ev")" = glitchtip_web_1 ]
  [ "$(yq -p json -oy -r '.actor' <<< "$ev")" = podman ]
  [ "$(yq -p json -oy -r '.data.pcm' <<< "$ev")" = glitchtip ]
  [ "$(yq -p json -oy -r '.data.id' <<< "$ev")" = 2f820df40bb2 ]
  grep -q '"state":"connected"' "$HOME/.local/state/anfsd/podman.json"
}
