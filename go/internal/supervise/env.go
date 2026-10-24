package supervise

import (
	"os"
	"strconv"
	"strings"
)

// The environment of a run is an allow-list (agents/02d, F7): nothing of anfsd's environment
// reaches a run unless it is named here. SSH_AUTH_SOCK, GH_TOKEN, GITHUB_TOKEN, GIT_ASKPASS,
// AWS_* and anything else a host's service manager or person put there stay with anfsd.
//
// This is a convenience the agent could undo if it found the values elsewhere; what enforces is
// claude's sandbox (denyRead, no network, credentials.envVars), see harness/claude.go.

// passNames pass from anfsd's environment when set: a working shell and the toolkit's paths
var passNames = map[string]bool{
	"PATH": true, "HOME": true, "USER": true, "LOGNAME": true, "SHELL": true,
	"LANG": true, "LANGUAGE": true, "TZ": true, "TERM": true, "__CF_USER_TEXT_ENCODING": true,
	"XDG_CONFIG_HOME": true, "XDG_DATA_HOME": true, "XDG_STATE_HOME": true, "XDG_CACHE_HOME": true,
	"XDG_RUNTIME_DIR": true, "XDG_CONFIG_DIRS": true, "XDG_DATA_DIRS": true,
}

// anfsOwn are the ANFS_* names that never pass: anfsd sets them per run, or they would point a
// run's tools at a job's copy (ANFS_WORK)
var anfsOwn = map[string]bool{
	"ANFS_WORK": true, "ANFS_WORK_SAID": true, "ANFS_EVENTS_SPOOL": true,
	"ANFS_ACTOR": true, "ANFS_AS": true, "ANFS_RUN": true, "ANFS_EVENT": true,
}

// harnessCreds are the credentials each harness needs to reach its model; the others read their
// own auth files
var harnessCreds = map[string][]string{
	"claude": {"ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"},
}

// runEnv is the part of anfsd's environment a run of the harness gets ("" for anfsd's own anfs
// calls: no harness credential). ANFSD_* are the daemon's and never pass.
func runEnv(env []string, harness string) []string {
	if env == nil {
		env = os.Environ()
	}
	creds := map[string]bool{}
	for _, k := range harnessCreds[harness] {
		creds[k] = true
	}
	var out []string
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch {
		case passNames[k], strings.HasPrefix(k, "LC_"), creds[k]:
		case strings.HasPrefix(k, "ANFS_") && !anfsOwn[k]:
		default:
			continue
		}
		out = append(out, kv)
	}
	return out
}

// gitNoCredentials makes git ask nobody: no terminal prompt, and every credential helper from the
// system and global config reset (an empty credential.helper clears the list, macOS's osxkeychain
// included). Its index follows any GIT_CONFIG_* the job already sets.
func gitNoCredentials(env []string) []string {
	n := 0
	if v := getEnv(env, "GIT_CONFIG_COUNT"); v != "" {
		if c, err := strconv.Atoi(v); err == nil && c > 0 {
			n = c
		}
	}
	env = setEnv(env, "GIT_TERMINAL_PROMPT", "0")
	env = setEnv(env, "GIT_CONFIG_COUNT", strconv.Itoa(n+1))
	env = setEnv(env, "GIT_CONFIG_KEY_"+strconv.Itoa(n), "credential.helper")
	return setEnv(env, "GIT_CONFIG_VALUE_"+strconv.Itoa(n), "")
}

func getEnv(env []string, k string) string {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, k+"="); ok {
			return v
		}
	}
	return ""
}
