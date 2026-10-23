package space

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Installed is a row of `wsm ls --json`, as far as anfsd needs it.
type Installed struct {
	Space  string  `json:"space"`
	Status *string `json:"status"`
	Dir    *string `json:"dir"`
	ID     *string `json:"id"`
}

// Discover lists the installed spaces with an id (`wsm ls --json`: a tool reads another's list
// with --json) and loads each one's space.yml. wsm is the program (wsm on PATH by default).
func Discover(ctx context.Context, wsm string, env Env) ([]*Space, error) {
	if wsm == "" {
		wsm = "wsm"
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, wsm, "ls", "--json")
	cmd.Env = append(cmd.Environ(), "ANFS_EVENTS=off")
	out, err := cmd.Output()
	if err != nil {
		msg := err.Error()
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			msg = strings.TrimSpace(string(ee.Stderr))
		}
		return nil, fmt.Errorf("wsm ls --json: %s", msg)
	}
	var rows []Installed
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, fmt.Errorf("wsm ls --json: %v", err)
	}
	var spaces []*Space
	for _, r := range rows {
		if r.Dir == nil || r.ID == nil || *r.ID == "" {
			continue
		}
		if r.Status != nil && (*r.Status == "invalid" || *r.Status == "not in the source") {
			continue
		}
		s := Load(filepath.Join(*r.Dir, "space.yml"), r.Space, *r.Dir, env)
		if s.Err == nil && s.ID != *r.ID {
			s.Err = fmt.Errorf("id %s, but wsm says %s", s.ID, *r.ID)
		}
		spaces = append(spaces, s)
	}
	return spaces, nil
}

// Rules is every rule of the spaces, in order.
func Rules(spaces []*Space) []*Rule {
	var out []*Rule
	for _, s := range spaces {
		out = append(out, s.Rules...)
	}
	return out
}
