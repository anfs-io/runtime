// Package config is anfsd's configuration: the XDG paths it reads and writes, its settings
// (anfsd.local.env, then anfsd.env; the environment wins), and how it reaches NATS (the port and
// the anfsd user's password that core/nats keeps in its STATE).
//
// The settings files are read as core's lib/anfs/paths.sh reads them (anfs_conf_read): NAME=value
// lines, blanks and # comments skipped, a leading "export " allowed, one pair of matching quotes
// stripped, and one expansion: a value that is exactly ~, or starts with ~/, has that ~ replaced
// by $HOME. Nothing else is expanded and nothing is run.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Paths are where anfsd's files are, from HOME and the XDG variables.
type Paths struct {
	Home      string
	Config    string // $XDG_CONFIG_HOME/anfsd
	State     string // $XDG_STATE_HOME/anfsd: cursors.json, anfsd.pid, anfsd.log (macOS), runs/
	Runs      string // $XDG_STATE_HOME/anfsd/runs: one directory per run (02b)
	Events    string // $XDG_STATE_HOME/anfs/events: the spool (lib/anfs/events.sh writes it)
	NatsState string // $XDG_STATE_HOME/nats: port, users/anfsd (core/nats writes them)
}

// Settings are anfsd.env's values.
type Settings struct {
	StreamMaxAge   time.Duration // ANFSD_STREAM_MAX_AGE (90d)
	StreamMaxBytes int64         // ANFSD_STREAM_MAX_BYTES (1GiB)
	DupWindow      time.Duration // ANFSD_DUP_WINDOW (24h)
	ShipRate       float64       // ANFSD_SHIP_RATE (100 events/s per scope)
	ShipBurst      int           // ANFSD_SHIP_BURST (1000)
	MaxRuns        int           // ANFSD_MAX_RUNS (2): runs at once on this host
}

// Nats is how anfsd connects: the URL from core/nats's port file, the anfsd user and its password.
type Nats struct {
	URL      string
	User     string
	Password string
}

// Config is all of it.
type Config struct {
	Paths    Paths
	Settings Settings
	Nats     Nats
}

// Defaults are the built-in values (anfsd.env documents them).
func Defaults() Settings {
	return Settings{
		StreamMaxAge:   90 * 24 * time.Hour,
		StreamMaxBytes: 1 << 30,
		DupWindow:      24 * time.Hour,
		ShipRate:       100,
		ShipBurst:      1000,
		MaxRuns:        2,
	}
}

// PathsFrom computes the paths from an environment lookup.
func PathsFrom(getenv func(string) string) (Paths, error) {
	home := getenv("HOME")
	if home == "" {
		return Paths{}, errors.New("HOME is not set")
	}
	xdg := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return filepath.Join(home, def)
	}
	cfg := xdg("XDG_CONFIG_HOME", ".config")
	state := xdg("XDG_STATE_HOME", ".local/state")
	p := Paths{
		Home:      home,
		Config:    filepath.Join(cfg, "anfsd"),
		State:     filepath.Join(state, "anfsd"),
		Events:    filepath.Join(state, "anfs", "events"),
		NatsState: filepath.Join(state, "nats"),
	}
	if v := getenv("ANFSD_CONFIG_HOME"); v != "" {
		p.Config = v
	}
	if v := getenv("ANFSD_STATE_HOME"); v != "" {
		p.State = v
	}
	p.Runs = filepath.Join(p.State, "runs")
	return p, nil
}

// Load reads everything: the paths, the settings files under the environment, and NATS's port and
// the anfsd password. A missing port or password file is an error that names the file.
func Load() (*Config, error) {
	p, err := PathsFrom(os.Getenv)
	if err != nil {
		return nil, err
	}
	vals, err := ReadSettings(p, os.LookupEnv)
	if err != nil {
		return nil, err
	}
	s, err := ParseSettings(vals)
	if err != nil {
		return nil, err
	}
	n, err := ReadNats(p)
	if err != nil {
		return nil, err
	}
	return &Config{Paths: p, Settings: s, Nats: n}, nil
}

// ReadSettings returns the ANFSD_* values in effect: the environment, then anfsd.local.env, then
// anfsd.env. A file that is missing is empty.
func ReadSettings(p Paths, lookup func(string) (string, bool)) (map[string]string, error) {
	out := map[string]string{}
	for _, f := range []string{filepath.Join(p.Config, "anfsd.local.env"), filepath.Join(p.Config, "anfsd.env")} {
		vals, err := ReadEnvFile(f, p.Home)
		if err != nil {
			return nil, err
		}
		for k, v := range vals {
			if _, ok := out[k]; !ok {
				out[k] = v
			}
		}
	}
	for _, k := range settingNames {
		if v, ok := lookup(k); ok {
			out[k] = expandTilde(v, p.Home)
		}
	}
	return out, nil
}

var settingNames = []string{
	"ANFSD_STREAM_MAX_AGE", "ANFSD_STREAM_MAX_BYTES", "ANFSD_DUP_WINDOW", "ANFSD_SHIP_RATE", "ANFSD_SHIP_BURST", "ANFSD_MAX_RUNS",
}

var nameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ReadEnvFile reads one NAME=value file (see the package comment). A missing file is empty.
func ReadEnvFile(path, home string) (map[string]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		name, val, ok := strings.Cut(line, "=")
		if !ok || !nameRE.MatchString(name) {
			continue
		}
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		}
		if _, seen := out[name]; !seen {
			out[name] = expandTilde(val, home)
		}
	}
	return out, sc.Err()
}

func expandTilde(v, home string) string {
	if v == "~" {
		return home
	}
	if strings.HasPrefix(v, "~/") {
		return home + v[1:]
	}
	return v
}

// ParseSettings turns the values into Settings over the defaults; a value that does not parse is
// an error naming it.
func ParseSettings(vals map[string]string) (Settings, error) {
	s := Defaults()
	var err error
	if v := vals["ANFSD_STREAM_MAX_AGE"]; v != "" {
		if s.StreamMaxAge, err = ParseDuration(v); err != nil {
			return s, fmt.Errorf("ANFSD_STREAM_MAX_AGE=%s: %w", v, err)
		}
	}
	if v := vals["ANFSD_STREAM_MAX_BYTES"]; v != "" {
		if s.StreamMaxBytes, err = ParseSize(v); err != nil {
			return s, fmt.Errorf("ANFSD_STREAM_MAX_BYTES=%s: %w", v, err)
		}
	}
	if v := vals["ANFSD_DUP_WINDOW"]; v != "" {
		if s.DupWindow, err = ParseDuration(v); err != nil {
			return s, fmt.Errorf("ANFSD_DUP_WINDOW=%s: %w", v, err)
		}
	}
	if v := vals["ANFSD_SHIP_RATE"]; v != "" {
		if s.ShipRate, err = strconv.ParseFloat(v, 64); err != nil || s.ShipRate <= 0 {
			return s, fmt.Errorf("ANFSD_SHIP_RATE=%s: a positive number of events a second", v)
		}
	}
	if v := vals["ANFSD_SHIP_BURST"]; v != "" {
		if s.ShipBurst, err = strconv.Atoi(v); err != nil || s.ShipBurst < 1 {
			return s, fmt.Errorf("ANFSD_SHIP_BURST=%s: a positive whole number", v)
		}
	}
	if v := vals["ANFSD_MAX_RUNS"]; v != "" {
		if s.MaxRuns, err = strconv.Atoi(v); err != nil || s.MaxRuns < 1 {
			return s, fmt.Errorf("ANFSD_MAX_RUNS=%s: a positive whole number", v)
		}
	}
	return s, nil
}

// LoadLocal is Load without NATS: the paths and the settings (anfsd rules, match).
func LoadLocal() (*Config, error) {
	p, err := PathsFrom(os.Getenv)
	if err != nil {
		return nil, err
	}
	vals, err := ReadSettings(p, os.LookupEnv)
	if err != nil {
		return nil, err
	}
	s, err := ParseSettings(vals)
	if err != nil {
		return nil, err
	}
	return &Config{Paths: p, Settings: s}, nil
}

var daysRE = regexp.MustCompile(`^([0-9]+)d$`)

// ParseDuration takes Go's durations (24h, 90m) and days (90d).
func ParseDuration(v string) (time.Duration, error) {
	if m := daysRE.FindStringSubmatch(v); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, errors.New("a duration: 90d, 24h, 30m")
	}
	return d, nil
}

var sizeRE = regexp.MustCompile(`^([0-9]+)(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?$`)

// ParseSize takes bytes with an optional unit: 1GiB, 512MiB, 2GB, 1048576.
func ParseSize(v string) (int64, error) {
	m := sizeRE.FindStringSubmatch(v)
	if m == nil {
		return 0, errors.New("a size: 1GiB, 512MiB, 2GB, or bytes")
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, err
	}
	mult := map[string]int64{"": 1, "B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}[m[2]]
	return n * mult, nil
}

// ReadNats reads core/nats's STATE: port (the client port) and users/anfsd (the password).
func ReadNats(p Paths) (Nats, error) {
	port, err := readTrim(filepath.Join(p.NatsState, "port"))
	if err != nil {
		return Nats{}, fmt.Errorf("NATS's port: %w (ppm install core/nats)", err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return Nats{}, fmt.Errorf("NATS's port file %s holds %q, not a port", filepath.Join(p.NatsState, "port"), port)
	}
	pw, err := readTrim(filepath.Join(p.NatsState, "users", "anfsd"))
	if err != nil {
		return Nats{}, fmt.Errorf("the anfsd user's password: %w (ppm install core/nats)", err)
	}
	return Nats{URL: "nats://127.0.0.1:" + port, User: "anfsd", Password: pw}, nil
}

func readTrim(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
