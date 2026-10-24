// Package testnats runs an embedded nats-server with JetStream for tests: a random port on
// 127.0.0.1, the store in the test's temp dir, no auth unless asked. Stop and Start again keep the
// port and the store (a restart).
package testnats

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Server is one embedded server that can be stopped and started again.
type Server struct {
	t     testing.TB
	opts  server.Options
	srv   *server.Server
	Port  int
	Store string
}

// Users are user/password pairs the server accepts (none: no auth).
type Users map[string]string

// Start runs a server; it is shut down when the test ends.
func Start(t testing.TB, users Users) *Server {
	t.Helper()
	s := &Server{t: t, Store: t.TempDir()}
	s.opts = server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  s.Store,
		NoLog:     true,
		NoSigs:    true,
		HTTPPort:  0,
	}
	for u, p := range users {
		s.opts.Users = append(s.opts.Users, &server.User{Username: u, Password: p})
	}
	s.start()
	s.Port = s.srv.Addr().(*net.TCPAddr).Port
	s.opts.Port = s.Port
	t.Cleanup(s.Stop)
	return s
}

// StartConfig runs a server from a NATS config file's text (its authorization block: users and
// permissions as core/nats renders them) on a random port with the store in the test's temp dir.
func StartConfig(t testing.TB, conf string) *Server {
	t.Helper()
	s := &Server{t: t, Store: t.TempDir()}
	f := filepath.Join(t.TempDir(), "server.conf")
	if err := os.WriteFile(f, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := server.ProcessConfigFile(f)
	if err != nil {
		t.Fatalf("nats config: %v", err)
	}
	o.Host, o.Port, o.JetStream, o.StoreDir, o.NoLog, o.NoSigs, o.HTTPPort = "127.0.0.1", -1, true, s.Store, true, true, 0
	s.opts = *o
	s.start()
	s.Port = s.srv.Addr().(*net.TCPAddr).Port
	s.opts.Port = s.Port
	t.Cleanup(s.Stop)
	return s
}

func (s *Server) start() {
	s.t.Helper()
	o := s.opts
	srv, err := server.NewServer(&o)
	if err != nil {
		s.t.Fatalf("nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		s.t.Fatal("nats-server not ready")
	}
	s.srv = srv
}

// URL is the client URL.
func (s *Server) URL() string { return fmt.Sprintf("nats://127.0.0.1:%d", s.Port) }

// Stop shuts it down (the store stays).
func (s *Server) Stop() {
	if s.srv != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
		s.srv = nil
	}
}

// Restart starts it again on the same port with the same store.
func (s *Server) Restart() {
	s.t.Helper()
	s.Stop()
	s.start()
}

// Running says whether it is up.
func (s *Server) Running() bool { return s.srv != nil }
