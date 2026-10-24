package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeLeaseServer listens on a socket in a short temporary directory and
// answers each connection with handle.
func fakeLeaseServer(t *testing.T, handle func(conn net.Conn)) string {
	t.Helper()
	path := shortSocketPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(conn)
		}
	}()
	return path
}

// shortSocketPath returns a socket path under os.TempDir: t.TempDir can
// exceed the socket path limit.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ql")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, leaseSocketName)
}

// replyWith answers the request with reply, then holds the connection
// until the client closes it, reporting the close on closed.
func replyWith(reply string, closed chan<- struct{}) func(net.Conn) {
	return func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		if _, err := r.ReadBytes('\n'); err != nil {
			return
		}
		_, _ = conn.Write([]byte(reply + "\n"))
		_, _ = r.ReadBytes('\n') // returns when the client closes
		if closed != nil {
			close(closed)
		}
	}
}

func TestRequestLease(t *testing.T) {
	t.Parallel()

	grant := `{"ws_endpoint":"ws://127.0.0.1:1/devtools/browser/x","pid":42}`

	t.Run("grant", func(t *testing.T) {
		var got leaseRequest
		path := fakeLeaseServer(t, func(conn net.Conn) {
			line, _ := bufio.NewReader(conn).ReadBytes('\n')
			_ = json.Unmarshal(line, &got)
			_, _ = conn.Write([]byte(grant + "\n"))
		})

		lease, err := requestLease(t.Context(), path, "sha256:abc")
		if err != nil {
			t.Fatalf("requestLease: %v", err)
		}
		if lease.WSEndpoint != "ws://127.0.0.1:1/devtools/browser/x" || lease.ServerPID != 42 {
			t.Errorf("lease = %+v", lease)
		}
		if got.ProxyHash != "sha256:abc" {
			t.Errorf("sent proxy hash %q", got.ProxyHash)
		}
	})

	t.Run("proxy mismatch", func(t *testing.T) {
		path := fakeLeaseServer(t, replyWith(`{"refused":"proxy mismatch"}`, nil))

		if _, err := requestLease(t.Context(), path, ""); !errors.Is(err, errProxyMismatch) {
			t.Errorf("err = %v, want errProxyMismatch", err)
		}
	})

	t.Run("other refusal is not a missing server", func(t *testing.T) {
		path := fakeLeaseServer(t, replyWith(`{"refused":"malformed request"}`, nil))

		_, err := requestLease(t.Context(), path, "")
		if err == nil || errors.Is(err, errNoServer) || errors.Is(err, errProxyMismatch) {
			t.Errorf("err = %v, want a plain refusal", err)
		}
	})

	noServer := []struct {
		name string
		path func(t *testing.T) string
	}{
		{"missing socket", shortSocketPath},
		{"stale socket file", func(t *testing.T) string {
			path := shortSocketPath(t)
			ln, err := net.Listen("unix", path)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			// Keep the file, as a killed server does, but stop listening.
			ln.(*net.UnixListener).SetUnlinkOnClose(false)
			_ = ln.Close()
			return path
		}},
		{"closed without a reply", func(t *testing.T) string {
			return fakeLeaseServer(t, func(conn net.Conn) { _ = conn.Close() })
		}},
	}
	for _, tt := range noServer {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := requestLease(t.Context(), tt.path(t), ""); !errors.Is(err, errNoServer) {
				t.Errorf("err = %v, want errNoServer", err)
			}
		})
	}

	t.Run("no reply in time leaves the server alone", func(t *testing.T) {
		hold := make(chan struct{})
		t.Cleanup(func() { close(hold) })
		path := fakeLeaseServer(t, func(conn net.Conn) {
			<-hold
			_ = conn.Close()
		})

		_, err := requestLease(t.Context(), path, "")
		if err == nil || errors.Is(err, errNoServer) || !strings.Contains(err.Error(), "no reply within") {
			t.Errorf("err = %v, want a timeout that is not errNoServer", err)
		}
	})

	t.Run("socket path over the limit", func(t *testing.T) {
		long := "/" + strings.Repeat("x", maxSocketPathLen)
		_, err := requestLease(t.Context(), long, "")
		if err == nil || errors.Is(err, errNoServer) {
			t.Errorf("err = %v, want a path error that is not errNoServer", err)
		}
	})
}

// The connection is the lease: it stays open until the run's context ends.
func TestRequestLease_HeldUntilContextEnds(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	path := fakeLeaseServer(t, replyWith(`{"ws_endpoint":"ws://127.0.0.1:1/devtools/browser/x","pid":1}`, closed))

	ctx, end := context.WithCancel(t.Context())
	if _, err := requestLease(ctx, path, ""); err != nil {
		t.Fatalf("requestLease: %v", err)
	}

	select {
	case <-closed:
		t.Fatal("lease closed while the run's context was live")
	case <-time.After(200 * time.Millisecond):
	}

	end()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("lease still open after the run's context ended")
	}
}
