package runtime

import (
	"errors"
	"fmt"
	"github.com/pithecene/quarry/internal/testutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

func TestProxyHash(t *testing.T) {
	t.Parallel()

	user := "admin"
	pass1 := "secret1"
	pass2 := "secret2"

	t.Run("nil returns empty", func(t *testing.T) {
		if h := proxyHash(nil); h != "" {
			t.Errorf("expected empty, got %q", h)
		}
	})

	tests := []struct {
		name  string
		a, b  *types.ProxyEndpoint
		equal bool
	}{
		{
			name:  "deterministic",
			a:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080},
			b:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080},
			equal: true,
		},
		{
			name:  "different hosts differ",
			a:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy1.example.com", Port: 8080},
			b:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy2.example.com", Port: 8080},
			equal: false,
		},
		{
			name:  "username matters",
			a:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080, Username: &user},
			b:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080},
			equal: false,
		},
		{
			name:  "password excluded",
			a:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080, Username: &user, Password: &pass1},
			b:     &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080, Username: &user, Password: &pass2},
			equal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ha, hb := proxyHash(tt.a), proxyHash(tt.b)
			if tt.equal && ha != hb {
				t.Errorf("expected equal hashes: %q != %q", ha, hb)
			}
			if !tt.equal && ha == hb {
				t.Errorf("expected different hashes: %q == %q", ha, hb)
			}
		})
	}
}

func TestDiscoveryDir_Fallback(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("XDG_RUNTIME_DIR", "")

	dir, err := discoveryDir()
	if err != nil {
		t.Fatalf("discoveryDir failed: %v", err)
	}
	if dir == "" {
		t.Fatal("expected non-empty discovery dir")
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("discovery dir does not exist: %v", err)
	}
	if !info.IsDir() {
		t.Error("discovery dir is not a directory")
	}
}

func TestDiscoveryDir_XDG(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	tmpDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", tmpDir)

	dir, err := discoveryDir()
	if err != nil {
		t.Fatalf("discoveryDir failed: %v", err)
	}

	expected := filepath.Join(tmpDir, "quarry", "browser-reuse")
	if dir != expected {
		t.Errorf("expected %q, got %q", expected, dir)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("discovery dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("discovery dir is not a directory")
	}
}

func TestHealthCheck_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
	}{
		{"invalid URL", "not-a-url"},
		{"unreachable port", "ws://127.0.0.1:19999/devtools/browser/test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := HealthCheckBrowser(tt.endpoint); err == nil {
				t.Error("expected error")
			}
		})
	}
}

// The browser server outlives the run that launches it, so its stderr must
// be the log file, never the run's own stderr: a reader of the run's output
// would otherwise wait for the server's idle shutdown to see end of file.
func TestStartBrowserServer_StderrGoesToLog(t *testing.T) {
	testutil.Serial(t, "writes and executes a file, which a fork in a parallel test can hold open (ETXTBSY)")
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("/proc unavailable")
	}

	dir := t.TempDir()
	cfg := fakeExecutor(t, dir, "echo 'browser server diagnostics' >&2\necho ready\nexec sleep 60\n")
	logPath := filepath.Join(dir, browserServerLogName)

	pid, err := startBrowserServer(t.Context(), cfg, dir, filepath.Join(dir, leaseSocketName), logPath)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })

	stderrTarget, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/2", pid))
	if err != nil {
		t.Fatalf("read server stderr: %v", err)
	}
	if stderrTarget != logPath {
		t.Errorf("server stderr = %q, want the log %q", stderrTarget, logPath)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "browser server diagnostics") {
		t.Errorf("log = %q, want the server's stderr", data)
	}
}

// A server that fails to start is the runtime's own child, so the runtime
// stops it; it must not be left running.
func TestStartBrowserServer_StopsAServerThatFailsToListen(t *testing.T) {
	testutil.Serial(t, "writes and executes a file, which a fork in a parallel test can hold open (ETXTBSY)")

	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	cfg := fakeExecutor(t, dir, "echo $$ > "+pidFile+"\necho 'not ready'\nexec sleep 60\n")

	_, err := startBrowserServer(t.Context(), cfg, dir, filepath.Join(dir, leaseSocketName), filepath.Join(dir, browserServerLogName))
	if err == nil || !strings.Contains(err.Error(), "unexpected browser server output") {
		t.Fatalf("err = %v, want unexpected output", err)
	}

	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse pid: %v", err)
	}
	// startBrowserServer reaped it, so signal 0 finds no process.
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("server still running after a failed start: kill(0) = %v", err)
	}
}

// fakeExecutor writes a shell script that stands in for the executor and
// returns a config that runs it.
func fakeExecutor(t *testing.T, dir, body string) ReusableBrowserConfig {
	t.Helper()
	fake := filepath.Join(dir, "fake-executor")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatalf("write fake executor: %v", err)
	}
	return ReusableBrowserConfig{ExecutorPath: fake, ScriptPath: "script.ts", IdleTimeout: time.Minute}
}

func TestIdleTimeoutFromEnv(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	tests := []struct {
		name     string
		envVal   string
		expected time.Duration
	}{
		{"unset", "", 0},
		{"valid 30", "30", 30 * time.Second},
		{"valid 120", "120", 120 * time.Second},
		{"invalid string", "abc", 0},
		{"zero", "0", 0},
		{"negative", "-1", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("QUARRY_BROWSER_IDLE_TIMEOUT", tt.envVal)
			if got := IdleTimeoutFromEnv(); got != tt.expected {
				t.Errorf("IdleTimeoutFromEnv() = %v, want %v", got, tt.expected)
			}
		})
	}
}
