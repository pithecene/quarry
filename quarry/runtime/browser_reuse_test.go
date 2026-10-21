package runtime

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

func TestProxyHash(t *testing.T) {
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
	tmpDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", tmpDir)

	dir, err := discoveryDir()
	if err != nil {
		t.Fatalf("discoveryDir failed: %v", err)
	}

	expected := filepath.Join(tmpDir, "quarry")
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

func TestReadWriteDiscovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "browser.json")

	disc := &BrowserDiscovery{
		WSEndpoint: "ws://127.0.0.1:9222/devtools/browser/abc123",
		PID:        12345,
		ProxyHash:  "sha256:deadbeef",
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	if err := writeDiscovery(path, disc); err != nil {
		t.Fatalf("writeDiscovery: %v", err)
	}

	got, err := readDiscovery(path)
	if err != nil {
		t.Fatalf("readDiscovery: %v", err)
	}

	if got.WSEndpoint != disc.WSEndpoint {
		t.Errorf("WSEndpoint: got %q, want %q", got.WSEndpoint, disc.WSEndpoint)
	}
	if got.PID != disc.PID {
		t.Errorf("PID: got %d, want %d", got.PID, disc.PID)
	}
	if got.ProxyHash != disc.ProxyHash {
		t.Errorf("ProxyHash: got %q, want %q", got.ProxyHash, disc.ProxyHash)
	}
	if got.StartedAt != disc.StartedAt {
		t.Errorf("StartedAt: got %q, want %q", got.StartedAt, disc.StartedAt)
	}
}

func TestReadDiscovery_Errors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) string
	}{
		{
			name: "missing file",
			setup: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "nonexistent.json")
			},
		},
		{
			name: "invalid JSON",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "browser.json")
				if err := os.WriteFile(path, []byte("not json"), 0600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "missing endpoint",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "browser.json")
				data, _ := json.Marshal(BrowserDiscovery{PID: 1234})
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.setup(t)
			if _, err := readDiscovery(path); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestWriteDiscovery_Atomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "browser.json")

	disc := &BrowserDiscovery{
		WSEndpoint: "ws://127.0.0.1:9222/devtools/browser/abc",
		PID:        1,
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	if err := writeDiscovery(path, disc); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() == "browser.json.tmp" {
			t.Error("temp file should be renamed, not left behind")
		}
	}
}

func TestHealthCheck_Errors(t *testing.T) {
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

func TestProcessStatus(t *testing.T) {
	t.Run("self is healthy", func(t *testing.T) {
		if s := processStatus(os.Getpid()); s != processHealthy {
			t.Errorf("current process: got %d, want processHealthy", s)
		}
	})

	t.Run("nonexistent is gone", func(t *testing.T) {
		// PID 2^22-1 is extremely unlikely to exist
		if s := processStatus(4194303); s != processGone {
			t.Errorf("nonexistent PID: got %d, want processGone", s)
		}
	})

	t.Run("zombie is detected", func(t *testing.T) {
		// Fork a child that exits immediately — without Wait() it becomes a zombie
		cmd := exec.Command("true")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		pid := cmd.Process.Pid

		// Give the child time to exit and become zombie
		time.Sleep(100 * time.Millisecond)

		s := processStatus(pid)

		// Reap the zombie so we don't leak
		_ = cmd.Wait()

		if s != processZombie {
			t.Errorf("zombie process: got %d, want processZombie", s)
		}
	})
}

func TestIsBrowserServerProcess(t *testing.T) {
	t.Run("self is not browser server", func(t *testing.T) {
		if isBrowserServerProcess(os.Getpid()) {
			t.Error("test process should not be identified as browser server")
		}
	})

	t.Run("nonexistent returns false", func(t *testing.T) {
		if isBrowserServerProcess(4194303) {
			t.Error("nonexistent PID should return false")
		}
	})
}

func TestCleanupStaleProcess_SkipsNonOwned(t *testing.T) {
	// Launch a non-browser-server process in its own session
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid

	// cleanupStaleProcess must NOT kill it — cmdline doesn't contain --browser-server
	cleanupStaleProcess(pid)

	// Verify it's still alive
	if err := syscall.Kill(pid, 0); err != nil {
		t.Errorf("non-owned process was killed by cleanupStaleProcess: %v", err)
	}

	// Clean up
	killProcessGroup(pid)
	_ = cmd.Wait()
}

func TestParseTimeOrZero(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		if parseTimeOrZero("2026-02-10T12:00:00Z").IsZero() {
			t.Error("expected non-zero time")
		}
	})
	t.Run("invalid", func(t *testing.T) {
		if !parseTimeOrZero("not-a-time").IsZero() {
			t.Error("expected zero time")
		}
	})
}

func TestKillProcessGroup(t *testing.T) {
	// Launch a child in its own session (matching browser server launch config)
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid

	// Verify it's alive
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("process not alive after start: %v", err)
	}

	killProcessGroup(pid)
	_ = cmd.Wait()

	// Verify it's dead (signal 0 is a liveness probe)
	if err := syscall.Kill(pid, 0); err == nil {
		t.Error("process still alive after killProcessGroup")
	}
}

func TestIdleTimeoutFromEnv(t *testing.T) {
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
