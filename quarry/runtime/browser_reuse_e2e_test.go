// E2E tests for browser reuse.
//
// These tests spawn real browser server processes and validate the full
// AcquireReusableBrowser flow including discovery file I/O, health checks,
// idle timeout, and proxy mismatch handling.
//
// Gating: all tests require the -e2e flag.
//
//	go test ./runtime/ -e2e
package runtime

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

var e2e = flag.Bool("e2e", false, "run E2E browser reuse tests (requires Node + Chromium)")

// e2eHarness holds shared state provisioned by setupE2E.
type e2eHarness struct {
	cfg           ReusableBrowserConfig
	discoveryPath string
	ctx           context.Context
}

// setupE2E gates on -e2e, checks prerequisites, and provisions an
// isolated discovery dir with cleanup. Returns a harness with a
// default config (30s idle, no proxy) that callers can modify.
func setupE2E(t *testing.T) e2eHarness {
	t.Helper()

	if !*e2e {
		t.Skip("-e2e flag not set")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}

	// Resolve repo root from this file's location (quarry/runtime/)
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("failed to get caller info")
	}
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	executorBin := filepath.Join(repoRoot, "executor-node", "dist", "bin", "executor.js")

	if _, err := os.Stat(executorBin); os.IsNotExist(err) {
		t.Skipf("executor not built at %s", executorBin)
	}

	scriptPath := filepath.Join(repoRoot, "examples", "demo.ts")
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("example script not found: %s", scriptPath)
	}

	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("QUARRY_NO_SANDBOX", "1")

	discoveryPath := filepath.Join(dir, "quarry", "browser.json")
	t.Cleanup(func() { killBrowserFromDiscovery(discoveryPath) })

	return e2eHarness{
		cfg: ReusableBrowserConfig{
			ExecutorPath: executorBin,
			ScriptPath:   scriptPath,
			IdleTimeout:  30 * time.Second,
		},
		discoveryPath: discoveryPath,
		ctx:           t.Context(),
	}
}

// killBrowserFromDiscovery reads a discovery file and kills the browser
// process group (best effort). Uses process group kill to ensure Chromium
// children are also terminated.
func killBrowserFromDiscovery(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var disc BrowserDiscovery
	if err := json.Unmarshal(data, &disc); err != nil {
		return
	}
	if disc.PID > 0 {
		killProcessGroup(disc.PID)
	}
	_ = os.Remove(path)
}

// wsPort extracts the port from a WebSocket endpoint URL.
func wsPort(t *testing.T, endpoint string) string {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse ws endpoint: %v", err)
	}
	return u.Port()
}

// readDiscoveryPID reads the PID from a discovery file.
func readDiscoveryPID(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read discovery: %v", err)
	}
	var disc BrowserDiscovery
	if err := json.Unmarshal(data, &disc); err != nil {
		t.Fatalf("parse discovery: %v", err)
	}
	return disc.PID
}

func TestE2E_BrowserReuse_SequentialAcquire(t *testing.T) {
	h := setupE2E(t)

	ws1, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !strings.HasPrefix(ws1, "ws://") {
		t.Fatalf("expected ws:// prefix, got %q", ws1)
	}

	if _, err := os.Stat(h.discoveryPath); err != nil {
		t.Fatalf("discovery file not created: %v", err)
	}

	ws2, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ws2 != ws1 {
		t.Errorf("expected reuse (same endpoint), got %q then %q", ws1, ws2)
	}
}

func TestE2E_BrowserReuse_ProxyMismatch(t *testing.T) {
	h := setupE2E(t)

	if _, err := AcquireReusableBrowser(h.ctx, h.cfg); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	proxyCfg := h.cfg
	proxyCfg.Proxy = &types.ProxyEndpoint{
		Protocol: types.ProxyProtocolHTTP,
		Host:     "proxy.example.com",
		Port:     8080,
	}

	_, err := AcquireReusableBrowser(h.ctx, proxyCfg)
	if err == nil {
		t.Fatal("expected error for proxy mismatch")
	}
	if !strings.Contains(err.Error(), "proxy mismatch") {
		t.Errorf("expected 'proxy mismatch' in error, got: %v", err)
	}
}

func TestE2E_BrowserReuse_StaleRecovery(t *testing.T) {
	h := setupE2E(t)

	ws1, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Kill the browser process group to simulate crash
	killProcessGroup(readDiscoveryPID(t, h.discoveryPath))
	time.Sleep(1 * time.Second)

	ws2, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("second acquire (stale recovery): %v", err)
	}
	if ws2 == ws1 {
		t.Error("expected different endpoint after stale recovery")
	}
}

func TestE2E_BrowserReuse_HealthCheck(t *testing.T) {
	h := setupE2E(t)

	ws, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := HealthCheckBrowser(ws); err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/json/version", wsPort(t, ws)))
	if err != nil {
		t.Fatalf("GET /json/version: %v", err)
	}
	defer iox.DiscardClose(resp.Body)

	var version map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&version); err != nil {
		t.Fatalf("decode /json/version: %v", err)
	}
	if _, ok := version["Browser"]; !ok {
		t.Error("expected 'Browser' field in /json/version response")
	}
}

func TestE2E_BrowserReuse_IdleShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	h := setupE2E(t)
	h.cfg.IdleTimeout = 10 * time.Second

	ws, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := HealthCheckBrowser(ws); err != nil {
		t.Fatalf("browser not alive after launch: %v", err)
	}

	// idle_timeout=10s, poll_interval=5s → max wait ~20s
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		if err := HealthCheckBrowser(ws); err != nil {
			return // Shut down as expected
		}
	}
	t.Error("browser did not self-terminate after idle timeout")
}
