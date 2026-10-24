// E2E tests for browser reuse.
//
// These tests spawn real browser server processes and validate the full
// AcquireReusableBrowser flow: leases over the server's socket, idle
// shutdown, recovery from a killed server, and proxy mismatch handling.
//
// Gating: all tests require the -e2e flag.
//
//	go test ./runtime/ -e2e
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

var e2e = flag.Bool("e2e", false, "run E2E browser reuse tests (requires Node + Chromium)")

// e2eHarness holds shared state provisioned by setupE2E.
type e2eHarness struct {
	cfg        ReusableBrowserConfig
	socketPath string
	ctx        context.Context
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

	executorBin := filepath.Join(executorNodeDir(t), "dist", "bin", "executor.js")
	if _, err := os.Stat(executorBin); os.IsNotExist(err) {
		t.Skipf("executor not built at %s", executorBin)
	}

	scriptPath := filepath.Join(filepath.Dir(executorNodeDir(t)), "examples", "demo.ts")
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("example script not found: %s", scriptPath)
	}

	// Under os.TempDir, not t.TempDir, whose long names can push the socket
	// path over its limit.
	dir, err := os.MkdirTemp("", "qe2e")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("QUARRY_NO_SANDBOX", "1")

	socketPath := filepath.Join(dir, "quarry", "browser-reuse", leaseSocketName)
	// Cleanups run after t.Context is done, so every lease has been released.
	t.Cleanup(func() {
		stopServer(socketPath)
		_ = os.RemoveAll(dir)
	})

	return e2eHarness{
		cfg: ReusableBrowserConfig{
			ExecutorPath: executorBin,
			ScriptPath:   scriptPath,
			IdleTimeout:  30 * time.Second,
		},
		socketPath: socketPath,
		ctx:        t.Context(),
	}
}

func executorNodeDir(t *testing.T) string {
	t.Helper()
	// Resolve from this file's location (quarry/runtime/)
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("failed to get caller info")
	}
	return filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))), "executor-node")
}

// serverPID returns the PID of the server listening on socketPath, from a
// lease that ends with the call.
func serverPID(t *testing.T, socketPath string) int {
	t.Helper()
	ctx, end := context.WithCancel(t.Context())
	defer end()
	lease, err := requestLease(ctx, socketPath, "")
	if err != nil {
		t.Fatalf("lease for the server PID: %v", err)
	}
	return lease.ServerPID
}

// stopServer sends SIGTERM to the server listening on socketPath, if any,
// and waits until it stops listening. The PID came from the server's own
// reply a moment earlier.
func stopServer(socketPath string) {
	ctx, end := context.WithCancel(context.Background())
	lease, err := requestLease(ctx, socketPath, "")
	end()
	if err != nil {
		return
	}
	_ = syscall.Kill(lease.ServerPID, syscall.SIGTERM)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && listening(socketPath) {
		time.Sleep(100 * time.Millisecond)
	}
}

// listening reports whether a server accepts connections on socketPath.
func listening(socketPath string) bool {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return false
	}
	iox.DiscardClose(conn)
	return true
}

// closeBrowser closes the browser at ws through CDP, as a user or a crash
// would, without its server's involvement.
func closeBrowser(t *testing.T, ws string) {
	t.Helper()
	cmd := exec.Command("node", "--input-type=module", "-e",
		"const p = (await import('puppeteer')).default; const b = await p.connect({ browserWSEndpoint: process.argv[1] }); await b.close()", ws)
	cmd.Dir = executorNodeDir(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("close browser: %v: %s", err, out)
	}
}

// waitFor polls cond until it holds, failing the test after limit.
func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(limit); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", limit)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// browserAlive reports whether the browser at ws answers.
func browserAlive(ws string) bool { return HealthCheckBrowser(ws) == nil }

// wsPort extracts the port from a WebSocket endpoint URL.
func wsPort(t *testing.T, endpoint string) string {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse ws endpoint: %v", err)
	}
	return u.Port()
}

func TestE2E_BrowserReuse_SequentialAcquire(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
	h := setupE2E(t)

	ws1, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !strings.HasPrefix(ws1, "ws://") {
		t.Fatalf("expected ws:// prefix, got %q", ws1)
	}

	if !listening(h.socketPath) {
		t.Fatal("no server listening after the first acquire")
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
	testutil.Serial(t, "setupE2E calls t.Setenv")
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

// A server killed outright leaves its socket file and its browser behind.
// The next run finds the socket refuses connections and starts a new server,
// which closes the browser the killed one left running.
func TestE2E_BrowserReuse_KilledServerReplaced(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
	h := setupE2E(t)

	ws1, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	pid := serverPID(t, h.socketPath)
	// Should the test stop before the new server closes the killed server's
	// browser, close it here, so that no browser outlives the test.
	t.Cleanup(func() {
		if browserAlive(ws1) {
			closeBrowser(t, ws1)
		}
	})
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill server: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return !listening(h.socketPath) })

	ws2, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ws2 == ws1 {
		t.Error("expected a new server's endpoint after the kill")
	}
	if browserAlive(ws1) {
		t.Error("the new server left the killed server's browser running")
	}
}

// Runs that start at once share one server: the lock serializes the start.
func TestE2E_BrowserReuse_ConcurrentAcquire(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
	h := setupE2E(t)

	const runs = 4
	endpoints := make(chan string, runs)
	var wg sync.WaitGroup
	for range runs {
		wg.Go(func() {
			ws, err := AcquireReusableBrowser(h.ctx, h.cfg)
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
			endpoints <- ws
		})
	}
	wg.Wait()
	close(endpoints)

	first := <-endpoints
	for ws := range endpoints {
		if ws != first {
			t.Errorf("runs got different browsers: %q and %q", first, ws)
		}
	}
}

func TestE2E_BrowserReuse_HealthCheck(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
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
	testutil.Serial(t, "setupE2E calls t.Setenv")
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	h := setupE2E(t)
	h.cfg.IdleTimeout = 3 * time.Second

	run, endRun := context.WithCancel(h.ctx)
	ws, err := AcquireReusableBrowser(run, h.cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	endRun()

	waitFor(t, 15*time.Second, func() bool { return !browserAlive(ws) })
	if _, err := os.Stat(h.socketPath); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket after idle shutdown: stat err = %v, want not exist", err)
	}
}

// A run that holds its lease keeps the server up past the idle timeout, even
// with no page open; the server shuts down once the run ends.
func TestE2E_BrowserReuse_LeaseKeepsServerAlive(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	h := setupE2E(t)
	h.cfg.IdleTimeout = 2 * time.Second

	run, endRun := context.WithCancel(h.ctx)
	ws, err := AcquireReusableBrowser(run, h.cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	time.Sleep(3 * h.cfg.IdleTimeout)
	if !browserAlive(ws) {
		t.Fatal("server shut down under a run that held its lease")
	}

	endRun()
	waitFor(t, 15*time.Second, func() bool { return !browserAlive(ws) })
}

// A server whose browser closes, by a user or a crash, shuts down, and the
// next run starts a new one.
func TestE2E_BrowserReuse_ClosedBrowserStopsServer(t *testing.T) {
	testutil.Serial(t, "setupE2E calls t.Setenv")
	h := setupE2E(t)

	ws1, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	closeBrowser(t, ws1)
	waitFor(t, 10*time.Second, func() bool { return !listening(h.socketPath) })

	ws2, err := AcquireReusableBrowser(h.ctx, h.cfg)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ws2 == ws1 {
		t.Error("expected a new server after the browser closed")
	}
}
