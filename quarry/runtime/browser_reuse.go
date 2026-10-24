package runtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// BrowserDiscovery is the on-disk schema for a reusable browser server.
// Written to $QUARRY_RUNTIME_DIR/browser.json.
type BrowserDiscovery struct {
	WSEndpoint string `json:"ws_endpoint"`
	PID        int    `json:"pid"`
	ProxyHash  string `json:"proxy_hash"`
	StartedAt  string `json:"started_at"`
}

// ReusableBrowserConfig holds inputs for AcquireReusableBrowser.
type ReusableBrowserConfig struct {
	ExecutorPath string
	ScriptPath   string
	Proxy        *types.ProxyEndpoint
	IdleTimeout  time.Duration // 0 means default (60s)
}

// defaultIdleTimeout is used when ReusableBrowserConfig.IdleTimeout is zero.
const defaultIdleTimeout = 60 * time.Second

// browserServerLogName is the file in the discovery directory that receives
// the browser server's stderr. The server outlives the run that launches it,
// so it must not hold that run's stderr: a reader of the run's output would
// wait for the server's idle shutdown before seeing end of file.
const browserServerLogName = "browser-server.log"

// AcquireReusableBrowser returns a WS endpoint for a reusable browser server.
//
// Flow:
//  1. Resolve discovery dir
//  2. Acquire flock on browser.lock
//  3. Read browser.json — if valid + healthy + proxy hash matches → return
//  4. If stale/missing → launch --browser-server, read endpoint, write browser.json
//  5. Release lock
func AcquireReusableBrowser(ctx context.Context, cfg ReusableBrowserConfig) (string, error) {
	dir, err := discoveryDir()
	if err != nil {
		return "", fmt.Errorf("browser reuse: %w", err)
	}

	lockPath := filepath.Join(dir, "browser.lock")
	discoveryPath := filepath.Join(dir, "browser.json")
	logPath := filepath.Join(dir, browserServerLogName)

	// Acquire exclusive file lock
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return "", fmt.Errorf("browser reuse: open lock: %w", err)
	}
	defer func() {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
	}()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return "", fmt.Errorf("browser reuse: flock: %w", err)
	}

	wantHash := proxyHash(cfg.Proxy)

	// Try existing discovery file
	disc, err := readDiscovery(discoveryPath)
	if err == nil {
		// Validate proxy hash
		if disc.ProxyHash != wantHash {
			fmt.Fprintf(os.Stderr, "Proxy mismatch, skipping browser reuse for this run\n")
			return "", errors.New("browser reuse: proxy mismatch")
		}

		// Fast-path: check process liveness before the 2s HTTP health check
		switch processStatus(disc.PID) {
		case processGone:
			fmt.Fprintf(os.Stderr, "Browser server process gone (pid=%d), relaunching\n", disc.PID)
			_ = os.Remove(discoveryPath)
		case processZombie:
			fmt.Fprintf(os.Stderr, "Zombie browser server detected (pid=%d), cleaning up and relaunching\n", disc.PID)
			cleanupStaleProcess(disc.PID)
			_ = os.Remove(discoveryPath)
		default:
			if err := HealthCheckBrowser(disc.WSEndpoint); err == nil {
				age := time.Since(parseTimeOrZero(disc.StartedAt))
				fmt.Fprintf(os.Stderr, "Reusing browser server (pid=%d, age=%s)\n", disc.PID, age.Round(time.Second))
				return disc.WSEndpoint, nil
			}
			fmt.Fprintf(os.Stderr, "Stale browser server detected (pid=%d), cleaning up and relaunching\n", disc.PID)
			cleanupStaleProcess(disc.PID)
			_ = os.Remove(discoveryPath)
		}
	}

	// Launch new browser server
	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	wsEndpoint, pid, err := launchBrowserServerProcess(ctx, cfg.ExecutorPath, cfg.ScriptPath, discoveryPath, logPath, idleTimeout, cfg.Proxy)
	if err != nil {
		return "", fmt.Errorf("browser reuse: launch (log: %s): %w", logPath, err)
	}

	// Write discovery file
	disc = &BrowserDiscovery{
		WSEndpoint: wsEndpoint,
		PID:        pid,
		ProxyHash:  wantHash,
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeDiscovery(discoveryPath, disc); err != nil {
		killProcessGroup(pid)
		return "", fmt.Errorf("browser reuse: write discovery: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Starting reusable browser server (pid=%d, log=%s)\n", pid, logPath)
	return wsEndpoint, nil
}

// discoveryDir returns the directory for browser discovery files.
// Uses $XDG_RUNTIME_DIR/quarry/ on Linux, falls back to $TMPDIR/quarry-$UID/.
func discoveryDir() (string, error) {
	var dir string
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		dir = filepath.Join(xdg, "quarry")
	} else {
		dir = filepath.Join(os.TempDir(), fmt.Sprintf("quarry-%d", os.Getuid()))
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("create discovery dir %s: %w", dir, err)
	}
	return dir, nil
}

// HealthCheckBrowser verifies a browser server is alive by hitting /json/version.
// Exported for use as a pre-run gate when --browser-ws-endpoint is set.
//
// Derives the HTTP(S) health URL from the WebSocket endpoint:
//   - ws://  → http://host:port/json/version
//   - wss:// → https://host:port/json/version
//
// Port is taken from the URL if present; no default is assumed (the standard
// library will use 80/443 based on scheme).
func HealthCheckBrowser(wsEndpoint string) error {
	u, err := url.Parse(wsEndpoint)
	if err != nil {
		return fmt.Errorf("parse ws endpoint: %w", err)
	}

	scheme := "http"
	if u.Scheme == "wss" {
		scheme = "https"
	}
	healthURL := fmt.Sprintf("%s://%s/json/version", scheme, u.Host)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(healthURL)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	iox.DiscardClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}
	return nil
}

type processState int

const (
	processGone    processState = iota // PID does not exist
	processZombie                      // PID exists but is zombie/defunct
	processHealthy                     // PID exists and is running
)

// processStatus checks whether a PID is alive, zombie, or gone.
// On non-Linux systems where /proc is unavailable, falls back to signal-0 only
// (conservative — zombies will be caught by the subsequent health check).
func processStatus(pid int) processState {
	if err := syscall.Kill(pid, 0); err != nil {
		return processGone
	}

	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		// /proc unavailable (non-Linux) — conservatively assume healthy
		return processHealthy
	}

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "State:") {
			if strings.Contains(line, "Z") {
				return processZombie
			}
			return processHealthy
		}
	}

	return processHealthy
}

// isBrowserServerProcess checks whether a PID belongs to a Quarry browser
// server by reading /proc/{pid}/cmdline for the --browser-server flag.
// Returns false if /proc is unavailable (non-Linux) or the cmdline doesn't match.
func isBrowserServerProcess(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return false
	}
	for _, arg := range strings.Split(string(data), "\x00") {
		if arg == "--browser-server" {
			return true
		}
	}
	return false
}

// cleanupStaleProcess attempts to kill a stale browser server's process group.
// Only kills if the PID is verified to belong to a browser server process
// (via /proc cmdline check), preventing accidental kills after PID reuse.
func cleanupStaleProcess(pid int) {
	if isBrowserServerProcess(pid) {
		killProcessGroup(pid)
	}
}

// proxyHash returns a deterministic hash of proxy config for mismatch detection.
// Returns empty string for nil proxy.
func proxyHash(proxy *types.ProxyEndpoint) string {
	if proxy == nil {
		return ""
	}

	// Hash host, port, protocol, username (not password — avoid leaking secrets to disk)
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s:%s:%d", proxy.Protocol, proxy.Host, proxy.Port)
	if proxy.Username != nil {
		_, _ = fmt.Fprintf(h, ":%s", *proxy.Username)
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

// launchBrowserServerProcess starts the executor in --browser-server mode as a
// detached process. Returns the WS endpoint and PID.
//
// Uses exec.Command (not CommandContext) because the browser server must outlive
// the parent quarry run invocation — that is the entire point of reuse.
func launchBrowserServerProcess(
	ctx context.Context,
	executorPath, scriptPath, discoveryPath, logPath string,
	idleTimeout time.Duration,
	proxy *types.ProxyEndpoint,
) (wsEndpoint string, pid int, err error) {
	cmd := exec.Command(executorPath, "--browser-server", scriptPath)

	// Detach: new session so the browser server outlives the parent
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	// Pass discovery file path and config so the server can self-manage
	env := append(os.Environ(),
		fmt.Sprintf("QUARRY_BROWSER_DISCOVERY_FILE=%s", discoveryPath),
		fmt.Sprintf("QUARRY_BROWSER_IDLE_TIMEOUT=%d", int(idleTimeout.Seconds())),
	)

	// Pass proxy URL so the browser server launches Chromium with --proxy-server
	if proxy != nil {
		env = append(env, fmt.Sprintf("QUARRY_BROWSER_PROXY=%s://%s:%d", proxy.Protocol, proxy.Host, proxy.Port))
	}

	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", 0, fmt.Errorf("stdout pipe: %w", err)
	}

	// Diagnostics go to a log file, truncated per launch; the lock held by
	// the caller means one server writes it at a time.
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return "", 0, fmt.Errorf("open browser server log: %w", err)
	}
	// The child holds its own descriptor once started.
	defer iox.DiscardClose(logFile)
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		return "", 0, fmt.Errorf("start browser server: %w", err)
	}

	// Read WS endpoint from first line of stdout
	scanner := bufio.NewScanner(stdout)
	wsURLCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		if scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "ws://") || strings.HasPrefix(line, "wss://") {
				wsURLCh <- line
				return
			}
			errCh <- fmt.Errorf("unexpected browser server output: %q", line)
			return
		}
		if scanErr := scanner.Err(); scanErr != nil {
			errCh <- fmt.Errorf("reading browser server stdout: %w", scanErr)
			return
		}
		errCh <- errors.New("browser server exited without printing WS endpoint")
	}()

	select {
	case ws := <-wsURLCh:
		// Detached process — do NOT call cmd.Wait() (we don't own the lifecycle)
		return ws, cmd.Process.Pid, nil
	case readErr := <-errCh:
		killProcessGroup(cmd.Process.Pid)
		_ = cmd.Wait()
		return "", 0, readErr
	case <-time.After(30 * time.Second):
		killProcessGroup(cmd.Process.Pid)
		_ = cmd.Wait()
		return "", 0, errors.New("timed out waiting for browser server WS endpoint")
	case <-ctx.Done():
		killProcessGroup(cmd.Process.Pid)
		_ = cmd.Wait()
		return "", 0, ctx.Err()
	}
}

// killProcessGroup kills an entire process group by negated PID.
// The browser server is launched with Setsid: true, so its PGID == PID.
// This ensures Chromium (a child of the Node wrapper) is also killed.
func killProcessGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

// readDiscovery reads and parses a browser discovery file.
func readDiscovery(path string) (*BrowserDiscovery, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var disc BrowserDiscovery
	if err := json.Unmarshal(data, &disc); err != nil {
		return nil, fmt.Errorf("parse discovery: %w", err)
	}
	if disc.WSEndpoint == "" {
		return nil, errors.New("discovery file missing ws_endpoint")
	}
	return &disc, nil
}

// writeDiscovery atomically writes a discovery file.
func writeDiscovery(path string, disc *BrowserDiscovery) error {
	data, err := json.MarshalIndent(disc, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file, then rename for atomicity
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// parseTimeOrZero parses an RFC3339 time string, returning zero time on error.
func parseTimeOrZero(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// IdleTimeoutFromEnv reads QUARRY_BROWSER_IDLE_TIMEOUT from the environment.
// Returns 0 if unset or invalid (caller should use defaultIdleTimeout).
func IdleTimeoutFromEnv() time.Duration {
	s := os.Getenv("QUARRY_BROWSER_IDLE_TIMEOUT")
	if s == "" {
		return 0
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
