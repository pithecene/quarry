package runtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
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

// browserServerStartTimeout bounds the wait for a new server to listen.
const browserServerStartTimeout = 30 * time.Second

// browserServerStopGrace is how long a server that failed to start has to
// exit after SIGTERM before it is killed.
const browserServerStopGrace = 5 * time.Second

// AcquireReusableBrowser returns the WS endpoint of the reusable browser
// server, starting the server if none is listening. The run holds a lease on
// the server until ctx is done, and the server stays up while any lease is
// held (see CONTRACT_CLI.md, "Server lifetime").
//
// An error means the run should launch its own browser. No error path stops
// a server other than one this call started and that failed to listen.
func AcquireReusableBrowser(ctx context.Context, cfg ReusableBrowserConfig) (string, error) {
	dir, err := discoveryDir()
	if err != nil {
		return "", fmt.Errorf("browser reuse: %w", err)
	}
	socketPath := filepath.Join(dir, leaseSocketName)
	hash := proxyHash(cfg.Proxy)

	lease, err := requestLease(ctx, socketPath, hash)
	if !errors.Is(err, errNoServer) {
		return reused(lease, err)
	}

	unlock, err := lockDiscovery(dir)
	if err != nil {
		return "", fmt.Errorf("browser reuse: %w", err)
	}
	defer unlock()

	// Another run may have started a server while this one waited for the lock.
	lease, err = requestLease(ctx, socketPath, hash)
	if !errors.Is(err, errNoServer) {
		return reused(lease, err)
	}

	logPath := filepath.Join(dir, browserServerLogName)
	pid, err := startBrowserServer(ctx, cfg, dir, socketPath, logPath)
	if err != nil {
		return "", fmt.Errorf("browser reuse: launch (log: %s): %w", logPath, err)
	}
	fmt.Fprintf(os.Stderr, "Starting reusable browser server (pid=%d, log=%s)\n", pid, logPath)

	lease, err = requestLease(ctx, socketPath, hash)
	if err != nil {
		return "", fmt.Errorf("browser reuse: lease on the new server: %w", err)
	}
	return lease.WSEndpoint, nil
}

// reused reports the outcome of a lease request on an existing server.
func reused(lease browserLease, err error) (string, error) {
	switch {
	case errors.Is(err, errProxyMismatch):
		fmt.Fprintf(os.Stderr, "Proxy mismatch, skipping browser reuse for this run\n")
		return "", fmt.Errorf("browser reuse: %w", err)
	case err != nil:
		return "", fmt.Errorf("browser reuse: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Reusing browser server (pid=%d)\n", lease.ServerPID)
	return lease.WSEndpoint, nil
}

// lockDiscovery takes the exclusive lock that serializes starting a server.
// The operating system releases it if this process ends while holding it.
func lockDiscovery(dir string) (unlock func(), err error) {
	lockFile, err := os.OpenFile(filepath.Join(dir, "browser.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		iox.DiscardClose(lockFile)
		return nil, fmt.Errorf("flock: %w", err)
	}
	return func() { iox.DiscardClose(lockFile) }, nil
}

// discoveryDir returns the directory for browser reuse state:
// browser-reuse/ under $XDG_RUNTIME_DIR/quarry/, falling back to
// $TMPDIR/quarry-$UID/. Releases before v0.14.0 kept a discovery file in the
// parent directory; keeping apart means neither release's servers see the
// other's.
func discoveryDir() (string, error) {
	var base string
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		base = filepath.Join(xdg, "quarry")
	} else {
		base = filepath.Join(os.TempDir(), fmt.Sprintf("quarry-%d", os.Getuid()))
	}
	dir := filepath.Join(base, "browser-reuse")

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

// startBrowserServer starts the executor in --browser-server mode as a
// detached process and waits until it listens on socketPath. The caller
// holds the discovery lock. Returns the server's PID.
//
// Uses exec.Command (not CommandContext) because the browser server must
// outlive the run that starts it: that is the point of reuse. A server that
// fails to start is this process's unreaped child, so signaling its PID
// cannot reach another process; it is the only server the runtime stops.
func startBrowserServer(ctx context.Context, cfg ReusableBrowserConfig, dir, socketPath, logPath string) (int, error) {
	// Under the lock no server is listening, so a socket file left by one
	// that was killed is stale, and the new server cannot bind over it.
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, fmt.Errorf("remove stale socket: %w", err)
	}

	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	cmd := exec.Command(cfg.ExecutorPath, "--browser-server", cfg.ScriptPath)
	// Detach: new session so the browser server outlives the parent
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(),
		"QUARRY_BROWSER_REUSE_DIR="+dir,
		"QUARRY_BROWSER_PROXY_HASH="+proxyHash(cfg.Proxy),
		fmt.Sprintf("QUARRY_BROWSER_IDLE_TIMEOUT=%d", int(idleTimeout.Seconds())),
	)
	// Pass proxy URL so the browser server launches Chromium with --proxy-server
	if cfg.Proxy != nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("QUARRY_BROWSER_PROXY=%s://%s:%d", cfg.Proxy.Protocol, cfg.Proxy.Host, cfg.Proxy.Port))
	}

	// Diagnostics go to a log file, truncated per launch; the lock held by
	// the caller means one server writes it at a time.
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return 0, fmt.Errorf("open browser server log: %w", err)
	}
	// The child holds its own descriptor once started.
	defer iox.DiscardClose(logFile)
	cmd.Stderr = logFile

	// A pipe of our own, not StdoutPipe, so that reaping the child cannot
	// close the read end under the readiness read.
	ready, readyW, err := os.Pipe()
	if err != nil {
		return 0, fmt.Errorf("stdout pipe: %w", err)
	}
	defer iox.DiscardClose(ready)
	cmd.Stdout = readyW

	err = cmd.Start()
	iox.DiscardClose(readyW)
	if err != nil {
		return 0, fmt.Errorf("start browser server: %w", err)
	}
	exited := make(chan struct{})
	go func() {
		// Reap the server whenever it exits, while this process lives.
		_ = cmd.Wait()
		close(exited)
	}()

	if err := awaitReady(ctx, ready); err != nil {
		stopFailedServer(cmd.Process.Pid, exited)
		return 0, err
	}
	return cmd.Process.Pid, nil
}

// awaitReady waits for the server to print "ready", which it does once it
// listens on its socket.
func awaitReady(ctx context.Context, ready io.Reader) error {
	readyErr := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(ready)
		switch {
		case scanner.Scan() && strings.TrimSpace(scanner.Text()) == "ready":
			readyErr <- nil
		case scanner.Err() != nil:
			readyErr <- fmt.Errorf("reading browser server stdout: %w", scanner.Err())
		case scanner.Text() != "":
			readyErr <- fmt.Errorf("unexpected browser server output: %q", scanner.Text())
		default:
			readyErr <- errors.New("browser server exited before it listened")
		}
	}()

	select {
	case err := <-readyErr:
		return err
	case <-time.After(browserServerStartTimeout):
		return fmt.Errorf("browser server did not listen within %s", browserServerStartTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopFailedServer stops a server that failed to start: SIGTERM to its
// process group, which lets Puppeteer close the browser, then SIGKILL after
// browserServerStopGrace. The server is unreaped until exited closes, so its
// PID still names it.
func stopFailedServer(pid int, exited <-chan struct{}) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	select {
	case <-exited:
		return
	case <-time.After(browserServerStopGrace):
	}
	fmt.Fprintf(os.Stderr, "Browser server (pid=%d) did not stop within %s; killing it, which may leave its browser running\n", pid, browserServerStopGrace)
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	<-exited
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
