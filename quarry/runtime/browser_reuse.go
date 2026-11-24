package runtime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pithecene/quarry/executor"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// ReusableBrowserConfig holds inputs for AcquireReusableBrowser.
type ReusableBrowserConfig struct {
	ExecutorPath string
	ScriptPath   string
	Proxy        *types.ProxyEndpoint
	ResolveFrom  string        // the run's module root, empty when it has none
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
	key, identity, err := browserKey(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("browser reuse: %w", err)
	}

	lease, err := requestLease(ctx, socketPath, key)
	if !errors.Is(err, errNoServer) {
		return reused(ctx, lease, err)
	}

	unlock, err := lockDiscovery(dir)
	if err != nil {
		return "", fmt.Errorf("browser reuse: %w", err)
	}
	defer unlock()

	// Another run may have started a server while this one waited for the lock.
	lease, err = requestLease(ctx, socketPath, key)
	if !errors.Is(err, errNoServer) {
		return reused(ctx, lease, err)
	}

	logPath := filepath.Join(dir, browserServerLogName)
	pid, err := startBrowserServer(ctx, cfg, key, identity, dir, socketPath, logPath)
	if err != nil {
		return "", fmt.Errorf("browser reuse: launch (log: %s): %w", logPath, err)
	}
	fmt.Fprintf(os.Stderr, "Starting reusable browser server (pid=%d, log=%s)\n", pid, logPath)

	lease, err = requestLease(ctx, socketPath, key)
	if err != nil {
		return "", fmt.Errorf("browser reuse: lease on the new server: %w", err)
	}
	return healthy(ctx, lease)
}

// reused reports the outcome of a lease request on an existing server.
func reused(ctx context.Context, lease browserLease, err error) (string, error) {
	switch {
	case errors.Is(err, errLaunchMismatch):
		fmt.Fprintf(os.Stderr, "Launch settings differ from the reusable browser's, skipping browser reuse for this run\n")
		return "", fmt.Errorf("browser reuse: %w", err)
	case err != nil:
		return "", fmt.Errorf("browser reuse: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Reusing browser server (pid=%d)\n", lease.ServerPID)
	return healthy(ctx, lease)
}

// healthy returns the lease's endpoint once the browser answers on it. A
// server can hold a browser that is connected but no longer answers; such a
// lease is released at once, so the run launches its own browser and the
// server, held by no run, stops at its idle timeout.
func healthy(ctx context.Context, lease browserLease) (string, error) {
	if err := HealthCheckBrowser(ctx, lease.WSEndpoint); err != nil {
		lease.Release()
		return "", fmt.Errorf("browser reuse: server's browser does not answer (pid=%d): %w", lease.ServerPID, err)
	}
	return lease.WSEndpoint, nil
}

// lockDiscovery takes the exclusive lock that serializes starting a server.
// The operating system releases it if this process ends while holding it.
func lockDiscovery(dir string) (unlock func(), err error) {
	lockFile, err := os.OpenFile(filepath.Join(dir, "browser.lock"), os.O_CREATE|os.O_RDWR, 0o600)
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

	if err := os.MkdirAll(dir, 0o700); err != nil {
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
func HealthCheckBrowser(ctx context.Context, wsEndpoint string) error {
	u, err := url.Parse(wsEndpoint)
	if err != nil {
		return fmt.Errorf("parse ws endpoint: %w", err)
	}

	scheme := "http"
	if u.Scheme == "wss" {
		scheme = "https"
	}
	healthURL := fmt.Sprintf("%s://%s/json/version", scheme, u.Host)

	// The probe is a quick reachability check, so it bounds itself within ctx.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("health check request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	iox.DiscardClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}
	return nil
}

// launchSettings are what a reusable browser is keyed on. The executor
// reports the identity of the browser it would launch (the Puppeteer modules
// it resolves, the Chrome binary, the plugins, and the launch arguments, the
// proxy among them), because only the resolution that launches can say what
// it launches. The runtime adds the executor's digest and the proxy it selected,
// so no report can make runs on different proxies share a browser. The proxy's
// password never reaches the key.
type launchSettings struct {
	ExecutorSHA256 string                       `json:"executor_sha256"`
	Proxy          *types.ProxyEndpointRedacted `json:"proxy,omitempty"`
	Identity       any                          `json:"identity"`
}

// launchIdentityTimeout bounds the executor's identity report.
const launchIdentityTimeout = 15 * time.Second

// browserKey returns a digest of the run's launch settings. A run shares a
// reusable browser only with runs whose key is the same, so it never gets a
// browser that resolves another Puppeteer, starts another Chrome, or launches
// with other plugins, arguments, proxy, or executor.
func browserKey(ctx context.Context, cfg ReusableBrowserConfig) (string, json.RawMessage, error) {
	executorSum, err := executorSHA256(cfg.ExecutorPath)
	if err != nil {
		return "", nil, fmt.Errorf("hash the executor: %w", err)
	}
	identity, err := launchIdentity(ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	settings := launchSettings{ExecutorSHA256: executorSum}
	// Decoded, so that the key does not depend on the order of object members,
	// with each number kept as it is written.
	decoder := json.NewDecoder(bytes.NewReader(identity))
	decoder.UseNumber()
	if err := decoder.Decode(&settings.Identity); err != nil {
		return "", nil, fmt.Errorf("read the launch identity: %w", err)
	}
	if cfg.Proxy != nil {
		redacted := cfg.Proxy.Redact()
		redacted.Host = unbracketHost(redacted.Host) // as the launch spells it
		settings.Proxy = &redacted
	}
	encoded, err := json.Marshal(settings)
	if err != nil {
		return "", nil, fmt.Errorf("encode launch settings: %w", err)
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(encoded)), identity, nil
}

// launchIdentity runs the executor's --launch-identity mode in the
// environment that the browser server would get, and returns its report,
// compacted to fit the server's environment.
func launchIdentity(ctx context.Context, cfg ReusableBrowserConfig) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, launchIdentityTimeout)
	defer cancel()
	cmd := executor.Command(ctx, cfg.ExecutorPath, "--launch-identity", cfg.ScriptPath)
	cmd.Env = withResolveFrom(browserProxyEnv(cfg.Proxy), cfg.ResolveFrom)
	// A child that inherits the pipes cannot hold the wait past the deadline.
	cmd.WaitDelay = time.Second
	stdout := &boundedBuffer{limit: maxIdentityReport}
	stderr := &boundedBuffer{limit: maxIdentityDiagnostic}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("report the launch identity: %w: %s", err, bytes.TrimSpace(stderr.kept))
	}
	if stdout.overflowed {
		return nil, fmt.Errorf("the launch identity report exceeds %d bytes", maxIdentityReport)
	}
	report := bytes.TrimSpace(stdout.kept)
	if err := validIdentity(report); err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, report); err != nil {
		return nil, fmt.Errorf("read the launch identity: %w", err)
	}
	return compact.Bytes(), nil
}

// maxIdentityReport and maxIdentityDiagnostic bound what the runtime keeps of
// the identity mode's output: a valid report is a few kilobytes.
const (
	maxIdentityReport     = 1 << 20
	maxIdentityDiagnostic = 4 << 10
)

// reportedIdentity is the shape of the executor's launch identity report.
type reportedIdentity struct {
	Modules map[string]struct {
		Location string `json:"location"`
		Version  string `json:"version"`
	} `json:"modules"`
	ExecutablePath string `json:"executablePath"`
	Executable     struct {
		File     string `json:"file"`
		Revision string `json:"revision"`
	} `json:"executable"`
	Plugins *struct {
		Stealth   *bool `json:"stealth"`
		Adblocker *bool `json:"adblocker"`
	} `json:"plugins"`
	Args []string `json:"args"`
}

// validIdentity refuses a report that does not carry a whole identity, so an
// executor that prints null, an empty object, or an unresolved value never
// yields a key that stays the same while its browser changes.
func validIdentity(report []byte) error {
	var id reportedIdentity
	if err := json.Unmarshal(report, &id); err != nil {
		return fmt.Errorf("read the launch identity: %w", err)
	}
	incomplete := errors.New("the launch identity report is incomplete")
	if id.ExecutablePath == "" || id.Executable.File == "" || id.Executable.Revision == "" || id.Args == nil ||
		id.Plugins == nil || id.Plugins.Stealth == nil || id.Plugins.Adblocker == nil {
		return incomplete
	}
	required := []string{"puppeteer"}
	if *id.Plugins.Stealth || *id.Plugins.Adblocker {
		required = append(required, "puppeteer-extra")
	}
	if *id.Plugins.Stealth {
		required = append(required, "puppeteer-extra-plugin-stealth")
	}
	if *id.Plugins.Adblocker {
		required = append(required, "puppeteer-extra-plugin-adblocker")
	}
	for _, name := range required {
		if _, reported := id.Modules[name]; !reported {
			return incomplete
		}
	}
	for _, m := range id.Modules {
		if m.Location == "" || m.Version == "" {
			return incomplete
		}
	}
	return nil
}

// boundedBuffer keeps at most limit bytes and records that more arrived. It
// does not embed its buffer: io.Copy would call the promoted ReadFrom, which
// passes the limit by.
type boundedBuffer struct {
	kept       []byte
	limit      int
	overflowed bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := min(len(p), b.limit-len(b.kept))
	b.overflowed = b.overflowed || n < len(p)
	b.kept = append(b.kept, p[:n]...)
	return len(p), nil
}

// executorSHA256 returns the hex SHA-256 of the executor that path names.
func executorSHA256(path string) (string, error) {
	if path == executor.Embedded {
		return executor.EmbeddedSHA256(), nil
	}
	return fileSHA256(path)
}

// fileSHA256 returns the hex SHA-256 of the regular file at path. It opens
// without blocking and checks the opened file, so a FIFO or a device, even one
// swapped in for the path, is refused rather than read.
func fileSHA256(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer iox.DiscardClose(f)
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// startBrowserServer starts the executor in --browser-server mode as a
// detached process and waits until it listens on socketPath. The caller
// holds the discovery lock. Returns the server's PID.
//
// Uses a context without cancellation because the browser server must
// outlive the run that starts it: that is the point of reuse. A server that
// fails to start is this process's unreaped child, so signaling its PID
// cannot reach another process; it is the only server the runtime stops.
func startBrowserServer(ctx context.Context, cfg ReusableBrowserConfig, key string, identity json.RawMessage, dir, socketPath, logPath string) (int, error) {
	// Under the lock no server is listening, so a socket file left by one
	// that was killed is stale, and the new server cannot bind over it.
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, fmt.Errorf("remove stale socket: %w", err)
	}

	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	cmd := executor.Command(context.WithoutCancel(ctx), cfg.ExecutorPath, "--browser-server", cfg.ScriptPath)
	// Detach: new session so the browser server outlives the parent
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(withResolveFrom(browserProxyEnv(cfg.Proxy), cfg.ResolveFrom),
		"QUARRY_BROWSER_REUSE_DIR="+dir,
		"QUARRY_BROWSER_KEY="+key,
		// The server checks that it would launch this identity before it serves.
		"QUARRY_BROWSER_IDENTITY="+string(identity),
		fmt.Sprintf("QUARRY_BROWSER_IDLE_TIMEOUT=%d", int(idleTimeout.Seconds())),
	)
	// Diagnostics go to a log file, truncated per launch; the lock held by
	// the caller means one server writes it at a time.
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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
