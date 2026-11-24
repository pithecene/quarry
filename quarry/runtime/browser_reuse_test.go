package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pithecene/quarry/executor"
	"github.com/pithecene/quarry/internal/testutil"

	"github.com/pithecene/quarry/types"
)

// The key follows the identity that the executor reports for the browser it
// would launch, plus the executor's digest and the proxy username, and never
// the proxy password. A fake executor prints an identity whose Puppeteer
// location is $FAKE_IDENTITY and whose Chrome path carries the module root it
// receives, so the test controls the identity.
func TestBrowserKey(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	dir := t.TempDir()
	fake := func(name, extra string) string {
		path := filepath.Join(dir, name)
		body := "#!/bin/sh\n[ \"$1\" = --launch-identity ] || exit 2\n" + extra +
			"printf '{\"modules\":{\"puppeteer\":{\"location\":\"%s\",\"version\":\"25.0.0\"}},\"executablePath\":\"/chrome%s\",\"plugins\":{\"stealth\":true,\"adblocker\":false},\"args\":[]}\\n' \"$FAKE_IDENTITY\" \"${QUARRY_RESOLVE_FROM-}\"\n"
		if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	executor := fake("executor", "")
	otherExecutor := fake("other", "# another build\n")
	failing := fake("failing", "echo 'puppeteer not found' >&2\nexit 1\n")
	invalid := filepath.Join(dir, "invalid")
	if err := os.WriteFile(invalid, []byte("#!/bin/sh\necho not-json\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	report := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho '"+body+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	nullReport := report("null", "null")
	emptyReport := report("empty", "{}")
	unresolvedReport := report("unresolved", `{"modules":{"puppeteer":{"location":"/a","version":"1"}},"executablePath":{},"plugins":{"stealth":true,"adblocker":false},"args":[]}`)
	user, pass1, pass2 := "admin", "secret1", "secret2"
	proxy := func(username, password *string) *types.ProxyEndpoint {
		return &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080, Username: username, Password: password}
	}
	base := ReusableBrowserConfig{ExecutorPath: executor, ScriptPath: filepath.Join(dir, "script.ts"), Proxy: proxy(&user, &pass1)}
	with := func(change func(*ReusableBrowserConfig)) ReusableBrowserConfig {
		cfg := base
		change(&cfg)
		return cfg
	}
	key := func(t *testing.T, cfg ReusableBrowserConfig, identity string) (string, error) {
		t.Helper()
		t.Setenv("FAKE_IDENTITY", identity)
		t.Setenv("QUARRY_RESOLVE_FROM", "")
		k, _, err := browserKey(t.Context(), cfg)
		return k, err
	}
	baseKey, err := key(t, base, `/a/puppeteer.js`)
	if err != nil {
		t.Fatalf("browserKey: %v", err)
	}
	cases := []struct {
		name     string
		cfg      ReusableBrowserConfig
		identity string
		equal    bool
	}{
		{"the same identity", base, `/a/puppeteer.js`, true},
		{"another proxy password", with(func(c *ReusableBrowserConfig) { c.Proxy = proxy(&user, &pass2) }), `/a/puppeteer.js`, true},
		{"another identity", base, `/b/puppeteer.js`, false},
		{"no proxy username", with(func(c *ReusableBrowserConfig) { c.Proxy = proxy(nil, nil) }), `/a/puppeteer.js`, false},
		{"another executor", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = otherExecutor }), `/a/puppeteer.js`, false},
		{"a module root, which the identity mode receives", with(func(c *ReusableBrowserConfig) { c.ResolveFrom = "/opt/quarry" }), `/a/puppeteer.js`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := key(t, tc.cfg, tc.identity)
			if err != nil {
				t.Fatalf("browserKey: %v", err)
			}
			if (got == baseKey) != tc.equal {
				t.Errorf("keys %q and %q, want equal %t", baseKey, got, tc.equal)
			}
		})
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	refused := []struct {
		name string
		cfg  ReusableBrowserConfig
	}{
		{"an executor whose identity report fails", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = failing })},
		{"an identity that is not JSON", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = invalid })},
		{"a null identity", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = nullReport })},
		{"an empty identity", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = emptyReport })},
		{"an identity with an unresolved executable path", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = unresolvedReport })},
		{"a missing executor", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = filepath.Join(dir, "missing") })},
		{"a FIFO in place of the executor", with(func(c *ReusableBrowserConfig) { c.ExecutorPath = fifo })},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := key(t, tc.cfg, `/a/puppeteer.js`); err == nil {
				t.Error("browserKey succeeded, want an error so the run launches its own browser")
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
			if err := HealthCheckBrowser(t.Context(), tt.endpoint); err == nil {
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

	pid, err := startBrowserServer(t.Context(), cfg, "sha256:test", nil, dir, filepath.Join(dir, leaseSocketName), logPath)
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

	_, err := startBrowserServer(t.Context(), cfg, "sha256:test", nil, dir, filepath.Join(dir, leaseSocketName), filepath.Join(dir, browserServerLogName))
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

// A granted browser that does not answer is not shared, and its lease is
// released at once, so that the server is left to idle out.
func TestHealthy_ReleasesALeaseOnABrowserThatDoesNotAnswer(t *testing.T) {
	t.Parallel()

	released := 0
	lease := browserLease{
		WSEndpoint: "ws://127.0.0.1:19999/devtools/browser/x",
		ServerPID:  1,
		Release:    func() { released++ },
	}

	if _, err := healthy(t.Context(), lease); err == nil {
		t.Fatal("expected an error for a browser that does not answer")
	}
	if released != 1 {
		t.Errorf("released %d times, want 1", released)
	}
}

// The embedded executor is not a file, so its digest comes from the bundle.
func TestExecutorSHA256_Embedded(t *testing.T) {
	t.Parallel()
	got, err := executorSHA256(executor.Embedded)
	if err != nil || got != executor.EmbeddedSHA256() {
		t.Errorf("executorSHA256(Embedded) = %q, %v, want %q", got, err, executor.EmbeddedSHA256())
	}
}
