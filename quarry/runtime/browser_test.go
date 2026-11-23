package runtime

import (
	"context"
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/types"
)

// fakeBrowserServer is an executor whose --launch-browser mode prints an
// endpoint that carries the proxy it was given, then waits for stdin to
// close, as the real browser server does.
func fakeBrowserServer(t *testing.T) string {
	t.Helper()
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	path := filepath.Join(t.TempDir(), "executor")
	script := "#!/bin/sh\necho \"ws://127.0.0.1:1/devtools/browser/proxy=${QUARRY_BROWSER_PROXY-none}\"\ncat >/dev/null\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// The managed fan-out browser routes its traffic through the run's resolved
// proxy, as the reusable browser server does, and runs without a proxy when
// the run resolved none.
func TestLaunchManagedBrowser_PassesTheProxy(t *testing.T) {
	testutil.Serial(t, "sets an inherited proxy in the process environment")
	t.Setenv("QUARRY_BROWSER_PROXY", "http://ambient.example:3128")
	cases := []struct {
		name  string
		proxy *types.ProxyEndpoint
		want  string
	}{
		{"no proxy", nil, "ws://127.0.0.1:1/devtools/browser/proxy=none"},
		{
			"resolved proxy", &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example", Port: 8080},
			"ws://127.0.0.1:1/devtools/browser/proxy=http://proxy.example:8080",
		},
		{
			"IPv6 proxy", &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "::1", Port: 8080},
			"ws://127.0.0.1:1/devtools/browser/proxy=http://[::1]:8080",
		},
		{
			"bracketed IPv6 proxy", &types.ProxyEndpoint{Protocol: types.ProxyProtocolHTTP, Host: "[::1]", Port: 8080},
			"ws://127.0.0.1:1/devtools/browser/proxy=http://[::1]:8080",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			mb, err := LaunchManagedBrowser(ctx, fakeBrowserServer(t), "script.js", "", tc.proxy)
			if err != nil {
				t.Fatal(err)
			}
			if err := mb.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
			if mb.WSEndpoint != tc.want {
				t.Errorf("endpoint %q, want %q", mb.WSEndpoint, tc.want)
			}
		})
	}
}
